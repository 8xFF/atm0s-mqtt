use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};
use thiserror::Error;

use atm0s_small_p2p::P2pService;
use mqtt::{
    control::ConnectReturnCode,
    packet::{suback::SubscribeReturnCode, ConnackPacket, PubackPacket, PublishPacket, SubackPacket, UnsubackPacket},
};
use tokio::{net::TcpListener, select};

use crate::{
    emqx_webhook::webhook_types::WebhookEvent,
    hub::{self, LegId, LegOutput},
    WebHook,
};

mod session;

pub struct MqttBroker {
    tcp_listener: TcpListener,
    hub: hub::Hub,
    hook: WebHook,
}

impl MqttBroker {
    pub async fn new(listen: SocketAddr, hub_service: P2pService, kv_service: P2pService, hook: WebHook) -> Result<MqttBroker, std::io::Error> {
        let tcp_listener = TcpListener::bind(listen).await?;
        log::info!("[MqttBroker] listening on {}", listen);
        let hub = hub::Hub::new(hub_service, kv_service);
        Ok(MqttBroker { tcp_listener, hub, hook })
    }

    pub async fn publish(&mut self, pkt: PublishPacket) -> Result<(), hub::HubPublishError> {
        self.hub.publish(pkt).await
    }

    pub async fn recv(&mut self) -> Result<(), std::io::Error> {
        select! {
            e = self.tcp_listener.accept() => {
                let (socket, remote) = e?;
                log::info!("[MqttBroker] new connection {remote}");
                let session = session::Session::new(socket);
                let leg = self.hub.create_leg();
                let mut runner = SessionRunner::new(remote.ip(), session, leg, self.hook.clone());
                tokio::spawn(async move {
                    log::info!("[MqttBroker] session {:?} runner started", runner.id());
                    loop {
                        match runner.recv_loop().await {
                            Ok(Some(_res)) => {},
                            Ok(None) => {
                                log::info!("[MqttBroker] session {:?} runner stopped", runner.id());
                            },
                            Err(err) => {
                                log::error!("[MqttBroker] session {:?} runner error {err:?}", runner.id());
                            },
                        }
                    }
                });
                Ok(())
            }
            _ = self.hub.recv() => {
                Ok(())
            }
        }
    }
}

struct SessionCtx {
    client_id: String,
    username: Option<String>,
}

enum SessionState {
    Authentication,
    Working(Arc<SessionCtx>),
}

#[derive(Debug, Error)]
enum Error {
    #[error("mqtt error: {0}")]
    Mqtt(#[from] session::Error),
    #[error("hub error")]
    Hub,
    #[error("state error: {0}")]
    State(&'static str),
}

struct SessionRunner {
    remote: IpAddr,
    session: session::Session,
    leg: hub::Leg,
    hook: WebHook,
    state: SessionState,
}

impl SessionRunner {
    pub fn new(remote: IpAddr, session: session::Session, leg: hub::Leg, hook: WebHook) -> SessionRunner {
        SessionRunner {
            remote,
            session,
            leg,
            hook,
            state: SessionState::Authentication,
        }
    }

    pub fn id(&self) -> LegId {
        self.leg.id()
    }

    async fn run_auth_state(&mut self) -> Result<Option<()>, Error> {
        select! {
            out = self.session.recv() => match out? {
                Some(out) => match out {
                    session::Output::Continue => {
                        Ok(Some(()))
                    },
                    session::Output::Connect(connect) => {
                        let client_id = connect.client_identifier().to_string();
                        let res = self.hook.authenticate(&connect).await;
                        match res {
                            Ok(_) => {
                                let ctx = Arc::new(SessionCtx { client_id, username: connect.user_name().map(|u| u.to_string()) });
                                self.session.conn_ack(ConnackPacket::new(false, ConnectReturnCode::ConnectionAccepted)).await?;
                                self.hook.send_event(Self::connected_event(self.remote, &ctx, connect.keep_alive()));
                                self.state = SessionState::Working(ctx);
                                Ok(Some(()))
                            },
                            Err(err) => {
                                self.session.conn_ack(ConnackPacket::new(false, err)).await?;
                                Err(Error::State("auth failed"))
                            }
                        }
                    },
                    _ => {
                        Err(Error::State("not connected"))
                    },
                },
                None => {
                    log::info!("[MqttBroker] connection closed");
                    Ok(None)
                },
            },
            event = self.leg.recv() => match event {
                Some(_event) => {
                    Ok(Some(()))
                },
                None => {
                    log::warn!("[MqttBroker] leg closed");
                    Err(Error::Hub)
                },
            }
        }
    }

    async fn run_working_state(&mut self, ctx: &SessionCtx) -> Result<Option<()>, Error> {
        select! {
            out = self.session.recv() => match out? {
                Some(out) => match out {
                    session::Output::Continue => {
                        Ok(Some(()))
                    },
                    session::Output::Connect(_connect) => {
                        self.hook.send_event(Self::disconnected_event(ctx, "connect event in working state".to_string()));
                        Err(Error::State("already connected"))
                    },
                    session::Output::Subscribe(subscribe) => {
                        let mut result = vec![];
                        for (topic, _qos) in subscribe.subscribes() {
                            let topic_str: String = topic.clone().into();
                            log::info!("[MqttBroker] subscribe {topic_str}");
                            if self.hook.authorize_subscribe(ctx.client_id.as_str(), &topic_str).await.is_ok() {
                                self.leg.subscribe(&topic_str).await;
                                //TODO fix with multi qos level
                                self.hook.send_event(Self::subscribe_event(ctx, &topic_str));
                                result.push(SubscribeReturnCode::MaximumQoSLevel0);
                            } else {
                                result.push(SubscribeReturnCode::Failure);
                            }
                        }

                        let ack = SubackPacket::new(subscribe.packet_identifier(), result);
                        self.session.sub_ack(ack).await?;
                        Ok(Some(()))
                    },
                    session::Output::Unsubscribe(unsubscribe) => {
                        for topic in unsubscribe.subscribes() {
                            let topic_str: String = topic.clone().into();
                            self.leg.unsubscribe(&topic_str).await;
                            self.hook.send_event(Self::unsubscribe_event(ctx, &topic_str));
                        }

                        let ack = UnsubackPacket::new(unsubscribe.packet_identifier());
                        self.session.unsub_ack(ack).await?;
                        Ok(Some(()))
                    },
                    session::Output::Publish(publish) => {
                        let validate = self.hook.authorize_publish(ctx.client_id.as_str(), publish.topic_name()).await;
                        match validate {
                            Ok(_) => {
                                let (_qos, pkid) = publish.qos().split();
                                //TODO avoid clone
                                let topic = publish.topic_name().to_string();
                                let payload = String::from_utf8_lossy(publish.payload()).to_string();
                                self.leg.publish(publish.clone()).await;
                                if let Some(pkid) = pkid {
                                    let ack = PubackPacket::new(pkid);
                                    self.session.pub_ack(ack).await?;
                                }
                                self.hook.send_event(Self::publish_event(ctx, &topic, &payload));
                            },
                            Err(_) => {
                                //dont need to publish because of it is rejected
                            },
                        }
                        Ok(Some(()))
                    },
                },
                None => {
                    log::info!("[MqttBroker] connection closed");
                    self.hook.send_event(Self::disconnected_event(ctx, "normal".to_string()));
                    Ok(None)
                },
            },
            event = self.leg.recv() => match event {
                Some(event) => match event {
                    LegOutput::Publish(pkt) => {
                        self.session.publish(pkt).await?;
                        Ok(Some(()))
                    },
                },
                None => {
                    log::warn!("[MqttBroker] leg closed");
                    self.hook.send_event(Self::disconnected_event(ctx, "internal_error".to_string()));
                    Err(Error::Hub)
                },
            }
        }
    }

    pub async fn recv_loop(&mut self) -> Result<Option<()>, Error> {
        match &self.state {
            SessionState::Authentication => self.run_auth_state().await,
            SessionState::Working(ctx) => {
                let ctx = ctx.clone();
                self.run_working_state(&ctx).await
            }
        }
    }

    fn connected_event(remote: IpAddr, ctx: &SessionCtx, keepalive: u16) -> WebhookEvent {
        WebhookEvent::ClientConnected {
            clientid: ctx.client_id.clone(),
            username: ctx.username.clone(),
            ipaddress: remote.to_string(),
            proto_ver: 5, //TODO what is this
            keepalive,
            connected_at: 0,
            conn_ack: 0,
        }
    }

    fn disconnected_event(ctx: &SessionCtx, reason: String) -> WebhookEvent {
        WebhookEvent::ClientDisconnected {
            clientid: ctx.client_id.clone(),
            username: ctx.username.clone(),
            reason,
        }
    }

    fn subscribe_event(ctx: &SessionCtx, topic: &str) -> WebhookEvent {
        WebhookEvent::ClientSubscribe {
            clientid: ctx.client_id.clone(),
            username: ctx.username.clone(),
            topic: topic.to_string(),
        }
    }

    fn unsubscribe_event(ctx: &SessionCtx, topic: &str) -> WebhookEvent {
        WebhookEvent::ClientUnsubscribe {
            clientid: ctx.client_id.clone(),
            username: ctx.username.clone(),
            topic: topic.to_string(),
        }
    }

    fn publish_event(ctx: &SessionCtx, topic: &str, payload: &str) -> WebhookEvent {
        WebhookEvent::MessagePublish {
            from_client_id: ctx.client_id.clone(),
            from_username: ctx.username.clone(),
            topic: topic.to_string(),
            payload: payload.to_string(),
            qos: 0,
            retain: false,
            ts: 0, //TODO
        }
    }
}
