use std::net::SocketAddr;
use thiserror::Error;

use atm0s_small_p2p::P2pService;
use mqtt::packet::{suback::SubscribeReturnCode, PubackPacket, PublishPacket, SubackPacket, UnsubackPacket};
use tokio::{net::TcpListener, select};

use crate::{
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
                let mut runner = SessionRunner::new(session, leg, self.hook.clone());
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

enum SessionState {
    Authentication,
    Working(String),
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
    session: session::Session,
    leg: hub::Leg,
    hook: WebHook,
    state: SessionState,
}

impl SessionRunner {
    pub fn new(session: session::Session, leg: hub::Leg, hook: WebHook) -> SessionRunner {
        SessionRunner {
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
                        let res = self.hook.authenticate(connect).await;
                        match res {
                            Ok(ack) => {
                                self.session.conn_ack(ack).await?;
                                self.state = SessionState::Working(client_id);
                                Ok(Some(()))
                            },
                            Err(ack) => {
                                self.session.conn_ack(ack).await?;
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

    async fn run_working_state(&mut self, client_id: &str) -> Result<Option<()>, Error> {
        select! {
            out = self.session.recv() => match out? {
                Some(out) => match out {
                    session::Output::Continue => {
                        Ok(Some(()))
                    },
                    session::Output::Connect(_connect) => {
                        Err(Error::State("already connected"))
                    },
                    session::Output::Subscribe(subscribe) => {
                        let mut result = vec![];
                        for (topic, _qos) in subscribe.subscribes() {
                            let topic_str: String = topic.clone().into();
                            log::info!("[MqttBroker] subscribe {topic_str}");
                            if self.hook.authorize_subscribe(client_id, &topic_str).await.is_ok() {
                                self.leg.subscribe(&topic_str).await;
                                //TODO fix with multi qos level
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
                        }

                        let ack = UnsubackPacket::new(unsubscribe.packet_identifier());
                        self.session.unsub_ack(ack).await?;
                        Ok(Some(()))
                    },
                    session::Output::Publish(publish) => {
                        let validate = self.hook.authorize_publish(client_id, publish.topic_name()).await;
                        match validate {
                            Ok(_) => {
                                let (_qos, pkid) = publish.qos().split();
                                self.leg.publish(publish).await;
                                if let Some(pkid) = pkid {
                                    let ack = PubackPacket::new(pkid);
                                    self.session.pub_ack(ack).await?;
                                }
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
                    Err(Error::Hub)
                },
            }
        }
    }

    pub async fn recv_loop(&mut self) -> Result<Option<()>, Error> {
        match &self.state {
            SessionState::Authentication => self.run_auth_state().await,
            SessionState::Working(client_id) => {
                //TODO avoid clone here
                let client_id = client_id.clone();
                self.run_working_state(&client_id).await
            }
        }
    }
}
