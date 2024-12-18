use std::sync::Arc;

use mqtt::{
    control::ConnectReturnCode,
    packet::{ConnackPacket, ConnectPacket},
};
use tokio::sync::{mpsc::UnboundedSender, oneshot};

pub mod webhook_types;
mod worker;

use webhook_types::AuthenticateRequest;
use worker::HttpResponse;
pub use worker::WebhookJob;

#[derive(Debug, Clone)]
pub struct WebhookConfig {
    pub authentication_endpoint: Option<String>,
    pub authorization_endpoint: Option<String>,
    pub event_endpoint: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WebHook {
    cfg: WebhookConfig,
    workers: Arc<Vec<UnboundedSender<WebhookJob>>>,
}

impl WebHook {
    pub fn new(workers_number: usize, cfg: WebhookConfig) -> Self {
        let mut workers = Vec::with_capacity(workers_number);
        for _ in 0..workers_number {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let mut worker = worker::Worker::new(rx, cfg.clone());
            tokio::spawn(async move {
                loop {
                    worker.recv_loop().await
                }
            });
            workers.push(tx);
        }
        Self { workers: Arc::new(workers), cfg }
    }

    fn send_job(&self, job: WebhookJob) {
        let index = self.workers.len() % rand::random::<usize>();
        self.workers[index].send(job).expect("should send job");
    }

    pub async fn authenticate(&self, connect: ConnectPacket) -> Result<ConnackPacket, ConnackPacket> {
        if self.cfg.authentication_endpoint.is_none() {
            return Ok(ConnackPacket::new(false, ConnectReturnCode::ConnectionAccepted));
        }

        let (tx, rx) = oneshot::channel();
        self.send_job(WebhookJob::Authentication(
            AuthenticateRequest {
                clientid: connect.client_identifier().to_string(),
                username: connect.user_name().map(|u| u.to_owned()),
                password: connect.password().map(|p| p.to_owned()),
            },
            tx,
        ));

        match rx.await {
            Ok(Ok(HttpResponse::Http200(res))) => match res.result {
                webhook_types::ValidateResult::Allow => {
                    log::info!("auth Allow");
                    Ok(ConnackPacket::new(false, ConnectReturnCode::ConnectionAccepted))
                }
                webhook_types::ValidateResult::Deny => {
                    log::warn!("auth Deny");
                    Err(ConnackPacket::new(false, ConnectReturnCode::BadUserNameOrPassword))
                }
            },
            Ok(Ok(HttpResponse::Http204)) => {
                log::info!("auth Allow");
                Ok(ConnackPacket::new(false, ConnectReturnCode::ConnectionAccepted))
            }
            Ok(Err(err)) => {
                log::error!("auth error {err}");
                Err(ConnackPacket::new(false, ConnectReturnCode::ServiceUnavailable))
            }
            Err(err) => {
                log::error!("auth error: {err}");
                Err(ConnackPacket::new(false, ConnectReturnCode::ServiceUnavailable))
            }
        }
    }

    pub async fn authorize_subscribe(&self, client_id: &str, topic: &str) -> Result<(), ()> {
        if self.cfg.authorization_endpoint.is_none() {
            return Ok(());
        }

        let (tx, rx) = oneshot::channel();
        self.send_job(WebhookJob::Authorization(
            webhook_types::AuthorizeRequest {
                clientid: client_id.to_string(),
                action: webhook_types::AuthorizeAction::Subscribe,
                topic: topic.to_owned(),
            },
            tx,
        ));

        match rx.await {
            Ok(Ok(HttpResponse::Http200(sub_res))) => {
                if sub_res.result.is_allow() {
                    Ok(())
                } else {
                    Err(())
                }
            }
            Ok(Ok(HttpResponse::Http204)) => Ok(()),
            Ok(Err(err)) => {
                log::error!("subscribe authorize error {err}");
                Err(())
            }
            Err(err) => {
                log::error!("subscribe authorize error: {err}");
                Err(())
            }
        }
    }

    pub async fn authorize_publish(&self, client_id: &str, topic: &str) -> Result<(), ()> {
        if self.cfg.authorization_endpoint.is_none() {
            return Ok(());
        }

        let (tx, rx) = oneshot::channel();
        self.send_job(WebhookJob::Authorization(
            webhook_types::AuthorizeRequest {
                clientid: client_id.to_string(),
                topic: topic.to_owned(),
                action: webhook_types::AuthorizeAction::Publish,
            },
            tx,
        ));

        match rx.await {
            Ok(Ok(HttpResponse::Http200(sub_res))) => {
                if sub_res.result.is_allow() {
                    Ok(())
                } else {
                    Err(())
                }
            }
            Ok(Ok(HttpResponse::Http204)) => Ok(()),
            Ok(Err(err)) => {
                log::error!("publish authorize error {err}");
                Err(())
            }
            Err(err) => {
                log::error!("publish authorize error: {err}");
                Err(())
            }
        }
    }
}
