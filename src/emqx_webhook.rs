use std::sync::Arc;

use mqtt::{control::ConnectReturnCode, packet::ConnectPacket};
use tokio::sync::{mpsc::UnboundedSender, oneshot};

pub mod webhook_types;
mod worker;

use webhook_types::{AuthenticateRequest, ValidateResult};
use worker::HttpResponse;
pub use worker::WebhookJob;

#[derive(Debug, Clone, Default)]
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
        let index = rand::random::<usize>() % self.workers.len();
        self.workers[index].send(job).expect("should send job");
    }

    pub async fn authenticate(&self, connect: &ConnectPacket) -> Result<(), ConnectReturnCode> {
        if self.cfg.authentication_endpoint.is_none() {
            return Ok(());
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
                ValidateResult::Allow => {
                    log::info!("auth Allow");
                    Ok(())
                }
                ValidateResult::Deny => {
                    log::warn!("auth Deny");
                    Err(ConnectReturnCode::BadUserNameOrPassword)
                }
            },
            Ok(Ok(HttpResponse::Http204)) => {
                log::info!("auth Allow");
                Ok(())
            }
            Ok(Err(err)) => {
                log::error!("auth error {err}");
                Err(ConnectReturnCode::ServiceUnavailable)
            }
            Err(err) => {
                log::error!("auth error: {err}");
                Err(ConnectReturnCode::ServiceUnavailable)
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

#[cfg(test)]
mod tests {
    use httpmock::MockServer;
    use mqtt::{control::ConnectReturnCode, packet::ConnectPacket};

    #[test_log::test(tokio::test)]
    async fn authenticate_200_ok() {
        let server = MockServer::start();
        let authentication_mook = server.mock(|when, then| {
            when.method("POST").path("/auth");
            then.status(200).header("Content-Type", "application/json").body(r#"{"result": "allow"}"#);
        });

        let webhook = super::WebHook::new(
            1,
            super::WebhookConfig {
                authentication_endpoint: Some(server.url("/auth")),
                ..Default::default()
            },
        );

        let mut req = ConnectPacket::new("c1");
        req.set_user_name(Some("u1".to_string()));
        req.set_password(Some("p1".to_string()));

        let res = webhook.authenticate(&req).await;
        assert_eq!(res, Ok(()));

        authentication_mook.assert();
    }

    #[test_log::test(tokio::test)]
    async fn authenticate_200_reject() {
        let server = MockServer::start();
        let authentication_mook = server.mock(|when, then| {
            when.method("POST").path("/auth");
            then.status(200).header("Content-Type", "application/json").body(r#"{"result": "deny"}"#);
        });

        let webhook = super::WebHook::new(
            1,
            super::WebhookConfig {
                authentication_endpoint: Some(server.url("/auth")),
                ..Default::default()
            },
        );

        let mut req = ConnectPacket::new("c1");
        req.set_user_name(Some("u1".to_string()));
        req.set_password(Some("p1".to_string()));

        let res = webhook.authenticate(&req).await;
        assert_eq!(res, Err(ConnectReturnCode::BadUserNameOrPassword));

        authentication_mook.assert();
    }

    #[test_log::test(tokio::test)]
    async fn authenticate_204_ok() {
        let server = MockServer::start();
        let authentication_mook = server.mock(|when, then| {
            when.method("POST").path("/auth");
            then.status(204);
        });

        let webhook = super::WebHook::new(
            1,
            super::WebhookConfig {
                authentication_endpoint: Some(server.url("/auth")),
                ..Default::default()
            },
        );

        let mut req = ConnectPacket::new("c1");
        req.set_user_name(Some("u1".to_string()));
        req.set_password(Some("p1".to_string()));

        let res = webhook.authenticate(&req).await;
        assert_eq!(res, Ok(()));

        authentication_mook.assert();
    }
}
