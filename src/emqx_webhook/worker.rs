use reqwest::StatusCode;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::{mpsc::UnboundedReceiver, oneshot};

use super::{
    webhook_types::{AuthenticateRequest, AuthenticateResponse, AuthorizeRequest, AuthorizeResponse},
    WebhookConfig,
};

#[derive(Debug)]
pub enum HttpResponse<T> {
    Http200(T),
    Http204,
}

#[derive(Debug)]
pub enum WebhookJob {
    Authentication(AuthenticateRequest, oneshot::Sender<Result<HttpResponse<AuthenticateResponse>, String>>),
    Authorization(AuthorizeRequest, oneshot::Sender<Result<HttpResponse<AuthorizeResponse>, String>>),
    Connected,
    Disconnected,
}

pub struct Worker {
    cfg: WebhookConfig,
    job_rx: UnboundedReceiver<WebhookJob>,
}

impl Worker {
    pub fn new(job_rx: UnboundedReceiver<WebhookJob>, cfg: WebhookConfig) -> Self {
        Self { cfg, job_rx }
    }

    async fn process_job(&mut self, job: WebhookJob) -> Result<(), String> {
        log::info!("processing job: {job:?}");
        match job {
            WebhookJob::Authentication(authenticate_request, sender) => {
                let endpoint = self.cfg.authentication_endpoint.as_ref().expect("should have authentication endpoint");
                let res = post_http(endpoint, authenticate_request).await;
                sender.send(res).expect("should send response");
            }
            WebhookJob::Authorization(authorize_request, sender) => {
                let endpoint = self.cfg.authorization_endpoint.as_ref().expect("should have authorization endpoint");
                let res = post_http(endpoint, authorize_request).await;
                sender.send(res).expect("should send response");
            }
            WebhookJob::Connected => todo!(),
            WebhookJob::Disconnected => todo!(),
        }
        Ok(())
    }

    pub async fn recv_loop(&mut self) {
        let job = self.job_rx.recv().await.expect("should receive job");
        if let Err(e) = self.process_job(job).await {
            log::error!("failed to process job: {e}");
        }
    }
}

async fn post_http<T: Serialize, R: DeserializeOwned>(dest: &str, body: T) -> Result<HttpResponse<R>, String> {
    let client = reqwest::Client::new();
    let res = client.post(dest).json(&body).send().await.map_err(|e| e.to_string())?;

    match res.status() {
        StatusCode::OK => Ok(HttpResponse::Http200(res.json().await.map_err(|e| e.to_string())?)),
        StatusCode::NO_CONTENT => Ok(HttpResponse::Http204),
        _ => Err(res.status().to_string()),
    }
}
