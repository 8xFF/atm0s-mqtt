use mqtt::{QualityOfService, TopicName};
use poem_openapi::{payload::Json, ApiResponse, Enum, Object, OpenApi};
use tokio::sync::{mpsc::Sender, oneshot};

use crate::{http::HttpCommand, HubPublishError};

#[derive(Debug, Enum)]
#[oai(rename_all = "lowercase")]
enum PayloadEncoding {
    Plain,
    Base64,
}

fn default_encoding() -> PayloadEncoding {
    PayloadEncoding::Plain
}

fn default_qos() -> u8 {
    0
}

fn parse_qos(qos: u8) -> Option<QualityOfService> {
    match qos {
        0 => Some(QualityOfService::Level0),
        1 => Some(QualityOfService::Level1),
        2 => Some(QualityOfService::Level2),
        _ => None,
    }
}

fn default_false() -> bool {
    false
}

#[derive(Debug, Object)]
struct PublishRequest {
    topic: String,
    payload: String,
    #[oai(default = "default_encoding")]
    payload_encoding: PayloadEncoding,
    #[oai(default = "default_qos")]
    qos: u8,
    #[oai(default = "default_false")]
    retain: bool,
}

#[derive(Debug, Object)]
struct PublishSuccess {
    id: String,
}

#[derive(Debug, Object)]
struct PublishFailure {
    reason_code: u32,
    message: String,
}

impl PublishFailure {
    fn new(reason_code: u32, message: &str) -> Self {
        Self {
            reason_code,
            message: message.to_string(),
        }
    }
}

#[derive(Debug, ApiResponse)]
enum PublishError {
    #[oai(status = 400)]
    BadRequest(Json<PublishFailure>),
    #[oai(status = 202)]
    NotFound(Json<PublishFailure>),
}

#[derive(Debug, ApiResponse)]
enum PublishResponse {
    #[oai(status = 200)]
    Ok(Json<PublishSuccess>),
}

pub struct EmqxV5 {
    cmd_tx: Sender<HttpCommand>,
}

impl EmqxV5 {
    pub fn new(cmd_tx: Sender<HttpCommand>) -> Self {
        Self { cmd_tx }
    }
}

#[OpenApi]
impl EmqxV5 {
    #[oai(path = "/publish", method = "post")]
    async fn publish(&self, Json(payload): Json<PublishRequest>) -> Result<PublishResponse, PublishError> {
        let (tx, rx) = oneshot::channel();
        let cmd = HttpCommand::Publish {
            topic: TopicName::new(payload.topic).map_err(|_e| PublishError::BadRequest(Json(PublishFailure::new(0, "invalid topic"))))?,
            payload: payload.payload.into_bytes(),
            qos: parse_qos(payload.qos).ok_or(PublishError::BadRequest(Json(PublishFailure::new(0, "invalid qos"))))?,
            retain: payload.retain,
            tx,
        };
        self.cmd_tx.send(cmd).await.expect("should send to main loop");
        let res = rx.await.expect("should receive response");
        match res {
            Ok(()) => Ok(PublishResponse::Ok(Json(PublishSuccess { id: "OK".to_string() }))),
            Err(e) => match e {
                HubPublishError::NoSubscribers => Err(PublishError::NotFound(Json(PublishFailure::new(16, "no_matching_subscribers")))),
                HubPublishError::TopicInvalid => Err(PublishError::BadRequest(Json(PublishFailure::new(17, "invalid topic")))),
            },
        }
    }
}
