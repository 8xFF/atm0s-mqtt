use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct AuthenticateRequest {
    pub clientid: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ValidateResult {
    Allow,
    Deny,
}

impl ValidateResult {
    pub fn is_allow(&self) -> bool {
        matches!(self, ValidateResult::Allow)
    }
}

#[derive(Debug, Deserialize)]
pub struct AuthenticateResponse {
    pub result: ValidateResult,
    #[serde(default)]
    pub is_superuser: bool,
    pub client_attrs: Option<HashMap<String, String>>,
    pub expire_at: Option<u64>,
    pub acl: Option<Vec<AclRule>>,
}

#[derive(Debug, Deserialize)]
pub struct AclRule {
    pub permission: String,
    pub action: String,
    pub topic: String,
    pub qos: Option<Vec<u8>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthorizeAction {
    Subscribe,
    Publish,
}

#[derive(Debug, Serialize)]
pub struct AuthorizeRequest {
    pub clientid: String,
    pub action: AuthorizeAction,
    pub topic: String,
}

#[derive(Debug, Deserialize)]
pub struct AuthorizeResponse {
    pub result: ValidateResult,
}

#[derive(Debug, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum WebhookEvent {
    ClientConnected {
        clientid: String,
        username: Option<String>,
        ipaddress: String,
        proto_ver: u8,
        keepalive: u16,
        connected_at: u64,
        conn_ack: u8,
    },
    ClientDisconnected {
        clientid: String,
        username: Option<String>,
        reason: String,
    },
    ClientSubscribe {
        clientid: String,
        username: Option<String>,
        topic: String,
    },
    ClientUnsubscribe {
        clientid: String,
        username: Option<String>,
        topic: String,
    },
    MessagePublish {
        from_client_id: String,
        from_username: Option<String>,
        topic: String,
        qos: u8,
        retain: bool,
        payload: String,
        ts: u64,
    },
}
