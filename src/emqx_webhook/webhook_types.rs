use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct AuthenticateRequest {
    pub clientid: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Deserialize)]
pub enum ValidateResult {
    #[serde(rename = "allow")]
    Allow,
    #[serde(rename = "deny")]
    Deny,
}

impl ValidateResult {
    pub fn is_allow(&self) -> bool {
        match self {
            ValidateResult::Allow => true,
            _ => false,
        }
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
