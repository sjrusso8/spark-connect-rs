use std::collections::HashMap;
use uuid::Uuid;

use crate::client::builder::{Host, Port};
use crate::client::ChannelBuilder;

/// Config handler to set custom SparkSessionBuilder options
#[derive(Clone, Debug, Default)]
pub struct Config {
    pub host: Host,
    pub port: Port,
    pub session_id: Uuid,
    pub token: Option<String>,
    pub user_id: Option<String>,
    pub user_agent: Option<String>,
    pub use_ssl: bool,
    pub headers: Option<HashMap<String, String>>,
}

impl Config {
    pub fn new() -> Self {
        Config {
            host: "localhost".to_string(),
            port: 15002,
            token: None,
            session_id: Uuid::new_v4(),
            user_id: ChannelBuilder::create_user_id(None),
            user_agent: ChannelBuilder::create_user_agent(None),
            use_ssl: false,
            headers: None,
        }
    }

    pub fn host(mut self, val: &str) -> Self {
        self.host = val.to_string();
        self
    }

    pub fn port(mut self, val: Port) -> Self {
        self.port = val;
        self
    }

    pub fn token(mut self, val: &str) -> Self {
        self.token = Some(val.to_string());
        self
    }

    pub fn session_id(mut self, val: Uuid) -> Self {
        self.session_id = val;
        self
    }

    pub fn user_id(mut self, val: &str) -> Self {
        self.user_id = Some(val.to_string());
        self
    }

    pub fn user_agent(mut self, val: &str) -> Self {
        self.user_agent = Some(val.to_string());
        self
    }

    pub fn use_ssl(mut self, val: bool) -> Self {
        self.use_ssl = val;
        self
    }

    pub fn headers(mut self, val: HashMap<String, String>) -> Self {
        self.headers = Some(val);
        self
    }
}

impl From<Config> for ChannelBuilder {
    fn from(config: Config) -> Self {
        // if there is a token, then it needs to be added to the headers
        // do not overwrite any existing authentication header

        let mut headers = config.headers.unwrap_or_default();

        if let Some(token) = &config.token {
            headers
                .entry("authorization".to_string())
                .or_insert_with(|| format!("Bearer {}", token));
        }

        Self {
            host: config.host,
            port: config.port,
            session_id: config.session_id,
            token: config.token,
            user_id: config.user_id,
            user_agent: config.user_agent,
            use_ssl: config.use_ssl,
            headers: if headers.is_empty() {
                None
            } else {
                Some(headers)
            },
        }
    }
}
