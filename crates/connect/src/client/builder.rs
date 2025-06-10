// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Implementation of ChannelBuilder

use std::collections::HashMap;
use std::env;
use std::str::FromStr;

use crate::errors::SparkError;

use url::Url;

use uuid::Uuid;

pub(crate) type Host = String;
pub(crate) type Port = u16;
pub(crate) type UrlParse = (Host, Port, Option<HashMap<String, String>>);

/// ChannelBuilder validates a connection string
/// based on the requirements from [Spark Documentation](https://github.com/apache/spark/blob/master/connector/connect/docs/client-connection-string.md)
#[derive(Clone, Debug)]
pub struct ChannelBuilder {
    pub(super) host: Host,
    pub(super) port: Port,
    pub(super) session_id: Uuid,
    pub(super) token: Option<String>,
    pub(super) user_id: Option<String>,
    pub(super) user_agent: Option<String>,
    pub(super) use_ssl: bool,
    pub(super) headers: Option<HashMap<String, String>>,
}

impl Default for ChannelBuilder {
    fn default() -> Self {
        let connection = match env::var("SPARK_REMOTE") {
            Ok(conn) => conn.to_string(),
            Err(_) => "sc://localhost:15002".to_string(),
        };

        ChannelBuilder::create(&connection).unwrap()
    }
}

impl ChannelBuilder {
    pub fn new() -> Self {
        ChannelBuilder::default()
    }

    pub(crate) fn endpoint(&self) -> String {
        let scheme = if cfg!(feature = "tls") {
            "https"
        } else {
            "http"
        };

        format!("{}://{}:{}", scheme, self.host, self.port)
    }

    pub(crate) fn headers(&self) -> Option<HashMap<String, String>> {
        self.headers.to_owned()
    }

    pub(crate) fn create_user_agent(user_agent: Option<&str>) -> Option<String> {
        let user_agent = user_agent.unwrap_or("_SPARK_CONNECT_RUST");
        let pkg_version = env!("CARGO_PKG_VERSION");
        let os = env::consts::OS.to_lowercase();

        Some(format!(
            "{} os/{} spark_connect_rs/{}",
            user_agent, os, pkg_version
        ))
    }

    pub(crate) fn create_user_id(user_id: Option<&str>) -> Option<String> {
        match user_id {
            Some(user_id) => Some(user_id.to_string()),
            None => env::var("USER").ok(),
        }
    }

    pub(crate) fn parse_connection_string(connection: &str) -> Result<UrlParse, SparkError> {
        let url = Url::parse(connection).map_err(|_| {
            SparkError::InvalidConnectionUrl("Failed to parse the connection URL".to_string())
        })?;

        if url.scheme() != "sc" {
            return Err(SparkError::InvalidConnectionUrl(
                "The URL must start with 'sc://'. Please update the URL to follow the correct format, e.g., 'sc://hostname:port'".to_string(),
            ));
        };

        let host = url
            .host_str()
            .ok_or_else(|| {
                SparkError::InvalidConnectionUrl(
                    "The hostname must not be empty. Please update
                    the URL to follow the correct format, e.g., 'sc://hostname:port'."
                        .to_string(),
                )
            })?
            .to_string();

        let port = url.port().ok_or_else(|| {
            SparkError::InvalidConnectionUrl(
                "The port must not be empty. Please update
                    the URL to follow the correct format, e.g., 'sc://hostname:port'."
                    .to_string(),
            )
        })?;

        let headers = ChannelBuilder::parse_headers(url);

        Ok((host, port, headers))
    }

    pub(crate) fn parse_headers(url: Url) -> Option<HashMap<String, String>> {
        let path: Vec<&str> = url
            .path()
            .split(';')
            .filter(|&pair| (pair != "/") & (!pair.is_empty()))
            .collect();

        if path.is_empty() || (path.len() == 1 && (path[0].is_empty() || path[0] == "/")) {
            return None;
        }

        let headers: HashMap<String, String> = path
            .iter()
            .copied()
            .map(|pair| {
                let mut parts = pair.splitn(2, '=');
                (
                    parts.next().unwrap_or("").to_string(),
                    parts.next().unwrap_or("").to_string(),
                )
            })
            .collect();

        if headers.is_empty() {
            return None;
        }

        Some(headers)
    }

    /// Create and validate a connnection string
    #[allow(unreachable_code)]
    pub fn create(connection: &str) -> Result<ChannelBuilder, SparkError> {
        let (host, port, headers) = ChannelBuilder::parse_connection_string(connection)?;

        let mut channel_builder = ChannelBuilder {
            host,
            port,
            session_id: Uuid::new_v4(),
            token: None,
            user_id: ChannelBuilder::create_user_id(None),
            user_agent: ChannelBuilder::create_user_agent(None),
            use_ssl: false,
            headers: None,
        };

        if let Some(mut headers) = headers {
            channel_builder.user_id = headers
                .remove("user_id")
                .map(|user_id| ChannelBuilder::create_user_id(Some(&user_id)))
                .unwrap_or_else(|| ChannelBuilder::create_user_id(None));

            channel_builder.user_agent = headers
                .remove("user_agent")
                .map(|user_agent| ChannelBuilder::create_user_agent(Some(&user_agent)))
                .unwrap_or_else(|| ChannelBuilder::create_user_agent(None));

            if let Some(token) = headers.remove("token") {
                let token = format!("Bearer {token}");
                channel_builder.token = Some(token.clone());
                headers.insert("authorization".to_string(), token);
            }

            if let Some(session_id) = headers.remove("session_id") {
                channel_builder.session_id = Uuid::from_str(&session_id)?
            }

            if let Some(use_ssl) = headers.remove("use_ssl") {
                if use_ssl.to_lowercase() == "true" {
                    #[cfg(not(feature = "tls"))]
                    {
                        panic!(
                        "The 'use_ssl' option requires the 'tls' feature, but it's not enabled!"
                    );
                    };
                    channel_builder.use_ssl = true
                }
            };

            if !headers.is_empty() {
                channel_builder.headers = Some(headers);
            }
        }

        Ok(channel_builder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_builder_default() {
        let expected_url = "http://localhost:15002".to_string();

        let cb = ChannelBuilder::default();

        assert_eq!(expected_url, cb.endpoint())
    }

    #[test]
    fn test_panic_incorrect_url_scheme() {
        let connection = "http://127.0.0.1:15002";

        assert!(ChannelBuilder::create(connection).is_err())
    }

    #[test]
    fn test_panic_missing_url_host() {
        let connection = "sc://:15002";

        assert!(ChannelBuilder::create(connection).is_err())
    }

    #[test]
    fn test_panic_missing_url_port() {
        let connection = "sc://127.0.0.1";

        assert!(ChannelBuilder::create(connection).is_err())
    }

    #[test]
    fn test_settings_builder() {
        let connection = "sc://myhost.com:443/;token=ABCDEFG;user_agent=some_agent;user_id=user123";

        let builder = ChannelBuilder::create(connection).unwrap();

        assert_eq!("http://myhost.com:443".to_string(), builder.endpoint());
        assert_eq!("Bearer ABCDEFG".to_string(), builder.token.unwrap());
        assert_eq!("user123".to_string(), builder.user_id.unwrap());
    }

    #[test]
    #[should_panic(
        expected = "The 'use_ssl' option requires the 'tls' feature, but it's not enabled!"
    )]
    fn test_panic_ssl() {
        let connection = "sc://127.0.0.1:443/;use_ssl=true";

        ChannelBuilder::create(connection).unwrap();
    }
}
