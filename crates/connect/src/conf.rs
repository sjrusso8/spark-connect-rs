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

//! Configuration for a Spark application. Used to set various Spark parameters as key-value pairs.

use std::collections::HashMap;

use crate::spark;

use crate::client::SparkClient;
use crate::errors::SparkError;

/// User-facing configuration API, accessible through SparkSession.conf.
pub struct RunTimeConfig {
    pub(crate) client: SparkClient,
}

/// User-facing configuration API, accessible through SparkSession.conf.
///
/// Options set here are automatically propagated to the Hadoop configuration during I/O.
///
/// # Example
/// ```rust
/// spark
///    .conf()
///    .set("spark.sql.shuffle.partitions", "42")
///    .await?;
/// ```
impl RunTimeConfig {
    pub fn new(client: &SparkClient) -> RunTimeConfig {
        RunTimeConfig {
            client: client.clone(),
        }
    }

    pub(crate) async fn set_configs(
        &mut self,
        map: &HashMap<String, String>,
    ) -> Result<(), SparkError> {
        for (key, value) in map {
            self.set(key.as_str(), value.as_str()).await?
        }
        Ok(())
    }

    /// Sets the given Spark runtime configuration property.
    pub async fn set(&mut self, key: &str, value: &str) -> Result<(), SparkError> {
        let op_type = spark::config_request::operation::OpType::Set(spark::config_request::Set {
            pairs: vec![spark::KeyValue {
                key: key.into(),
                value: Some(value.into()),
            }],
        });
        let operation = spark::config_request::Operation {
            op_type: Some(op_type),
        };

        let _ = self.client.config_request(operation).await?;

        Ok(())
    }

    /// Resets the configuration property for the given key.
    pub async fn unset(&mut self, key: &str) -> Result<(), SparkError> {
        let op_type =
            spark::config_request::operation::OpType::Unset(spark::config_request::Unset {
                keys: vec![key.to_string()],
            });
        let operation = spark::config_request::Operation {
            op_type: Some(op_type),
        };

        let _ = self.client.config_request(operation).await?;

        Ok(())
    }

    /// Indicates whether the configuration property with the given key is modifiable in the current session.
    pub async fn get(&mut self, key: &str, default: Option<&str>) -> Result<String, SparkError> {
        let operation = match default {
            Some(default) => {
                let op_type = spark::config_request::operation::OpType::GetWithDefault(
                    spark::config_request::GetWithDefault {
                        pairs: vec![spark::KeyValue {
                            key: key.into(),
                            value: Some(default.into()),
                        }],
                    },
                );
                spark::config_request::Operation {
                    op_type: Some(op_type),
                }
            }
            None => {
                let op_type =
                    spark::config_request::operation::OpType::Get(spark::config_request::Get {
                        keys: vec![key.to_string()],
                    });
                spark::config_request::Operation {
                    op_type: Some(op_type),
                }
            }
        };

        let resp = self.client.config_request(operation).await?;

        let val = resp.pairs.first().unwrap().value().to_string();

        Ok(val)
    }

    /// Indicates whether the configuration property with the given key is modifiable in the current session.
    pub async fn is_modifable(&mut self, key: &str) -> Result<bool, SparkError> {
        let op_type = spark::config_request::operation::OpType::IsModifiable(
            spark::config_request::IsModifiable {
                keys: vec![key.to_string()],
            },
        );
        let operation = spark::config_request::Operation {
            op_type: Some(op_type),
        };

        let resp = self.client.config_request(operation).await?;

        let val = resp.pairs.first().unwrap().value();

        match val {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(SparkError::AnalysisException(
                "Unexpected response value for boolean".to_string(),
            )),
        }
    }
}
