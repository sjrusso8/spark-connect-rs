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

//! Middleware services implemented with tower.rs

use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;
use std::task::{Context, Poll};

use futures_util::future::BoxFuture;
use http_body::combinators::UnsyncBoxBody;

use tonic::codegen::http::Request;
use tonic::codegen::http::{HeaderName, HeaderValue};

use tower::Service;

/// Headers to apply a gRPC request
#[derive(Debug, Clone)]
pub struct HeadersLayer {
    headers: HashMap<String, String>,
}

impl HeadersLayer {
    pub fn new(headers: HashMap<String, String>) -> Self {
        Self { headers }
    }
}

impl<S> tower::Layer<S> for HeadersLayer {
    type Service = HeadersMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        HeadersMiddleware::new(inner, self.headers.clone())
    }
}

/// Middleware used to apply provided headers onto a gRPC request
#[derive(Clone, Debug)]
pub struct HeadersMiddleware<S> {
    inner: S,
    headers: HashMap<String, String>,
}

#[allow(dead_code)]
impl<S> HeadersMiddleware<S> {
    pub fn new(inner: S, headers: HashMap<String, String>) -> Self {
        Self { inner, headers }
    }
}

// TODO! as of now Request is not clone. So the retry logic does not work.
// https://github.com/tower-rs/tower/pull/790
impl<S> Service<Request<UnsyncBoxBody<prost::bytes::Bytes, tonic::Status>>> for HeadersMiddleware<S>
where
    S: Service<Request<UnsyncBoxBody<prost::bytes::Bytes, tonic::Status>>>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
    S::Response: Send + Debug + 'static,
    S::Error: Debug,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(
        &mut self,
        mut request: Request<UnsyncBoxBody<prost::bytes::Bytes, tonic::Status>>,
    ) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        let headers = self.headers.clone();

        Box::pin(async move {
            for (key, value) in &headers {
                let meta_key = HeaderName::from_str(key.as_str()).unwrap();
                let meta_val = HeaderValue::from_str(value.as_str()).unwrap();

                request.headers_mut().insert(meta_key, meta_val);
            }

            inner.call(request).await
        })
    }
}
