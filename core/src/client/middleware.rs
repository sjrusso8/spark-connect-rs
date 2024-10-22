use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;
use std::task::{Context, Poll};

use futures_util::future::BoxFuture;
use http_body::combinators::UnsyncBoxBody;

use tonic::codegen::http::Request;
use tonic::codegen::http::{HeaderName, HeaderValue};

use tower::Service;

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

// TODO! as of now Request is not clone. So the retry logic does not work.
// https://github.com/tower-rs/tower/pull/790
//
// use futures_util::future;
// use tower::retry::Policy;
// use tonic::codegen::http::Response;
// use tonic::transport::Body;
//
// #[derive(Clone, Debug)]
// pub struct RetryPolicy {
//     max_retries: usize,
//     backoff_multiplier: i32,
//     max_backoff: usize,
// }
//
// impl Default for RetryPolicy {
//     fn default() -> Self {
//         Self {
//             max_retries: 15,
//             backoff_multiplier: 4,
//             max_backoff: 600,
//         }
//     }
// }
//
// type Req = Request<UnsyncBoxBody<prost::bytes::Bytes, tonic::Status>>;
// type Res = Response<Body>;
//
// impl<E> Policy<Req, Res, E> for RetryPolicy {
//     type Future = future::Ready<()>;
//
//     fn retry(&mut self, _req: &mut Req, result: &mut Result<Res, E>) -> Option<Self::Future> {
//         match result {
//             Ok(_) => {
//                 None
//             }
//             Err(_) => {
//                 if self.max_retries > 0 {
//                     self.max_retries -= 1;
//                     Some(future::ready(()))
//                 } else {
//                     None
//                 }
//             }
//         }
//     }
//
//     fn clone_request(&mut self, req: &Req) -> Option<Req> {
//         let (parts, body) = req.into_parts();
//
//         let req = Request::from_parts(parts, body);
//
//         Some(req)
//     }
// }
