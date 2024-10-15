use std::str::FromStr;

use std::collections::HashMap;
use tonic::metadata::{
    Ascii, AsciiMetadataValue, KeyAndValueRef, MetadataKey, MetadataMap, MetadataValue,
};
use tonic::service::Interceptor;
use tonic::Status;

#[derive(Clone, Debug)]
pub struct MetadataInterceptor {
    token: Option<String>,
    metadata: Option<MetadataMap>,
}

impl Interceptor for MetadataInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        if let Some(header) = &self.metadata {
            merge_metadata(req.metadata_mut(), header);
        }
        if let Some(token) = &self.token {
            req.metadata_mut().insert(
                "authorization",
                AsciiMetadataValue::from_str(token.as_str()).unwrap(),
            );
        }

        Ok(req)
    }
}

impl MetadataInterceptor {
    pub fn new(token: Option<String>, metadata: Option<MetadataMap>) -> Self {
        MetadataInterceptor { token, metadata }
    }
}

pub(super) fn metadata_builder(headers: &HashMap<String, String>) -> MetadataMap {
    let mut metadata_map = MetadataMap::new();
    for (key, val) in headers.iter() {
        let meta_val = MetadataValue::from_str(val.as_str()).unwrap();
        let meta_key = MetadataKey::from_str(key.as_str()).unwrap();

        metadata_map.insert(meta_key, meta_val);
    }

    metadata_map
}

fn merge_metadata(metadata_into: &mut MetadataMap, metadata_from: &MetadataMap) {
    metadata_for_each(metadata_from, |key, value| {
        if key.to_string().starts_with("x-") {
            metadata_into.insert(key, value.to_owned());
        }
    })
}

fn metadata_for_each<F>(metadata: &MetadataMap, mut f: F)
where
    F: FnMut(&MetadataKey<Ascii>, &MetadataValue<Ascii>),
{
    for kv_ref in metadata.iter() {
        match kv_ref {
            KeyAndValueRef::Ascii(key, value) => f(key, value),
            KeyAndValueRef::Binary(_key, _value) => {}
        }
    }
}

//
// #[derive(Clone, Debug)]
// pub struct MetadataInterceptor {
//     token: Option<String>,
//     metadata: Option<MetadataMap>,
// }
//
// impl Interceptor for MetadataInterceptor {
//     fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
//         if let Some(header) = &self.metadata {
//             merge_metadata(req.metadata_mut(), header);
//         }
//         if let Some(token) = &self.token {
//             req.metadata_mut().insert(
//                 "authorization",
//                 AsciiMetadataValue::from_str(token.as_str()).unwrap(),
//             );
//         }
//
//         Ok(req)
//     }
// }
//
// impl MetadataInterceptor {
//     pub fn new(token: Option<String>, metadata: Option<MetadataMap>) -> Self {
//         MetadataInterceptor { token, metadata }
//     }
// }
//
// pub fn metadata_builder(headers: &HashMap<String, String>) -> MetadataMap {
//     let mut metadata_map = MetadataMap::new();
//     for (key, val) in headers.iter() {
//         let meta_val = MetadataValue::from_str(val.as_str()).unwrap();
//         let meta_key = MetadataKey::from_str(key.as_str()).unwrap();
//
//         metadata_map.insert(meta_key, meta_val);
//     }
//
//     metadata_map
// }
//
// fn merge_metadata(metadata_into: &mut MetadataMap, metadata_from: &MetadataMap) {
//     metadata_for_each(metadata_from, |key, value| {
//         if key.to_string().starts_with("x-") {
//             metadata_into.insert(key, value.to_owned());
//         }
//     })
// }
//
// fn metadata_for_each<F>(metadata: &MetadataMap, mut f: F)
// where
//     F: FnMut(&MetadataKey<Ascii>, &MetadataValue<Ascii>),
// {
//     for kv_ref in metadata.iter() {
//         match kv_ref {
//             KeyAndValueRef::Ascii(key, value) => f(key, value),
//             KeyAndValueRef::Binary(_key, _value) => {}
//         }
//     }
// }
//
// pub mod service {
//     use hyper::body::Body;
//     use hyper::{Request, Response};
//     use std::future::Future;
//     use std::pin::Pin;
//     use std::task::{Context, Poll};
//     use tonic::body::BoxBody;
//     use tonic::transport::Channel;
//     use tonic::IntoRequest;
//     use tower::Service;
//
//     #[derive(Debug, Clone)]
//     pub struct AuthSvc {
//         inner: Channel,
//     }
//
//     impl AuthSvc {
//         pub fn new(inner: Channel) -> Self {
//             AuthSvc { inner }
//         }
//     }
//
//     impl Service<Request<BoxBody>> for AuthSvc {
//         type Response = Response<BoxBody>;
//         type Error = Box<dyn std::error::Error + Send + Sync>;
//
//         #[allow(clippy::type_complexity)]
//         type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
//
//         fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//             self.inner.poll_ready(cx).map_err(Into::into)
//         }
//
//         fn call(&mut self, req: Request<BoxBody>) -> Self::Future {
//             // This is necessary because tonic internally uses `tower::buffer::Buffer`.
//             // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
//             // for details on why this is necessary
//             let clone = self.inner.clone();
//             let mut inner = std::mem::replace(&mut self.inner, clone);
//
//             Box::pin(async move {
//                 // Do extra async work here...
//                 let response = inner.call(req).await?;
//
//                 // let (parts, _) = response..into_parts();
//
//                 Ok(response)
//             })
//         }
//     }
// }
