//! DataFrame representation based on the Spark Connect gRPC protobuf

#[allow(clippy::module_inception)]
pub mod dataframe;

pub use dataframe::DataFrame;
pub use dataframe::DataFrameReader;
pub use dataframe::DataFrameWriter;
