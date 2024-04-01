//! Rust Types to Spark Types

use crate::impl_to_data_type;
use crate::spark;

pub trait ToDataType {
    fn to_proto_type(&self) -> spark::DataType;
}

// Call the macro with the input pairs
impl_to_data_type!(bool, Boolean);
impl_to_data_type!(i16, Short);
impl_to_data_type!(i32, Integer);
impl_to_data_type!(i64, Long);
impl_to_data_type!(isize, Long);
impl_to_data_type!(f32, Float);
impl_to_data_type!(f64, Double);
impl_to_data_type!(&str, String);
impl_to_data_type!(String, String);

impl ToDataType for chrono::NaiveDate {
    fn to_proto_type(&self) -> spark::DataType {
        spark::DataType {
            kind: Some(spark::data_type::Kind::Date(spark::data_type::Date {
                type_variation_reference: 0,
            })),
        }
    }
}

impl<Tz: chrono::TimeZone> ToDataType for chrono::DateTime<Tz> {
    fn to_proto_type(&self) -> spark::DataType {
        spark::DataType {
            kind: Some(spark::data_type::Kind::TimestampNtz(
                spark::data_type::TimestampNtz {
                    type_variation_reference: 0,
                },
            )),
        }
    }
}
