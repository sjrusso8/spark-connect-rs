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
