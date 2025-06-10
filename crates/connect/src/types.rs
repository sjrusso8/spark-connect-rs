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

//! Rust Types to Spark Types
#![allow(dead_code)]

use crate::spark;

/// Represents basic methods for a [SparkDataType]
pub trait SparkDataType {
    /// JSON representation of the object
    fn json(&self) -> String;

    fn type_name(&self) -> String {
        self.as_str_name()
    }

    fn json_value(&self) -> String {
        self.as_str_name()
    }

    fn as_str_name(&self) -> String {
        self.json()
    }
}

/// Representation of a Spark StructType
///
/// Used to create the a schema with other [DataType]
///
/// # Example:
///
/// ```
/// let schema = StructType::new(vec![
///        StructField {
///             name: "name",
///             data_type: DataType::String,
///             nullable: false,
///             metadata: None,
///         },
///         StructField {
///             name: "age",
///             data_type: DataType::Short,
///             nullable: true,
///             metadata: None,
///         },
///     ]);
/// ```
///
/// Complex types are also supported. The example below creates an [DataType::Array]
/// that contains a two [StructField].
///
/// # Example:
///
/// ```
/// let complex_schema = DataType::Array {
///         element_type: Box::new(DataType::Struct(Box::new(StructType::new(vec![
///             StructField {
///                 name: "col5",
///                 data_type: DataType::String,
///                 nullable: true,
///                 metadata: None,
///             },
///             StructField {
///                 name: "col6",
///                 data_type: DataType::Char(200),
///                 nullable: true,
///                 metadata: None,
///             },
///         ])))),
///         contains_null: true,
///     };
/// ```
#[derive(Clone, Debug)]
pub struct StructType {
    fields: Vec<StructField>,
}

impl StructType {
    /// Create an empty StructType
    pub fn empty() -> Self {
        StructType { fields: vec![] }
    }

    /// Create a new StructType from a vector of [StructField]
    pub fn new(fields: Vec<StructField>) -> Self {
        StructType { fields }
    }

    pub fn fields(&self) -> Vec<StructField> {
        self.fields.clone()
    }

    /// Append a new field onto the exist fields
    pub fn append(mut self, field: StructField) -> Self {
        self.fields.push(field);
        self
    }
}

impl From<StructType> for spark::DataType {
    fn from(value: StructType) -> spark::DataType {
        let fields: Vec<spark::data_type::StructField> = value
            .fields
            .iter()
            .map(|f| f.clone().to_proto_type())
            .collect();

        let struct_type = spark::data_type::Struct {
            fields,
            type_variation_reference: 0,
        };

        spark::DataType {
            kind: Some(spark::data_type::Kind::Struct(struct_type)),
        }
    }
}

impl SparkDataType for StructType {
    fn type_name(&self) -> String {
        String::from("struct")
    }

    fn as_str_name(&self) -> String {
        self.type_name()
    }

    fn json(&self) -> String {
        let fields: String = self
            .fields
            .iter()
            .map(|f| f.json())
            .collect::<Vec<_>>()
            .join(",");

        clean_json_brackets(format!("{{\"fields\":[{}],\"type\":\"struct\"}}", fields))
    }
}

/// A Field in a [StructType]
#[derive(Clone, Debug)]
pub struct StructField {
    pub name: &'static str,
    pub data_type: DataType,
    pub nullable: bool,
    pub metadata: Option<String>,
}

impl StructField {
    pub fn new(
        name: &'static str,
        data_type: DataType,
        nullable: Option<bool>,
        metadata: Option<String>,
    ) -> Self {
        StructField {
            name,
            data_type,
            nullable: nullable.unwrap_or(true),
            metadata,
        }
    }

    pub(crate) fn to_proto_type(&self) -> spark::data_type::StructField {
        let data_type = self.data_type.clone().to_proto_type();

        spark::data_type::StructField {
            name: self.name.to_string(),
            data_type: Some(data_type),
            nullable: self.nullable,
            metadata: self.metadata.clone(),
        }
    }
}

impl SparkDataType for StructField {
    fn json(&self) -> String {
        format!(
            "{{\"name\":\"{}\",\"type\":\"{}\",\"nullable\":{},\"metadata\":{{}}}}",
            self.name,
            self.data_type.json(),
            self.nullable
        )
    }
}

/// A set of DataTypes which represent Spark DataTypes.
///
/// These DataTypes variants are used to constructs a schema representation
///
/// # Example:
///
/// ```
/// let schema = StructType::new(vec![
///        StructField {
///             name: "name",
///             data_type: DataType::String,
///             nullable: false,
///             metadata: None,
///         },
///         StructField {
///             name: "age",
///             data_type: DataType::Short,
///             nullable: true,
///             metadata: None,
///         },
///     ]);
/// ```
#[derive(Clone, Debug)]
pub enum DataType {
    /// NullType
    Null,
    /// BinaryType
    Binary,
    /// BooleanType
    Boolean,
    /// ByteType
    Byte,
    /// ShortType
    Short,
    /// IntegerType
    Integer,
    /// LongType
    Long,
    /// FloatType
    Float,
    /// DoubleType
    Double,
    /// DecimalType with scale and precision
    Decimal {
        scale: Option<i32>,
        precision: Option<i32>,
    },
    /// StringType
    String,
    /// CharType with length
    Char(i32),
    /// VarCharType with length
    VarChar(i32),
    /// DateType
    Date,
    /// TimestampType
    Timestamp,
    /// TimestampNtzType
    TimestampNtz,
    /// CalendarIntervalType
    CalendarInterval,
    /// YearMonthIntervalType
    YearMonthInterval {
        start_field: Option<i32>,
        end_field: Option<i32>,
    },
    /// DayTimeInterval Type
    DayTimeInterval {
        start_field: Option<i32>,
        end_field: Option<i32>,
    },
    /// ArrayType
    Array {
        element_type: Box<DataType>,
        contains_null: bool,
    },
    /// MapType
    Map {
        key_type: Box<DataType>,
        value_type: Box<DataType>,
        value_contains_null: bool,
    },
    /// StructType
    Struct(Box<StructType>),
}

impl DataType {
    pub fn from_str_name(value: &str) -> DataType {
        match value.to_lowercase().as_str() {
            "bool" | "boolean" => DataType::Boolean,
            "int" | "integer" => DataType::Integer,
            "str" | "string" => DataType::String,
            _ => unimplemented!("not implemented"),
        }
    }

    pub fn to_proto_type(&self) -> spark::DataType {
        let type_variation_reference = 0;

        match self {
            Self::Null => spark::DataType {
                kind: Some(spark::data_type::Kind::Null(spark::data_type::Null {
                    type_variation_reference,
                })),
            },
            Self::Binary => spark::DataType {
                kind: Some(spark::data_type::Kind::Binary(spark::data_type::Binary {
                    type_variation_reference,
                })),
            },
            Self::Boolean => spark::DataType {
                kind: Some(spark::data_type::Kind::Boolean(spark::data_type::Boolean {
                    type_variation_reference,
                })),
            },
            Self::Byte => spark::DataType {
                kind: Some(spark::data_type::Kind::Byte(spark::data_type::Byte {
                    type_variation_reference,
                })),
            },
            Self::Short => spark::DataType {
                kind: Some(spark::data_type::Kind::Short(spark::data_type::Short {
                    type_variation_reference,
                })),
            },
            Self::Integer => spark::DataType {
                kind: Some(spark::data_type::Kind::Integer(spark::data_type::Integer {
                    type_variation_reference,
                })),
            },
            Self::Long => spark::DataType {
                kind: Some(spark::data_type::Kind::Long(spark::data_type::Long {
                    type_variation_reference,
                })),
            },
            Self::Float => spark::DataType {
                kind: Some(spark::data_type::Kind::Float(spark::data_type::Float {
                    type_variation_reference,
                })),
            },
            Self::Double => spark::DataType {
                kind: Some(spark::data_type::Kind::Double(spark::data_type::Double {
                    type_variation_reference,
                })),
            },
            Self::String => spark::DataType {
                kind: Some(spark::data_type::Kind::String(spark::data_type::String {
                    type_variation_reference,
                })),
            },
            Self::Date => spark::DataType {
                kind: Some(spark::data_type::Kind::Date(spark::data_type::Date {
                    type_variation_reference,
                })),
            },
            Self::Timestamp => spark::DataType {
                kind: Some(spark::data_type::Kind::Timestamp(
                    spark::data_type::Timestamp {
                        type_variation_reference,
                    },
                )),
            },
            Self::TimestampNtz => spark::DataType {
                kind: Some(spark::data_type::Kind::TimestampNtz(
                    spark::data_type::TimestampNtz {
                        type_variation_reference,
                    },
                )),
            },
            Self::CalendarInterval => spark::DataType {
                kind: Some(spark::data_type::Kind::CalendarInterval(
                    spark::data_type::CalendarInterval {
                        type_variation_reference,
                    },
                )),
            },
            Self::Decimal { scale, precision } => spark::DataType {
                kind: Some(spark::data_type::Kind::Decimal(spark::data_type::Decimal {
                    scale: *scale,
                    precision: *precision,
                    type_variation_reference,
                })),
            },
            Self::Char(length) => spark::DataType {
                kind: Some(spark::data_type::Kind::Char(spark::data_type::Char {
                    length: *length,
                    type_variation_reference,
                })),
            },
            Self::VarChar(length) => spark::DataType {
                kind: Some(spark::data_type::Kind::Char(spark::data_type::Char {
                    length: *length,
                    type_variation_reference,
                })),
            },
            Self::YearMonthInterval {
                start_field,
                end_field,
            } => spark::DataType {
                kind: Some(spark::data_type::Kind::YearMonthInterval(
                    spark::data_type::YearMonthInterval {
                        start_field: *start_field,
                        end_field: *end_field,
                        type_variation_reference,
                    },
                )),
            },
            Self::DayTimeInterval {
                start_field,
                end_field,
            } => spark::DataType {
                kind: Some(spark::data_type::Kind::DayTimeInterval(
                    spark::data_type::DayTimeInterval {
                        start_field: *start_field,
                        end_field: *end_field,
                        type_variation_reference,
                    },
                )),
            },
            Self::Array {
                element_type,
                contains_null,
            } => spark::DataType {
                kind: Some(spark::data_type::Kind::Array(Box::new(
                    spark::data_type::Array {
                        element_type: Some(Box::new(element_type.to_proto_type())),
                        contains_null: *contains_null,
                        type_variation_reference,
                    },
                ))),
            },
            Self::Map {
                key_type,
                value_type,
                value_contains_null,
            } => spark::DataType {
                kind: Some(spark::data_type::Kind::Map(Box::new(
                    spark::data_type::Map {
                        key_type: Some(Box::new(key_type.to_proto_type())),
                        value_type: Some(Box::new(value_type.to_proto_type())),
                        value_contains_null: *value_contains_null,
                        type_variation_reference,
                    },
                ))),
            },

            Self::Struct(val) => {
                let fields = val
                    .fields()
                    .iter()
                    .map(|f| f.clone().to_proto_type())
                    .collect();

                spark::DataType {
                    kind: Some(spark::data_type::Kind::Struct(spark::data_type::Struct {
                        fields,
                        type_variation_reference,
                    })),
                }
            }
        }
    }
}

impl SparkDataType for DataType {
    fn json(&self) -> String {
        match self {
            Self::Null => String::from("void"),
            Self::Binary => String::from("binary"),
            Self::Boolean => String::from("boolean"),
            Self::Byte => String::from("byte"),
            Self::Short => String::from("short"),
            Self::Integer => String::from("integer"),
            Self::Long => String::from("long"),
            Self::Float => String::from("float"),
            Self::Double => String::from("double"),
            Self::Decimal { scale, precision } => {
                format!(
                    "decimal({},{})",
                    scale.unwrap_or(10),
                    precision.unwrap_or(0)
                )
            }
            Self::String => String::from("string"),
            Self::Char(length) => format!("char({})", length),
            Self::VarChar(length) => format!("varchar({})", length),
            Self::Date => String::from("date"),

            Self::Timestamp => String::from("timestamp"),
            Self::TimestampNtz => String::from("timestamp_ntz"),
            Self::CalendarInterval => String::from("interval"),
            Self::YearMonthInterval {
                start_field,
                end_field,
            } => {
                let start = start_field.unwrap_or(0);
                let end = end_field.unwrap_or(1);
                match (start, end) {
                    (0, 0) => String::from("interval year"),
                    (0, 1) => String::from("interval month to year"),
                    (1, 1) => String::from("interval month"),
                    (1, 0) => String::from("interval year to month"),
                    (_, _) => unimplemented!("Invalid YearMonthInterval"),
                }
            }
            Self::DayTimeInterval {
                start_field,
                end_field,
            } => {
                let start = start_field.unwrap_or(0);
                let end = end_field.unwrap_or(3);
                match (start, end) {
                    (0, 0) => String::from("interval day"),
                    (0, 1) => String::from("interval day to hour"),
                    (0, 2) => String::from("interval day to minute"),
                    (0, 3) => String::from("interval day to second"),
                    (1, 0) => String::from("interval hour to day"),
                    (1, 1) => String::from("interval hour"),
                    (1, 2) => String::from("interval hour to minute"),
                    (1, 3) => String::from("interval hour to second"),
                    (2, 0) => String::from("interval minute to day"),
                    (2, 1) => String::from("interval minute to hour"),
                    (2, 2) => String::from("interval minute"),
                    (2, 3) => String::from("interval minute to second"),
                    (3, 0) => String::from("interval second to day"),
                    (3, 1) => String::from("interval second to hour"),
                    (3, 2) => String::from("interval second to minute"),
                    (3, 3) => String::from("interval second"),
                    (_, _) => unimplemented!("Invalid DayTimeInterval"),
                }
            }
            Self::Array {
                element_type,
                contains_null,
            } => clean_json_brackets(format!(
                "{{\"type\":\"array\",\"elementType\":{},\"containsNull\":{}}}",
                element_type.json(),
                contains_null
            )),
            Self::Map {
                key_type,
                value_type,
                value_contains_null,
            } => clean_json_brackets(format!(
                "{{\"type\":\"map\",\"keyType\":{},\"valueType\":{},\"valueContainsNull\":{}}}",
                key_type.json(),
                value_type.json(),
                value_contains_null
            )),
            Self::Struct(val) => val.json(),
        }
    }
}

impl From<DataType> for spark::DataType {
    fn from(value: DataType) -> spark::DataType {
        value.to_proto_type()
    }
}

macro_rules! impl_to_proto_type {
    ($type:ty, $inner_type:ident) => {
        impl From<$type> for spark::DataType {
            fn from(_value: $type) -> spark::DataType {
                spark::DataType {
                    kind: Some(spark::data_type::Kind::$inner_type(
                        spark::data_type::$inner_type {
                            type_variation_reference: 0,
                        },
                    )),
                }
            }
        }
    };
}
// Call the macro with the input pairs
impl_to_proto_type!(bool, Boolean);
impl_to_proto_type!(i16, Short);
impl_to_proto_type!(i32, Integer);
impl_to_proto_type!(i64, Long);
impl_to_proto_type!(isize, Long);
impl_to_proto_type!(f32, Float);
impl_to_proto_type!(f64, Double);
impl_to_proto_type!(&str, String);
impl_to_proto_type!(String, String);

impl From<&[u8]> for spark::DataType {
    fn from(_value: &[u8]) -> spark::DataType {
        spark::DataType {
            kind: Some(spark::data_type::Kind::Binary(spark::data_type::Binary {
                type_variation_reference: 0,
            })),
        }
    }
}

impl From<chrono::NaiveDate> for spark::DataType {
    fn from(_value: chrono::NaiveDate) -> spark::DataType {
        spark::DataType {
            kind: Some(spark::data_type::Kind::Date(spark::data_type::Date {
                type_variation_reference: 0,
            })),
        }
    }
}

impl<Tz: chrono::TimeZone> From<chrono::DateTime<Tz>> for spark::DataType {
    fn from(_value: chrono::DateTime<Tz>) -> spark::DataType {
        spark::DataType {
            kind: Some(spark::data_type::Kind::Timestamp(
                spark::data_type::Timestamp {
                    type_variation_reference: 0,
                },
            )),
        }
    }
}

impl From<chrono::NaiveDateTime> for spark::DataType {
    fn from(_value: chrono::NaiveDateTime) -> spark::DataType {
        spark::DataType {
            kind: Some(spark::data_type::Kind::TimestampNtz(
                spark::data_type::TimestampNtz {
                    type_variation_reference: 0,
                },
            )),
        }
    }
}

pub(crate) fn clean_json_brackets(json: String) -> String {
    json.replace("\"{", "{").replace("}\"", "}")
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_schema() {
        let complex_schema = DataType::Array {
            element_type: Box::new(DataType::Struct(Box::new(StructType::new(vec![
                StructField {
                    name: "col5",
                    data_type: DataType::String,
                    nullable: true,
                    metadata: None,
                },
                StructField {
                    name: "col6",
                    data_type: DataType::Char(200),
                    nullable: true,
                    metadata: None,
                },
            ])))),
            contains_null: true,
        };

        let schema = StructType::new(vec![
            StructField {
                name: "col1",
                data_type: DataType::Integer,
                nullable: false,
                metadata: None,
            },
            StructField {
                name: "col2",
                data_type: DataType::Short,
                nullable: false,
                metadata: None,
            },
            StructField {
                name: "col3",
                data_type: DataType::Array {
                    element_type: Box::new(DataType::String),
                    contains_null: true,
                },
                nullable: false,
                metadata: None,
            },
            StructField {
                name: "col4",
                data_type: complex_schema,
                nullable: true,
                metadata: None,
            },
            StructField {
                name: "col7",
                data_type: DataType::Map {
                    key_type: Box::new(DataType::String),
                    value_type: Box::new(DataType::Long),
                    value_contains_null: true,
                },
                nullable: true,
                metadata: None,
            },
        ]);

        let expected: String = "{\"fields\":[{\"name\":\"col1\",\"type\":\
            \"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\
            \"col2\",\"type\":\"short\",\"nullable\":false,\"metadata\":{}},{\
            \"name\":\"col3\",\"type\":{\"type\":\"array\",\"elementType\
            \":string,\"containsNull\":true},\"nullable\":false,\"metadata\
            \":{}},{\"name\":\"col4\",\"type\":{\"type\":\"array\",\"elementType\
            \":{\"fields\":[{\"name\":\"col5\",\"type\":\"string\",\"nullable\
            \":true,\"metadata\":{}},{\"name\":\"col6\",\"type\":\"char(200)\",\
            \"nullable\":true,\"metadata\":{}}],\"type\":\"struct\"},\"containsNull\
            \":true},\"nullable\":true,\"metadata\":{}},{\"name\":\"col7\",\
            \"type\":{\"type\":\"map\",\"keyType\":string,\"valueType\":long,\
            \"valueContainsNull\":true},\"nullable\":true,\"metadata\":{}}],\"type\":\"struct\"}"
            .into();

        assert_eq!(expected, schema.json());
    }
}
