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

//! DataFrameReader & DataFrameWriter representations

use std::collections::HashMap;

use crate::column::Column;
use crate::errors::SparkError;
use crate::expressions::VecExpression;
use crate::plan::LogicalPlanBuilder;
use crate::session::SparkSession;
use crate::spark;
use crate::types::{SparkDataType, StructType};
use crate::DataFrame;

use spark::write_operation::SaveMode;
use spark::write_operation_v2::Mode;
use spark::Expression;

/// A trait used to a create a DDL string or JSON string
///
/// Primarily used for [StructType] and Strings
pub trait ToSchema {
    fn to_schema(&self) -> String;
}

impl ToSchema for StructType {
    fn to_schema(&self) -> String {
        self.json()
    }
}

impl ToSchema for String {
    fn to_schema(&self) -> String {
        self.to_string()
    }
}

impl ToSchema for &str {
    fn to_schema(&self) -> String {
        self.to_string()
    }
}

/// A trait used to convert to the expected Spark Options for readwriters
///
/// This sets multiple options at once using a HashMap
pub trait ConfigOpts {
    fn to_options(&self) -> HashMap<String, String>;
}

#[macro_export]
macro_rules! define_file_options {
    (
        $(#[doc = $struct_d:tt])* // Struct-level documentation attributes
        $vis:vis struct $struct_name:ident {
            // Custom fields with options
            $(
                $field_name:ident : $field_type:ty,
                camel_case = $camel_case:expr
            )*$(,)*
        }
    ) => {

        $(#[doc = $struct_d])* // Apply struct documentation
        #[derive(Debug, Clone, Default)]
        $vis struct $struct_name {
            // Custom fields
            $(
               $field_name: Option<$field_type>,
            )*
        }

        impl $struct_name {
            // Builder methods for custom fields
            $(
                pub fn $field_name(mut self, value: $field_type) -> Self {
                    self.$field_name = Some(value);
                    self
                }
            )*
        }

        impl ConfigOpts for $struct_name {
            fn to_options(&self) -> HashMap<String, String> {
                let mut options: HashMap<String, String> = HashMap::new();

                $(
                    if let Some(value) = &self.$field_name {
                        options.insert(
                            $camel_case.to_string(),
                            value.to_string()
                        );
                    }
                )*

                options
            }
        }
    };
}

define_file_options! {
    /// A struct that represents options for configuring CSV file parsing.
    ///
    /// `CsvOptions` provides various settings to customize the reading of CSV files.
    /// It allows users to define the format, schema inference, handling of null values,
    /// and many other parsing behaviors. These options are used by the Spark DataFrame reader
    /// to correctly interpret and load CSV files into a DataFrame.
    ///
    /// # Fields
    /// - `path`: Specifies the file path or directory path to the CSV file(s).
    /// - `schema`: Defines the schema for the CSV data. If not provided, schema will be inferred based on the data.
    /// - `sep`: Character used to separate fields in the CSV file. Default is a comma (`,`).
    /// - `delimiter`: Alternative character used to separate fields in the CSV file.
    /// - `encoding`: Character encoding used for the CSV file. Default is `UTF-8`.
    /// - `quote`: Character used for quoting strings. Default is a double quote (`"`).
    /// - `quote_all`: Whether to quote all fields or only those containing special characters.
    /// - `escape`: Character used to escape quotes inside quoted strings. Default is a backslash (`\`).
    /// - `comment`: Character that denotes the start of a comment line in the file.
    /// - `header`: Whether the first line of the CSV file is a header that contains column names. Default is `false`.
    /// - `infer_schema`: Whether to infer the schema from the CSV data. Default is `false`.
    /// - `ignore_leading_white_space`: Whether to ignore leading white space in fields. Default is `false`.
    /// - `ignore_trailing_white_space`: Whether to ignore trailing white space in fields. Default is `false`.
    /// - `null_value`: String representation of a null value in the CSV file.
    /// - `nan_value`: String representation of a NaN value in the CSV file.
    /// - `positive_inf`: String representation of positive infinity in the CSV file.
    /// - `negative_inf`: String representation of negative infinity in the CSV file.
    /// - `date_format`: Format for parsing date fields in the CSV file.
    /// - `timestamp_format`: Format for parsing timestamp fields in the CSV file.
    /// - `timestamp_ntz_format`: Format for parsing timestamp fields without timezone information.
    /// - `enable_datetime_parsing_fallback`: Whether to enable fallback parsing for date and time formats.
    /// - `max_columns`: Maximum number of columns allowed in the CSV file.
    /// - `max_chars_per_column`: Maximum number of characters allowed per column.
    /// - `max_malformed_log_per_partition`: Maximum number of malformed rows logged per partition.
    /// - `mode`: Handling mode for corrupt/malformed records. Options are "PERMISSIVE", "DROPMALFORMED", and "FAILFAST".
    /// - `column_name_of_corrupt_record`: Name of the column to store malformed records.
    /// - `multi_line`: Whether to treat a row as spanning multiple lines. Default is `false`.
    /// - `char_to_escape_quote_escaping`: Sets a character for escaping quotes inside a quoted field.
    /// - `sampling_ratio`: Fraction of rows used for schema inference.
    /// - `enforce_schema`: Whether to force schema on the CSV file.
    /// - `empty_value`: Representation of an empty value in the CSV file.
    /// - `locale`: Locale of the CSV file, used for number formatting.
    /// - `line_sep`: Line separator character in the CSV file.
    /// - `unescaped_quote_handling`: How to handle unescaped quotes in quoted fields. Options are "STOP_AT_CLOSING_QUOTE" and "BACK_TO_DELIMITER".
    /// - `common` - Common file options that are shared across multiple file formats.
    pub struct CsvOptions {
        schema : String, camel_case = "schema"
        sep : String, camel_case = "sep"
        delimiter : String, camel_case = "delimiter"
        encoding : String, camel_case = "encoding"
        quote : String, camel_case = "quote"
        quote_all : bool, camel_case = "quoteAll"
        escape : String, camel_case = "escape"
        escape_quotes : bool, camel_case = "escapeQuotes"
        comment : String, camel_case = "comment"
        header : bool, camel_case = "header"
        infer_schema : bool, camel_case = "inferSchema"
        ignore_leading_white_space : bool, camel_case = "ignoreLeadingWhiteSpace"
        ignore_trailing_white_space : bool, camel_case = "ignoreTrailingWhiteSpace"
        null_value : String, camel_case = "nullValue"
        nan_value : String, camel_case = "nanValue"
        positive_inf : String, camel_case = "positiveInf"
        negative_inf : String, camel_case = "negativeInf"
        date_format : String, camel_case = "dateFormat"
        timestamp_format : String, camel_case = "timestampFormat"
        timestamp_ntz_format : String, camel_case = "timestampNtzFormat"
        enable_datetime_parsing_fallback : bool, camel_case = "enableDatetimeParsingFallback"
        max_columns : i32, camel_case = "maxColumns"
        max_chars_per_column : i32, camel_case = "maxCharsPerColumn"
        max_malformed_log_per_partition : i32, camel_case = "maxMalformedLogPerPartition"
        mode : String, camel_case = "mode"
        column_name_of_corrupt_record : String, camel_case = "columnNameOfCorruptRecord"
        multi_line : bool, camel_case = "multiLine"
        char_to_escape_quote_escaping : String, camel_case = "charToEscapeQuoteEscaping"
        sampling_ratio : f64, camel_case = "samplingRatio"
        prefer_date : bool, camel_case = "preferDate"
        enforce_schema : bool, camel_case = "enforceSchema"
        empty_value : String, camel_case = "emptyValue"
        locale : String, camel_case = "locale"
        line_sep : String, camel_case = "lineSep"
        unescaped_quote_handling : String, camel_case = "unescapedQuoteHandling"
        path_glob_filter : String, camel_case = "pathGlobFilter"
        recursive_file_lookup : bool, camel_case = "recursiveFileLookup"
        modified_before : String, camel_case = "modifiedBefore"
        modified_after : String, camel_case = "modifiedAfter"
        ignore_corrupt_files : bool, camel_case = "ignoreCorruptFiles"
        ignore_missing_files : bool, camel_case = "ignoreMissingFiles"
    }
}

define_file_options! {
    /// A struct that represents options for configuring JSON file parsing.
    ///
    /// By default, this supports `JSON Lines` (newline-delimited JSON).
    /// For single-record-per-file JSON, set the `multi_line` option to `true`.
    ///
    /// If the `schema` option is not specified, the input schema is inferred from the data.
    ///
    /// # Options
    ///
    /// - `schema`: An optional schema for the JSON data, either as a `StructType` or a DDL string.
    /// - `compression`: Compression codec to use when reading JSON files (e.g., `gzip`, `bzip2`).
    /// - `primitives_as_string`: Treat primitive types (e.g., integers, booleans) as strings.
    /// - `prefers_decimal`: Prefer parsing numbers as decimals rather than floating points.
    /// - `allow_comments`: Allow comments in JSON files (e.g., lines starting with `//` or `/* */`).
    /// - `allow_unquoted_field_names`: Allow field names without quotes.
    /// - `allow_single_quotes`: Allow the use of single quotes instead of double quotes for strings.
    /// - `allow_numeric_leading_zeros`: Allow numbers to have leading zeros (e.g., `007`).
    /// - `allow_backslash_escaping_any_character`: Allow backslashes to escape any character.
    /// - `mode`: The parsing mode (e.g., `PERMISSIVE`, `DROPMALFORMED`, `FAILFAST`).
    /// - `column_name_of_corrupt_record`: Name of the column where corrupted records are placed.
    /// - `date_format`: Custom date format (e.g., `yyyy-MM-dd`).
    /// - `timestamp_format`: Custom timestamp format (e.g., `yyyy-MM-dd HH:mm:ss`).
    /// - `multi_line`: Read multiline JSON files (e.g., when a single JSON object spans multiple lines).
    /// - `allow_unquoted_control_chars`: Allow unquoted control characters in JSON (e.g., ASCII control characters).
    /// - `line_sep`: Custom line separator (default is `\n`).
    /// - `sampling_ratio`: Fraction of the data used for schema inference (e.g., `0.1` for 10%).
    /// - `drop_field_if_all_null`: Drop fields that are `NULL` in all rows.
    /// - `encoding`: Character encoding (default is `UTF-8`).
    /// - `locale`: Locale for parsing dates and numbers (e.g., `en-US`).
    /// - `allow_non_numeric_numbers`: Allow special non-numeric numbers (e.g., `NaN`, `Infinity`).
    /// - `time_zone`: Time zone used for parsing dates and timestamps (e.g., `UTC`, `America/Los_Angeles`).
    /// - `timestamp_ntz_format`: Format for parsing timestamp without time zone (NTZ) values (e.g., `yyyy-MM-dd'T'HH:mm:ss`).
    /// - `enable_datetime_parsing_fallback`: Enable fallback mechanism for datetime parsing if the initial parsing fails.
    /// - `ignore_null_fields`: Ignore `NULL` fields in the JSON structure, treating them as absent.
    /// - `common` - Common file options that are shared across multiple file formats.
    ///
    /// # Example
    /// ```
    /// let options = JsonOptions::new()
    ///     .schema("name STRING, salary INT")
    ///     .multi_line(true)
    ///     .allow_comments(true)
    ///     .encoding("UTF-8")
    ///     .time_zone("UTC")
    ///     .compression("gzip");
    ///
    /// let df = spark.read().json(["/path/to/json"], options)?;
    /// ```
    pub struct JsonOptions {
        schema : String, camel_case = "schema"
        compression : String, camel_case = "compression"
        primitives_as_string : bool, camel_case = "primitivesAsString"
        prefers_decimal : bool, camel_case = "prefersDecimal"
        allow_comments : bool, camel_case = "allowComments"
        allow_unquoted_field_names : bool, camel_case = "allowUnquotedFieldNames"
        allow_single_quotes : bool, camel_case = "allowSingleQuotes"
        allow_numeric_leading_zeros : bool, camel_case = "allowNumericLeadingZeros"
        allow_backslash_escaping_any_character : bool, camel_case = "allowBackslashEscapingAnyCharacter"
        mode : String, camel_case = "mode"
        column_name_of_corrupt_record : String, camel_case = "columnNameOfCorruptRecord"
        date_format : String, camel_case = "dateFormat"
        timestamp_format : String, camel_case = "timestampFormat"
        multi_line : bool, camel_case = "multiLine"
        allow_unquoted_control_chars : bool, camel_case = "allowUnquotedControlChars"
        line_sep : String, camel_case = "lineSep"
        sampling_ratio : f64, camel_case = "samplingRatio"
        drop_field_if_all_null : bool, camel_case = "dropFieldIfAllNull"
        encoding : String, camel_case = "encoding"
        locale : String, camel_case = "locale"
        allow_non_numeric_numbers : bool, camel_case = "allowNonNumericNumbers"
        time_zone : String, camel_case = "timeZone"
        timestamp_ntz_format : String, camel_case = "timestampNtzFormat"
        enable_datetime_parsing_fallback : bool, camel_case = "enableDatetimeParsingFallback"
        ignore_null_fields : bool, camel_case = "ignoreNullFields"
        path_glob_filter : String, camel_case = "pathGlobFilter"
        recursive_file_lookup : bool, camel_case = "recursiveFileLookup"
        modified_before : String, camel_case = "modifiedBefore"
        modified_after : String, camel_case = "modifiedAfter"
        ignore_corrupt_files : bool, camel_case = "ignoreCorruptFiles"
        ignore_missing_files : bool, camel_case = "ignoreMissingFiles"
    }
}

define_file_options! {
    /// A struct that represents options for configuring ORC file parsing.
    ///
    /// # Options
    ///
    /// - `merge_schema`: Merge schemas from all ORC files.
    /// - `path_glob_filter`: A glob pattern to filter files by their path.
    /// - `common` - Common file options that are shared across multiple file formats.
    ///
    /// # Example
    /// ```
    /// let options = OrcOptions::new()
    ///     .merge_schema(true)
    ///     .common.path_glob_filter("*.orc".to_string())
    ///     .common.recursive_file_lookup(true);
    ///
    /// let df = spark.read().orc(["/path/to/orc"], options)?;
    /// ```
     pub struct OrcOptions {
        compression : String, camel_case = "compression"
        merge_schema : bool, camel_case = "mergeSchema"
        path_glob_filter : String, camel_case = "pathGlobFilter"
        recursive_file_lookup : bool, camel_case = "recursiveFileLookup"
        modified_before : String, camel_case = "modifiedBefore"
        modified_after : String, camel_case = "modifiedAfter"
        ignore_corrupt_files : bool, camel_case = "ignoreCorruptFiles"
        ignore_missing_files : bool, camel_case = "ignoreMissingFiles"
    }
}

define_file_options! {
    /// A struct that represents options for configuring Parquet file parsing.
    ///
    /// # Options
    ///
    /// - `merge_schema`: Merge schemas from all Parquet files.
    /// - `datetime_rebase_mode`: Controls how Spark handles rebase of datetime fields.
    /// - `int96_rebase_mode`: Controls how Spark handles rebase of INT96 fields.
    /// - `common`: Common file options that are shared across multiple file formats.
    ///
    /// # Example
    /// ```
    /// let options = ParquetOptions::new()
    ///     .merge_schema(true)
    ///     .common.path_glob_filter("*.parquet".to_string())
    ///     .common.recursive_file_lookup(true);
    ///
    /// let df = spark.read().parquet(["/path/to/parquet"], options)?;
    /// ```
    pub struct ParquetOptions {
        compression : String, camel_case = "compression"
        merge_schema : bool, camel_case = "mergeSchema"
        datetime_rebase_mode : String, camel_case = "datetimeRebaseMode"
        int96_rebase_mode : String, camel_case = "int96RebaseMode"
        path_glob_filter : String, camel_case = "pathGlobFilter"
        recursive_file_lookup : bool, camel_case = "recursiveFileLookup"
        modified_before : String, camel_case = "modifiedBefore"
        modified_after : String, camel_case = "modifiedAfter"
        ignore_corrupt_files : bool, camel_case = "ignoreCorruptFiles"
        ignore_missing_files : bool, camel_case = "ignoreMissingFiles"
    }
}

define_file_options! {
    /// A struct that represents options for configuring text file parsing.
    ///
    /// # Options
    ///
    /// - `whole_text`: Read the entire file as a single string.
    /// - `line_sep`: Define the line separator (default is `\n`).
    /// - `common` - Common file options that are shared across multiple file formats.
    ///
    /// # Example
    /// ```
    /// let options = TextOptions::new()
    ///     .whole_text(true)
    ///     .line_sep("\n".to_string())
    ///     .common.path_glob_filter("*.txt".to_string());
    ///
    /// let df = spark.read().text(["/path/to/text"], options)?;
    /// ```
    pub struct TextOptions {
        whole_text : bool, camel_case = "wholeText"
        line_sep : String, camel_case = "lineSep"
        path_glob_filter : String, camel_case = "pathGlobFilter"
        recursive_file_lookup : bool, camel_case = "recursiveFileLookup"
        modified_before : String, camel_case = "modifiedBefore"
        modified_after : String, camel_case = "modifiedAfter"
        ignore_corrupt_files : bool, camel_case = "ignoreCorruptFiles"
        ignore_missing_files : bool, camel_case = "ignoreMissingFiles"

    }
}

/// DataFrameReader represents the entrypoint to create a DataFrame
/// from a specific file format.
#[derive(Clone, Debug)]
pub struct DataFrameReader {
    spark_session: SparkSession,
    format: Option<String>,
    schema: Option<String>,
    read_options: HashMap<String, String>,
}

impl DataFrameReader {
    /// Create a new DataFrameReader with a [SparkSession]
    pub fn new(spark_session: SparkSession) -> Self {
        Self {
            spark_session,
            format: None,
            schema: None,
            read_options: HashMap::new(),
        }
    }

    /// Specifies the input data source format
    pub fn format(mut self, format: &str) -> Self {
        self.format = Some(format.to_string());
        self
    }

    /// Add an input option for the underlying data source
    pub fn option(mut self, key: &str, value: &str) -> Self {
        self.read_options.insert(key.to_string(), value.to_string());
        self
    }

    pub fn schema<T: ToSchema>(mut self, schema: T) -> Self {
        self.schema = Some(schema.to_schema());
        self
    }

    /// Set many input options based on an iterator of (key/value pairs) for the underlying data source
    pub fn options<I, K, V>(mut self, options: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        self.read_options = options
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_string(), v.as_ref().to_string()))
            .collect();

        self
    }

    /// Loads data from a data source and returns it as a [DataFrame]
    ///
    /// Example:
    /// ```rust
    /// let path = vec!["some/dir/path/on/the/remote/cluster/"];
    ///
    /// // returns a DataFrame from a csv file with a header from a the specific path
    /// let mut df = spark.read().format("csv").option("header", "true").load(path);
    /// ```
    pub fn load<'a, I>(self, paths: I) -> Result<DataFrame, SparkError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let read_type = Some(spark::relation::RelType::Read(spark::Read {
            is_streaming: false,
            read_type: Some(spark::read::ReadType::DataSource(spark::read::DataSource {
                format: self.format,
                schema: self.schema,
                options: self.read_options,
                paths: paths.into_iter().map(|p| p.to_string()).collect(),
                predicates: vec![],
            })),
        }));

        let relation = spark::Relation {
            common: Some(spark::RelationCommon {
                source_info: "NA".to_string(),
                plan_id: Some(1),
            }),
            rel_type: read_type,
        };

        let logical_plan = LogicalPlanBuilder::new(relation);

        Ok(DataFrame::new(self.spark_session, logical_plan))
    }

    /// Returns the specific table as a [DataFrame]
    ///
    /// # Arguments:
    /// * `table_name`: &str of the table name
    /// * `options`: (optional Hashmap) contains additional read options for a table
    ///
    pub fn table(
        self,
        table_name: &str,
        options: Option<HashMap<String, String>>,
    ) -> Result<DataFrame, SparkError> {
        let read_type = Some(spark::relation::RelType::Read(spark::Read {
            is_streaming: false,
            read_type: Some(spark::read::ReadType::NamedTable(spark::read::NamedTable {
                unparsed_identifier: table_name.to_string(),
                options: options.unwrap_or(self.read_options),
            })),
        }));

        let relation = spark::Relation {
            common: Some(spark::RelationCommon {
                source_info: "NA".to_string(),
                plan_id: Some(1),
            }),
            rel_type: read_type,
        };

        let logical_plan = LogicalPlanBuilder::new(relation);

        Ok(DataFrame::new(self.spark_session, logical_plan))
    }

    /// Reads data from CSV files with the specified options.
    pub fn csv<'a, C, I>(mut self, paths: I, config: C) -> Result<DataFrame, SparkError>
    where
        C: ConfigOpts,
        I: IntoIterator<Item = &'a str>,
    {
        self.format = Some("csv".to_string());
        self.read_options.extend(config.to_options());
        self.load(paths)
    }

    /// Reads data from JSON files with the specified options.
    pub fn json<'a, C, I>(mut self, paths: I, config: C) -> Result<DataFrame, SparkError>
    where
        C: ConfigOpts,
        I: IntoIterator<Item = &'a str>,
    {
        self.format = Some("json".to_string());
        self.read_options.extend(config.to_options());
        self.load(paths)
    }

    /// Reads data from ORC files with the specified options.
    pub fn orc<'a, C, I>(mut self, paths: I, config: C) -> Result<DataFrame, SparkError>
    where
        C: ConfigOpts,
        I: IntoIterator<Item = &'a str>,
    {
        self.format = Some("orc".to_string());
        self.read_options.extend(config.to_options());
        self.load(paths)
    }

    /// Reads data from Parquet files with the specified options.
    pub fn parquet<'a, C, I>(mut self, paths: I, config: C) -> Result<DataFrame, SparkError>
    where
        C: ConfigOpts,
        I: IntoIterator<Item = &'a str>,
    {
        self.format = Some("parquet".to_string());
        self.read_options.extend(config.to_options());
        self.load(paths)
    }

    /// Reads data from text files with the specified options.
    pub fn text<'a, C, I>(mut self, paths: I, config: C) -> Result<DataFrame, SparkError>
    where
        C: ConfigOpts,
        I: IntoIterator<Item = &'a str>,
    {
        self.format = Some("text".to_string());
        self.read_options.extend(config.to_options());
        self.load(paths)
    }
}

/// DataFrameWriter provides the ability to output a [DataFrame]
/// to a specific file format supported by Spark
pub struct DataFrameWriter {
    dataframe: DataFrame,
    format: Option<String>,
    mode: SaveMode,
    bucket_by: Option<spark::write_operation::BucketBy>,
    partition_by: Vec<String>,
    sort_by: Vec<String>,
    write_options: HashMap<String, String>,
}

impl DataFrameWriter {
    /// Create a new DataFrameWriter from a provided [DataFrame]
    ///
    /// # Defaults
    /// - `format`: None,
    /// - `mode`: [SaveMode::Overwrite],
    /// - `bucket_by`: None,
    /// - `partition_by`: vec![],
    /// - `sort_by`: vec![],
    /// - `write_options`: HashMap::new()
    ///
    pub fn new(dataframe: DataFrame) -> Self {
        Self {
            dataframe,
            format: None,
            mode: SaveMode::Overwrite,
            bucket_by: None,
            partition_by: vec![],
            sort_by: vec![],
            write_options: HashMap::new(),
        }
    }

    /// Target format to output the [DataFrame]
    pub fn format(mut self, format: &str) -> Self {
        self.format = Some(format.to_string());
        self
    }

    /// Specifies the behavior when data or table already exists
    ///
    /// # Arguments:
    /// - `mode`: [SaveMode] enum from the protobuf
    ///
    pub fn mode(mut self, mode: SaveMode) -> Self {
        self.mode = mode;
        self
    }

    /// Buckets the output by the given columns.
    /// If specified, the output is laid out on the file system
    /// similar to Hive’s bucketing scheme.
    pub fn bucket_by<'a, I>(mut self, num_buckets: i32, buckets: I) -> Self
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.bucket_by = Some(spark::write_operation::BucketBy {
            bucket_column_names: buckets.into_iter().map(|b| b.to_string()).collect(),
            num_buckets,
        });
        self
    }

    /// Sorts the output in each bucket by the given columns on the file system
    pub fn sort_by<'a, I>(mut self, cols: I) -> Self
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.sort_by = cols.into_iter().map(|col| col.to_string()).collect();
        self
    }

    /// Partitions the output by the given columns on the file system
    pub fn partition_by<'a, I>(mut self, cols: I) -> Self
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.sort_by = cols.into_iter().map(|col| col.to_string()).collect();
        self
    }

    /// Add an input option for the underlying data source
    pub fn option(mut self, key: &str, value: &str) -> Self {
        self.write_options
            .insert(key.to_string(), value.to_string());
        self
    }

    /// Set many input options based on an iterator of (key/value pairs) for the underlying data source
    pub fn options<I, K, V>(mut self, options: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        self.write_options = options
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_string(), v.as_ref().to_string()))
            .collect();
        self
    }

    /// Save the contents of the [DataFrame] to a data source.
    ///
    /// The data source is specified by the `format` and a set of `options`.
    pub async fn save(self, path: &str) -> Result<(), SparkError> {
        let write_command = spark::command::CommandType::WriteOperation(spark::WriteOperation {
            input: Some(self.dataframe.plan.clone().relation()),
            source: self.format,
            mode: self.mode.into(),
            sort_column_names: self.sort_by,
            partitioning_columns: self.partition_by,
            bucket_by: self.bucket_by,
            options: self.write_options,
            save_type: Some(spark::write_operation::SaveType::Path(path.to_string())),
        });

        let plan = LogicalPlanBuilder::plan_cmd(write_command);

        self.dataframe
            .spark_session
            .client()
            .execute_command(plan)
            .await
    }

    async fn save_table(self, table_name: &str, save_method: i32) -> Result<(), SparkError> {
        let write_command = spark::command::CommandType::WriteOperation(spark::WriteOperation {
            input: Some(self.dataframe.plan.relation()),
            source: self.format,
            mode: self.mode.into(),
            sort_column_names: self.sort_by,
            partitioning_columns: self.partition_by,
            bucket_by: self.bucket_by,
            options: self.write_options,
            save_type: Some(spark::write_operation::SaveType::Table(
                spark::write_operation::SaveTable {
                    table_name: table_name.to_string(),
                    save_method,
                },
            )),
        });

        let plan = LogicalPlanBuilder::plan_cmd(write_command);

        self.dataframe
            .spark_session
            .client()
            .execute_command(plan)
            .await
    }

    /// Saves the context of the [DataFrame] as the specified table.
    pub async fn save_as_table(self, table_name: &str) -> Result<(), SparkError> {
        self.save_table(table_name, 1).await
    }

    /// Inserts the content of the [DataFrame] to the specified table.
    ///
    /// It requires that the schema of the [DataFrame] is the same as the
    /// schema of the target table.
    ///
    /// Unlike `saveAsTable()`, this method ignores the column names and just uses
    /// position-based resolution
    pub async fn insert_into(self, table_name: &str) -> Result<(), SparkError> {
        self.save_table(table_name, 2).await
    }

    /// Writes the DataFrame to a CSV file with the specified options.
    pub async fn csv<C: ConfigOpts>(mut self, path: &str, config: C) -> Result<(), SparkError> {
        self.format = Some("csv".to_string());
        self.write_options.extend(config.to_options());
        self.save(path).await
    }

    /// Writes the DataFrame to a JSON file with the specified options.
    pub async fn json<C: ConfigOpts>(mut self, path: &str, config: C) -> Result<(), SparkError> {
        self.format = Some("json".to_string());
        self.write_options.extend(config.to_options());
        self.save(path).await
    }

    /// Writes the DataFrame to an ORC file with the specified options.
    pub async fn orc<C: ConfigOpts>(mut self, path: &str, config: C) -> Result<(), SparkError> {
        self.format = Some("orc".to_string());
        self.write_options.extend(config.to_options());
        self.save(path).await
    }

    /// Writes the DataFrame to a Parquet file with the specified options.
    pub async fn parquet<C: ConfigOpts>(mut self, path: &str, config: C) -> Result<(), SparkError> {
        self.format = Some("parquet".to_string());
        self.write_options.extend(config.to_options());
        self.save(path).await
    }

    /// Writes the DataFrame to a text file with the specified options.
    pub async fn text<C: ConfigOpts>(mut self, path: &str, config: C) -> Result<(), SparkError> {
        self.format = Some("text".to_string());
        self.write_options.extend(config.to_options());
        self.save(path).await
    }
}

pub struct DataFrameWriterV2 {
    dataframe: DataFrame,
    table: String,
    provider: Option<String>,
    options: HashMap<String, String>,
    properties: HashMap<String, String>,
    partitioning: Vec<Expression>,
    overwrite_condition: Option<Expression>,
}

impl DataFrameWriterV2 {
    pub fn new(dataframe: DataFrame, table: &str) -> Self {
        Self {
            dataframe,
            table: table.to_string(),
            provider: None,
            options: HashMap::new(),
            properties: HashMap::new(),
            partitioning: vec![],
            overwrite_condition: None,
        }
    }

    pub fn using(mut self, provider: &str) -> Self {
        self.provider.replace(provider.to_string());
        self
    }

    pub fn option(mut self, key: &str, value: &str) -> Self {
        self.options.insert(key.to_string(), value.to_string());
        self
    }

    pub fn options(mut self, provider: HashMap<String, String>) -> Self {
        self.options.extend(provider);
        self
    }

    pub fn table_property(mut self, property: &str, value: &str) -> Self {
        self.properties
            .insert(property.to_string(), value.to_string());
        self
    }

    pub fn partition_by<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Column>,
    {
        self.partitioning = VecExpression::from_iter(columns).expr;
        self
    }

    pub async fn create(self) -> Result<(), SparkError> {
        self.execute_write(Mode::Create).await
    }

    pub async fn replace(self) -> Result<(), SparkError> {
        self.execute_write(Mode::Replace).await
    }

    pub async fn create_or_replace(self) -> Result<(), SparkError> {
        self.execute_write(Mode::CreateOrReplace).await
    }

    pub async fn append(self) -> Result<(), SparkError> {
        self.execute_write(Mode::Append).await
    }

    pub async fn overwrite(self) -> Result<(), SparkError> {
        self.execute_write(Mode::Overwrite).await
    }

    pub async fn overwrite_partitions(self) -> Result<(), SparkError> {
        self.execute_write(Mode::OverwritePartitions).await
    }

    async fn execute_write(self, mode: Mode) -> Result<(), SparkError> {
        let mut builder = spark::WriteOperationV2 {
            input: Some(self.dataframe.plan.relation()),
            table_name: self.table,
            provider: self.provider,
            partitioning_columns: self.partitioning,
            options: self.options,
            table_properties: self.properties,
            mode: 0,
            overwrite_condition: self.overwrite_condition,
        };

        builder.set_mode(mode);

        let cmd = spark::command::CommandType::WriteOperationV2(builder);
        let plan = LogicalPlanBuilder::plan_cmd(cmd);

        self.dataframe
            .spark_session
            .client()
            .execute_command(plan)
            .await
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use std::sync::Arc;

    use crate::errors::SparkError;
    use crate::functions::*;
    use crate::types::{DataType, StructField, StructType};
    use crate::SparkSessionBuilder;

    use arrow::{
        array::{ArrayRef, StringArray},
        record_batch::RecordBatch,
    };

    async fn setup() -> SparkSession {
        println!("SparkSession Setup");

        let connection = "sc://127.0.0.1:15002/;user_id=rust_write;session_id=32c39012-896c-42fa-b487-969ee50e253b";

        SparkSessionBuilder::remote(connection)
            .build()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_dataframe_read() -> Result<(), SparkError> {
        let spark = setup().await;

        let path = ["/opt/spark/work-dir/datasets/people.csv"];

        let df = spark
            .read()
            .format("csv")
            .option("header", "True")
            .option("delimiter", ";")
            .load(path)?;

        let rows = df.collect().await?;

        assert_eq!(rows.num_rows(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_read_csv_with_options() -> Result<(), SparkError> {
        let spark = setup().await;

        let path = ["/opt/spark/work-dir/datasets/people.csv"];

        let opts = CsvOptions::default()
            .header(true)
            .null_value("NULL".to_string())
            .sep(";".to_string())
            .infer_schema(true)
            .encoding("UTF-8".to_string())
            .quote("\"".to_string())
            .escape("\\".to_string())
            .multi_line(false)
            .date_format("yyyy-MM-dd".to_string())
            .timestamp_format("yyyy-MM-dd'T'HH:mm:ss".to_string())
            .ignore_leading_white_space(true)
            .ignore_trailing_white_space(true)
            .mode("DROPMALFORMED".to_string());

        let df = spark.read().csv(path, opts)?;

        let rows = df.clone().collect().await?;

        assert_eq!(rows.num_rows(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_read_json_with_options() -> Result<(), SparkError> {
        let spark = setup().await;

        let path = ["/opt/spark/work-dir/datasets/employees.json"];

        let opts = JsonOptions::default()
            .schema("name STRING, salary INT".to_string())
            .multi_line(false)
            .allow_comments(false)
            .allow_unquoted_field_names(false)
            .primitives_as_string(false)
            .compression("gzip".to_string())
            .ignore_null_fields(true);

        let df = spark.read().json(path, opts)?;

        let rows = df.clone().collect().await?;

        assert_eq!(rows.num_rows(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_read_orc_with_options() -> Result<(), SparkError> {
        let spark = setup().await;

        let path = ["/opt/spark/work-dir/datasets/users.orc"];

        let opts = OrcOptions::default()
            .compression("snappy".to_string())
            .merge_schema(true)
            .path_glob_filter("*.orc".to_string())
            .recursive_file_lookup(true);

        let df = spark.read().orc(path, opts)?;

        let rows = df.clone().collect().await?;

        assert_eq!(rows.num_rows(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_read_parquet_with_options() -> Result<(), SparkError> {
        let spark = setup().await;

        let path = ["/opt/spark/work-dir/datasets/users.parquet"];

        let opts = ParquetOptions::default()
            .compression("snappy".to_string())
            .path_glob_filter("*.parquet".to_string())
            .recursive_file_lookup(true);

        let df = spark.read().parquet(path, opts)?;

        let rows = df.clone().collect().await?;

        assert_eq!(rows.num_rows(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_read_text_with_options() -> Result<(), SparkError> {
        let spark = setup().await;

        let path = ["/opt/spark/work-dir/datasets/people.txt"];

        let opts = TextOptions::default()
            .whole_text(false)
            .line_sep("\n".to_string())
            .path_glob_filter("*.txt".to_string())
            .recursive_file_lookup(true);

        let df = spark.read().text(path, opts)?;

        let rows = df.clone().collect().await?;

        assert_eq!(rows.num_rows(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_read_schema() -> Result<(), SparkError> {
        let spark = setup().await;

        let path = ["/opt/spark/work-dir/datasets/people.csv"];

        let schema = StructType::new(vec![
            StructField {
                name: "name",
                data_type: DataType::String,
                nullable: false,
                metadata: None,
            },
            StructField {
                name: "age",
                data_type: DataType::Short,
                nullable: true,
                metadata: None,
            },
        ]);

        let df = spark.read().format("json").schema(schema).load(path)?;

        let schema_datatype = df.print_schema(None).await?;

        let df = spark
            .read()
            .format("json")
            .schema("name string, age short")
            .load(path)?;

        let schema_ddl = df.print_schema(None).await?;

        assert_eq!(schema_datatype, schema_ddl);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_write() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark
            .range(None, 1000, 1, Some(16))
            .select_expr(vec!["id AS range_id"]);

        let path = "/tmp/range_id/";

        df.write()
            .mode(SaveMode::Overwrite)
            .format("csv")
            .option("header", "true")
            .save(path)
            .await?;

        let df = spark
            .read()
            .format("csv")
            .option("header", "true")
            .load([path])?;

        let records = df.select(vec![col("range_id")]).collect().await?;

        assert_eq!(records.num_rows(), 1000);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_write_table() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark
            .range(None, 1000, 1, Some(16))
            .select_expr(vec!["id AS range_id"]);

        df.write()
            .mode(SaveMode::Overwrite)
            .save_as_table("test_table")
            .await?;

        let df = spark.read().table("test_table", None)?;

        let records = df.select(vec![col("range_id")]).collect().await?;

        assert_eq!(records.num_rows(), 1000);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframev2_write() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark
            .range(None, 1000, 1, Some(16))
            .select_expr(vec!["id AS range_id"]);

        let table = "employees";

        df.write_to(table).using("csv").create().await?;

        let df = spark.table(table)?;

        let records = df.select(vec![col("range_id")]).collect().await?;

        assert_eq!(records.num_rows(), 1000);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_write_csv_with_options() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark
            .range(None, 1000, 1, Some(16))
            .select_expr(vec!["id AS range_id"]);

        let path = "/tmp/csv_with_options_rande_id/";

        let write_opts = CsvOptions::default()
            .header(true)
            .null_value("NULL".to_string());

        let _ = df
            .write()
            .mode(SaveMode::Overwrite)
            .csv(path, write_opts)
            .await;

        let path = ["/tmp/csv_with_options_rande_id/"];

        let read_opts = CsvOptions::default().header(true);

        let df = spark.read().csv(path, read_opts)?;

        let records = df.select(vec![col("range_id")]).collect().await?;

        assert_eq!(records.num_rows(), 1000);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_write_json_with_options() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark
            .range(None, 1000, 1, Some(16))
            .select_expr(vec!["id AS range_id"]);

        let path = "/tmp/json_with_options_rande_id/";

        let write_opts = JsonOptions::default()
            .multi_line(true)
            .allow_comments(false)
            .allow_unquoted_field_names(false)
            .primitives_as_string(false);

        let _ = df
            .write()
            .mode(SaveMode::Overwrite)
            .json(path, write_opts)
            .await;

        let path = ["/tmp/json_with_options_rande_id/"];

        let read_opts = JsonOptions::default();

        let df = spark.read().json(path, read_opts)?;

        let records = df.select(vec![col("range_id")]).collect().await?;

        assert_eq!(records.num_rows(), 1000);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_write_orc_with_options() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark
            .range(None, 1000, 1, Some(16))
            .select_expr(vec!["id AS range_id"]);

        let path = "/tmp/orc_with_options_rande_id/";

        let write_opts = OrcOptions::default();

        let _ = df
            .write()
            .mode(SaveMode::Overwrite)
            .orc(path, write_opts)
            .await;

        let path = ["/tmp/orc_with_options_rande_id/"];

        let read_opts = OrcOptions::default()
            .merge_schema(true)
            .path_glob_filter("*.orc".to_string())
            .recursive_file_lookup(true);

        let df = spark.read().orc(path, read_opts)?;

        let records = df.select(vec![col("range_id")]).collect().await?;

        assert_eq!(records.num_rows(), 1000);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_write_parquet_with_options() -> Result<(), SparkError> {
        let spark = setup().await;

        let df = spark
            .range(None, 1000, 1, Some(16))
            .select_expr(vec!["id AS range_id"]);

        let path = "/tmp/parquet_with_options_rande_id/";

        let write_opts = ParquetOptions::default()
            .datetime_rebase_mode("CORRECTED".to_string())
            .int96_rebase_mode("LEGACY".to_string());

        let _ = df
            .write()
            .mode(SaveMode::Overwrite)
            .parquet(path, write_opts)
            .await;

        let path = ["/tmp/parquet_with_options_rande_id/"];

        let read_opts = ParquetOptions::default()
            .merge_schema(false)
            .path_glob_filter("*.parquet".to_string())
            .recursive_file_lookup(true);

        let df = spark.read().parquet(path, read_opts)?;

        let records = df.select(vec![col("range_id")]).collect().await?;

        assert_eq!(records.num_rows(), 1000);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_write_text_with_options() -> Result<(), SparkError> {
        let spark = setup().await;

        let names: ArrayRef = Arc::new(StringArray::from(vec![
            Some("Michael"),
            Some("Andy"),
            Some("Justin"),
        ]));

        let data = RecordBatch::try_from_iter(vec![("names", names)])?;

        let df = spark.create_dataframe(&data)?;

        let path = "/tmp/text_with_options_rande_id/";

        let write_opts = TextOptions::default().whole_text(true);

        // Note that, in order to use write.text(), the dataframe
        // must have only one column else it will throw error.
        // Hence you need to covert all columns into single column.
        let _ = df
            .write()
            .mode(SaveMode::Overwrite)
            .text(path, write_opts)
            .await;

        let path = ["/tmp/text_with_options_rande_id/"];

        let read_opts = TextOptions::default()
            .whole_text(true)
            .line_sep("\n".to_string())
            .path_glob_filter("*.txt".to_string())
            .recursive_file_lookup(true);

        let df = spark.read().text(path, read_opts)?;

        let rows = df.clone().collect().await?;

        assert_eq!(rows.num_rows(), 3);
        Ok(())
    }
}
