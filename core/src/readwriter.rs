//! DataFrameReader & DataFrameWriter representations

use std::collections::HashMap;

use crate::errors::SparkError;
use crate::plan::LogicalPlanBuilder;
use crate::session::SparkSession;
use crate::spark;
use crate::types::{SparkDataType, StructType};
use crate::DataFrame;

use crate::expressions::ToVecExpr;
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

/// A trait used to handle individual options via [option]
/// and bulk options via [options]
///
/// This sets multiple options at once using a HashMap
pub trait ConfigOpts {
    fn to_options(&self) -> HashMap<String, String>;
}

/// A struct that represents options for configuring
/// CsvOptions for CSV file parsing
pub struct CsvOptions {
    pub header: Option<bool>,
    pub delimiter: Option<u8>,
    pub null_value: Option<String>,
}

impl CsvOptions {
    pub fn new() -> Self {
        Self {
            header: None,
            delimiter: None,
            null_value: None,
        }
    }

    pub fn header(mut self, value: bool) -> Self {
        self.header = Some(value);
        self
    }

    pub fn delimiter(mut self, value: u8) -> Self {
        self.delimiter = Some(value);
        self
    }

    pub fn null_value(mut self, value: &str) -> Self {
        self.null_value = Some(value.to_string());
        self
    }
}

impl ConfigOpts for CsvOptions {
    fn to_options(&self) -> HashMap<String, String> {
        let mut options: HashMap<String, String> = HashMap::new();

        if let Some(header) = self.header {
            options.insert("header".to_string(), header.to_string());
        }

        if let Some(delimiter) = self.delimiter {
            options.insert("delimiter".to_string(), (delimiter as char).to_string());
        }

        if let Some(null_value) = &self.null_value {
            options.insert("null_value".to_string(), null_value.to_string());
        }

        options
    }
}

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
/// - `primitives_as_string`: Treat primitive types (e.g., integers, booleans) as strings.
/// - `prefers_decimal`: Prefer parsing numbers as decimals rather than floating points.
/// - `allow_comments`: Allow comments in JSON files (e.g., lines starting with `//` or `/* */`).
/// - `allow_unquoted_field_names`: Allow field names without quotes.
/// - `allow_single_quotes`: Allow the use of single quotes instead of double quotes for strings.
/// - `allow_numeric_leading_zero`: Allow numbers to have leading zeros (e.g., `007`).
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
/// - `path_glob_filter`: A glob pattern to filter files by their path.
/// - `recursive_file_lookup`: Enable recursive search for files in directories.
/// - `allow_non_numeric_numbers`: Allow special non-numeric numbers (e.g., `NaN`, `Infinity`).
///
/// # Example
/// ```
/// let options = JsonOptions::new()
///     .schema("name STRING, salary INT")
///     .multi_line(true)
///     .allow_comments(true)
///     .encoding("UTF-8".to_string());
///
/// let df = spark.read().json("/path/to/json", options)?;
/// ```
#[derive(Debug)]
pub struct JsonOptions {
    pub schema: Option<String>,
    pub primitives_as_string: Option<bool>,
    pub prefers_decimal: Option<bool>,
    pub allow_comments: Option<bool>,
    pub allow_unquoted_field_names: Option<bool>,
    pub allow_single_quotes: Option<bool>,
    pub allow_numeric_leading_zero: Option<bool>,
    pub allow_backslash_escaping_any_character: Option<bool>,
    pub mode: Option<String>,
    pub column_name_of_corrupt_record: Option<String>,
    pub date_format: Option<String>,
    pub timestamp_format: Option<String>,
    pub multi_line: Option<bool>,
    pub allow_unquoted_control_chars: Option<bool>,
    pub line_sep: Option<String>,
    pub sampling_ratio: Option<f64>,
    pub drop_field_if_all_null: Option<bool>,
    pub encoding: Option<String>,
    pub locale: Option<String>,
    pub path_glob_filter: Option<bool>,
    pub recursive_file_lookup: Option<bool>,
    pub allow_non_numeric_numbers: Option<bool>,
}

impl JsonOptions {
    pub fn new() -> Self {
        Self {
            schema: None,
            primitives_as_string: None,
            prefers_decimal: None,
            allow_comments: None,
            allow_unquoted_field_names: None,
            allow_single_quotes: None,
            allow_numeric_leading_zero: None,
            allow_backslash_escaping_any_character: None,
            mode: None,
            column_name_of_corrupt_record: None,
            date_format: None,
            timestamp_format: None,
            multi_line: None,
            allow_unquoted_control_chars: None,
            line_sep: None,
            sampling_ratio: None,
            drop_field_if_all_null: None,
            encoding: None,
            locale: None,
            path_glob_filter: None,
            recursive_file_lookup: None,
            allow_non_numeric_numbers: None,
        }
    }

    pub fn schema(mut self, value: &str) -> Self {
        self.schema = Some(value.to_string());
        self
    }

    pub fn primitives_as_string(mut self, value: bool) -> Self {
        self.primitives_as_string = Some(value);
        self
    }

    pub fn prefers_decimal(mut self, value: bool) -> Self {
        self.prefers_decimal = Some(value);
        self
    }

    pub fn allow_comments(mut self, value: bool) -> Self {
        self.allow_comments = Some(value);
        self
    }

    pub fn allow_unquoted_field_names(mut self, value: bool) -> Self {
        self.allow_unquoted_field_names = Some(value);
        self
    }

    pub fn allow_single_quotes(mut self, value: bool) -> Self {
        self.allow_single_quotes = Some(value);
        self
    }

    pub fn allow_numeric_leading_zero(mut self, value: bool) -> Self {
        self.allow_numeric_leading_zero = Some(value);
        self
    }

    pub fn allow_backslash_escaping_any_character(mut self, value: bool) -> Self {
        self.allow_backslash_escaping_any_character = Some(value);
        self
    }

    pub fn mode(mut self, value: &str) -> Self {
        self.mode = Some(value.to_string());
        self
    }

    pub fn column_name_of_corrupt_record(mut self, value: &str) -> Self {
        self.column_name_of_corrupt_record = Some(value.to_string());
        self
    }

    pub fn date_format(mut self, value: &str) -> Self {
        self.date_format = Some(value.to_string());
        self
    }

    pub fn timestamp_format(mut self, value: &str) -> Self {
        self.timestamp_format = Some(value.to_string());
        self
    }

    pub fn multi_line(mut self, value: bool) -> Self {
        self.multi_line = Some(value);
        self
    }

    pub fn allow_unquoted_control_chars(mut self, value: bool) -> Self {
        self.allow_unquoted_control_chars = Some(value);
        self
    }

    pub fn line_sep(mut self, value: &str) -> Self {
        self.line_sep = Some(value.to_string());
        self
    }

    pub fn sampling_ratio(mut self, value: f64) -> Self {
        self.sampling_ratio = Some(value);
        self
    }

    pub fn drop_field_if_all_null(mut self, value: bool) -> Self {
        self.drop_field_if_all_null = Some(value);
        self
    }

    pub fn encoding(mut self, value: &str) -> Self {
        self.encoding = Some(value.to_string());
        self
    }

    pub fn locale(mut self, value: &str) -> Self {
        self.locale = Some(value.to_string());
        self
    }

    pub fn path_glob_filter(mut self, value: bool) -> Self {
        self.path_glob_filter = Some(value);
        self
    }

    pub fn recursive_file_lookup(mut self, value: bool) -> Self {
        self.recursive_file_lookup = Some(value);
        self
    }

    pub fn allow_non_numeric_numbers(mut self, value: bool) -> Self {
        self.allow_non_numeric_numbers = Some(value);
        self
    }
}

impl ConfigOpts for JsonOptions {
    fn to_options(&self) -> HashMap<String, String> {
        let mut options: HashMap<String, String> = HashMap::new();

        if let Some(schema) = &self.schema {
            options.insert("schema".to_string(), schema.clone());
        }

        if let Some(primitives_as_string) = self.primitives_as_string {
            options.insert(
                "primitives_as_string".to_string(),
                primitives_as_string.to_string(),
            );
        }

        if let Some(prefers_decimal) = self.prefers_decimal {
            options.insert("prefers_decimal".to_string(), prefers_decimal.to_string());
        }

        if let Some(allow_comments) = self.allow_comments {
            options.insert("allow_comments".to_string(), allow_comments.to_string());
        }

        if let Some(allow_unquoted_field_names) = self.allow_unquoted_field_names {
            options.insert(
                "allow_unquoted_field_names".to_string(),
                allow_unquoted_field_names.to_string(),
            );
        }

        if let Some(allow_single_quotes) = self.allow_single_quotes {
            options.insert(
                "allow_single_quotes".to_string(),
                allow_single_quotes.to_string(),
            );
        }

        if let Some(allow_numeric_leading_zero) = self.allow_numeric_leading_zero {
            options.insert(
                "allow_numeric_leading_zero".to_string(),
                allow_numeric_leading_zero.to_string(),
            );
        }

        if let Some(allow_backslash_escaping_any_character) =
            self.allow_backslash_escaping_any_character
        {
            options.insert(
                "allow_backslash_escaping_any_character".to_string(),
                allow_backslash_escaping_any_character.to_string(),
            );
        }

        if let Some(mode) = &self.mode {
            options.insert("mode".to_string(), mode.clone());
        }

        if let Some(column_name_of_corrupt_record) = &self.column_name_of_corrupt_record {
            options.insert(
                "column_name_of_corrupt_record".to_string(),
                column_name_of_corrupt_record.clone(),
            );
        }

        if let Some(date_format) = &self.date_format {
            options.insert("date_format".to_string(), date_format.clone());
        }

        if let Some(timestamp_format) = &self.timestamp_format {
            options.insert("timestamp_format".to_string(), timestamp_format.clone());
        }

        if let Some(multi_line) = self.multi_line {
            options.insert("multi_line".to_string(), multi_line.to_string());
        }

        if let Some(allow_unquoted_control_chars) = self.allow_unquoted_control_chars {
            options.insert(
                "allow_unquoted_control_chars".to_string(),
                allow_unquoted_control_chars.to_string(),
            );
        }

        if let Some(line_sep) = &self.line_sep {
            options.insert("line_sep".to_string(), line_sep.clone());
        }

        if let Some(sampling_ratio) = self.sampling_ratio {
            options.insert("sampling_ratio".to_string(), sampling_ratio.to_string());
        }

        if let Some(drop_field_if_all_null) = self.drop_field_if_all_null {
            options.insert(
                "drop_field_if_all_null".to_string(),
                drop_field_if_all_null.to_string(),
            );
        }

        if let Some(encoding) = &self.encoding {
            options.insert("encoding".to_string(), encoding.clone());
        }

        if let Some(locale) = &self.locale {
            options.insert("locale".to_string(), locale.clone());
        }

        if let Some(path_glob_filter) = self.path_glob_filter {
            options.insert("path_glob_filter".to_string(), path_glob_filter.to_string());
        }

        if let Some(recursive_file_lookup) = self.recursive_file_lookup {
            options.insert(
                "recursive_file_lookup".to_string(),
                recursive_file_lookup.to_string(),
            );
        }

        if let Some(allow_non_numeric_numbers) = self.allow_non_numeric_numbers {
            options.insert(
                "allow_non_numeric_numbers".to_string(),
                allow_non_numeric_numbers.to_string(),
            );
        }

        options
    }
}

/// A struct that represents options for configuring ORC file parsing.
///
/// # Options
///
/// - `merge_schema`: Merge schemas from all ORC files.
/// - `path_glob_filter`: A glob pattern to filter files by their path.
/// - `recursive_file_lookup`: Enable recursive search for files in directories.
/// - `modified_before`: Only include files modified before a given timestamp.
/// - `modified_after`: Only include files modified after a given timestamp.
///
/// # Example
/// ```
/// let options = OrcOptions::new()
///     .merge_schema(true)
///     .path_glob_filter("*.orc".to_string())
///     .recursive_file_lookup(true);
///
/// let df = spark.read().orc("/path/to/orc", options)?;
/// ```
#[derive(Debug)]
pub struct OrcOptions {
    pub merge_schema: Option<bool>,
    pub path_glob_filter: Option<String>,
    pub recursive_file_lookup: Option<bool>,
    pub modified_before: Option<String>,
    pub modified_after: Option<String>,
}

impl OrcOptions {
    pub fn new() -> Self {
        OrcOptions {
            merge_schema: None,
            path_glob_filter: None,
            recursive_file_lookup: None,
            modified_before: None,
            modified_after: None,
        }
    }

    pub fn merge_schema(mut self, value: bool) -> Self {
        self.merge_schema = Some(value);
        self
    }

    pub fn path_glob_filter(mut self, value: &str) -> Self {
        self.path_glob_filter = Some(value.to_string());
        self
    }

    pub fn recursive_file_lookup(mut self, value: bool) -> Self {
        self.recursive_file_lookup = Some(value);
        self
    }

    pub fn modified_before(mut self, value: &str) -> Self {
        self.modified_before = Some(value.to_string());
        self
    }

    pub fn modified_after(mut self, value: &str) -> Self {
        self.modified_after = Some(value.to_string());
        self
    }
}

impl ConfigOpts for OrcOptions {
    fn to_options(&self) -> HashMap<String, String> {
        let mut options: HashMap<String, String> = HashMap::new();

        if let Some(merge_schema) = self.merge_schema {
            options.insert("merge_schema".to_string(), merge_schema.to_string());
        }

        if let Some(path_glob_filter) = &self.path_glob_filter {
            options.insert("path_glob_filter".to_string(), path_glob_filter.to_string());
        }

        if let Some(recursive_file_lookup) = self.recursive_file_lookup {
            options.insert(
                "recursive_file_lookup".to_string(),
                recursive_file_lookup.to_string(),
            );
        }

        if let Some(modified_before) = &self.modified_before {
            options.insert("modified_before".to_string(), modified_before.to_string());
        }

        if let Some(modified_after) = &self.modified_after {
            options.insert("modified_after".to_string(), modified_after.to_string());
        }

        options
    }
}

/// A struct that represents options for configuring Parquet file parsing.
///
/// # Options
///
/// - `merge_schema`: Merge schemas from all Parquet files.
/// - `path_glob_filter`: A glob pattern to filter files by their path.
/// - `recursive_file_lookup`: Enable recursive search for files in directories.
/// - `modified_before`: Only include files modified before a given timestamp.
/// - `modified_after`: Only include files modified after a given timestamp.
/// - `datetime_rebase_mode`: Controls how Spark handles rebase of datetime fields.
/// - `int96_rebase_mode`: Controls how Spark handles rebase of INT96 fields.
///
/// # Example
/// ```
/// let options = ParquetOptions::new()
///     .merge_schema(true)
///     .path_glob_filter("*.parquet".to_string())
///     .recursive_file_lookup(true);
///
/// let df = spark.read().parquet("/path/to/parquet", options)?;
/// ```
#[derive(Debug)]
pub struct ParquetOptions {
    pub merge_schema: Option<bool>,
    pub path_glob_filter: Option<String>,
    pub recursive_file_lookup: Option<bool>,
    pub modified_before: Option<String>,
    pub modified_after: Option<String>,
    pub datetime_rebase_mode: Option<String>,
    pub int96_rebase_mode: Option<String>,
}

impl ParquetOptions {
    pub fn new() -> Self {
        Self {
            merge_schema: None,
            path_glob_filter: None,
            recursive_file_lookup: None,
            modified_before: None,
            modified_after: None,
            datetime_rebase_mode: None,
            int96_rebase_mode: None,
        }
    }

    pub fn merge_schema(mut self, value: bool) -> Self {
        self.merge_schema = Some(value);
        self
    }

    pub fn path_glob_filter(mut self, value: &str) -> Self {
        self.path_glob_filter = Some(value.to_string());
        self
    }

    pub fn recursive_file_lookup(mut self, value: bool) -> Self {
        self.recursive_file_lookup = Some(value);
        self
    }

    pub fn modified_before(mut self, value: &str) -> Self {
        self.modified_before = Some(value.to_string());
        self
    }

    pub fn modified_after(mut self, value: &str) -> Self {
        self.modified_after = Some(value.to_string());
        self
    }

    pub fn datetime_rebase_mode(mut self, value: &str) -> Self {
        self.datetime_rebase_mode = Some(value.to_string());
        self
    }

    pub fn int96_rebase_mode(mut self, value: &str) -> Self {
        self.int96_rebase_mode = Some(value.to_string());
        self
    }
}

impl ConfigOpts for ParquetOptions {
    fn to_options(&self) -> HashMap<String, String> {
        let mut options: HashMap<String, String> = HashMap::new();

        if let Some(merge_schema) = self.merge_schema {
            options.insert("merge_schema".to_string(), merge_schema.to_string());
        }

        if let Some(path_glob_filter) = &self.path_glob_filter {
            options.insert("path_glob_filter".to_string(), path_glob_filter.to_string());
        }

        if let Some(recursive_file_lookup) = self.recursive_file_lookup {
            options.insert(
                "recursive_file_lookup".to_string(),
                recursive_file_lookup.to_string(),
            );
        }

        if let Some(modified_before) = &self.modified_before {
            options.insert("modified_before".to_string(), modified_before.to_string());
        }

        if let Some(modified_after) = &self.modified_after {
            options.insert("modified_after".to_string(), modified_after.to_string());
        }

        if let Some(datetime_rebase_mode) = &self.datetime_rebase_mode {
            options.insert(
                "datetime_rebase_mode".to_string(),
                datetime_rebase_mode.to_string(),
            );
        }

        if let Some(int96_rebase_mode) = &self.int96_rebase_mode {
            options.insert(
                "int96_rebase_mode".to_string(),
                int96_rebase_mode.to_string(),
            );
        }

        options
    }
}

/// A struct that represents options for configuring text file parsing.
///
/// # Options
///
/// - `whole_text`: Read the entire file as a single string.
/// - `line_sep`: Define the line separator (default is `\n`).
/// - `path_glob_filter`: A glob pattern to filter files by their path.
/// - `recursive_file_lookup`: Enable recursive search for files in directories.
/// - `modified_before`: Only include files modified before a given timestamp.
/// - `modified_after`: Only include files modified after a given timestamp.
///
/// # Example
/// ```
/// let options = TextOptions::new()
///     .whole_text(true)
///     .line_sep("\n".to_string())
///     .path_glob_filter("*.txt".to_string());
///
/// let df = spark.read().text("/path/to/text", options)?;
/// ```
#[derive(Debug)]
pub struct TextOptions {
    pub whole_text: Option<bool>,
    pub line_sep: Option<String>,
    pub path_glob_filter: Option<String>,
    pub recursive_file_lookup: Option<bool>,
    pub modified_before: Option<String>,
    pub modified_after: Option<String>,
}

impl TextOptions {
    pub fn new() -> Self {
        Self {
            whole_text: None,
            line_sep: None,
            path_glob_filter: None,
            recursive_file_lookup: None,
            modified_before: None,
            modified_after: None,
        }
    }

    pub fn whole_text(mut self, value: bool) -> Self {
        self.whole_text = Some(value);
        self
    }

    pub fn line_sep(mut self, value: &str) -> Self {
        self.line_sep = Some(value.to_string());
        self
    }

    pub fn path_glob_filter(mut self, value: &str) -> Self {
        self.path_glob_filter = Some(value.to_string());
        self
    }

    pub fn recursive_file_lookup(mut self, value: bool) -> Self {
        self.recursive_file_lookup = Some(value);
        self
    }

    pub fn modified_before(mut self, value: &str) -> Self {
        self.modified_before = Some(value.to_string());
        self
    }

    pub fn modified_after(mut self, value: &str) -> Self {
        self.modified_after = Some(value.to_string());
        self
    }
}

impl ConfigOpts for TextOptions {
    fn to_options(&self) -> HashMap<String, String> {
        let mut options: HashMap<String, String> = HashMap::new();

        if let Some(whole_text) = self.whole_text {
            options.insert("whole_text".to_string(), whole_text.to_string());
        }

        if let Some(line_sep) = &self.line_sep {
            options.insert("line_sep".to_string(), line_sep.to_string());
        }

        if let Some(path_glob_filter) = &self.path_glob_filter {
            options.insert("path_glob_filter".to_string(), path_glob_filter.to_string());
        }

        if let Some(recursive_file_lookup) = &self.recursive_file_lookup {
            options.insert(
                "recursive_file_lookup".to_string(),
                recursive_file_lookup.to_string(),
            );
        }

        if let Some(modified_before) = &self.modified_before {
            options.insert("modified_before".to_string(), modified_before.to_string());
        }

        if let Some(modified_after) = &self.modified_after {
            options.insert("modified_after".to_string(), modified_after.to_string());
        }

        options
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
    pub async fn insert_tnto(self, table_name: &str) -> Result<(), SparkError> {
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

    pub fn partition_by<T: ToVecExpr>(mut self, columns: T) -> Self {
        self.partitioning = columns.to_vec_expr();
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

        let mut opts = CsvOptions::new();

        opts.header = Some(true);
        opts.delimiter = Some(b';');
        opts.null_value = Some("NULL".to_string());

        let df = spark.read().csv(path, opts)?;

        let rows = df.clone().collect().await?;

        assert_eq!(rows.num_rows(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_read_json_with_options() -> Result<(), SparkError> {
        let spark = setup().await;

        let path = ["/opt/spark/work-dir/datasets/employees.json"];

        let mut opts = JsonOptions::new();

        opts.schema = Some("name STRING, salary INT".to_string());
        opts.multi_line = Some(true);
        opts.allow_comments = Some(false);
        opts.allow_unquoted_field_names = Some(false);
        opts.primitives_as_string = Some(false);

        let df = spark.read().json(path, opts)?;

        let rows = df.clone().collect().await?;

        assert_eq!(rows.num_rows(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_read_orc_with_options() -> Result<(), SparkError> {
        let spark = setup().await;

        let path = ["/opt/spark/work-dir/datasets/users.orc"];

        let mut opts = OrcOptions::new();

        opts.merge_schema = Some(true);
        opts.path_glob_filter = Some("*.orc".to_string());
        opts.recursive_file_lookup = Some(true);

        let df = spark.read().orc(path, opts)?;

        let rows = df.clone().collect().await?;

        assert_eq!(rows.num_rows(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_read_parquet_with_options() -> Result<(), SparkError> {
        let spark = setup().await;

        let path = ["/opt/spark/work-dir/datasets/users.parquet"];

        let mut opts = ParquetOptions::new();

        opts.merge_schema = Some(true);
        opts.path_glob_filter = Some("*.parquet".to_string());
        opts.recursive_file_lookup = Some(true);

        let df = spark.read().parquet(path, opts)?;

        let rows = df.clone().collect().await?;

        assert_eq!(rows.num_rows(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_read_text_with_options() -> Result<(), SparkError> {
        let spark = setup().await;

        let path = ["/opt/spark/work-dir/datasets/people.txt"];

        let mut opts = TextOptions::new();

        opts.whole_text = Some(true);
        opts.line_sep = Some("\n".to_string());
        opts.path_glob_filter = Some("*.txt".to_string());
        opts.recursive_file_lookup = Some(true);

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

        let path = "/tmp/range_id/";

        let mut write_opts = CsvOptions::new();

        write_opts.header = Some(true);
        write_opts.null_value = Some("NULL".to_string());

        df.write()
            .mode(SaveMode::Overwrite)
            .csv(path, write_opts)
            .await;

        let path = ["/tmp/range_id/"];

        let mut read_opts = CsvOptions::new();

        read_opts.header = Some(true);

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

        let path = "/tmp/range_id/";

        let mut write_opts = JsonOptions::new();

        write_opts.multi_line = Some(true);
        write_opts.allow_comments = Some(false);
        write_opts.allow_unquoted_field_names = Some(false);
        write_opts.primitives_as_string = Some(false);

        df.write()
            .mode(SaveMode::Overwrite)
            .json(path, write_opts)
            .await;

        let path = ["/tmp/range_id/"];

        let read_opts = JsonOptions::new();

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

        let path = "/tmp/range_id/";

        let write_opts = OrcOptions::new();

        df.write()
            .mode(SaveMode::Overwrite)
            .orc(path, write_opts)
            .await;

        let path = ["/tmp/range_id/"];

        let mut read_opts = OrcOptions::new();

        read_opts.merge_schema = Some(true);
        read_opts.path_glob_filter = Some("*.orc".to_string());
        read_opts.recursive_file_lookup = Some(true);

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

        let path = "/tmp/range_id/";

        let mut write_opts = ParquetOptions::new();

        // Configure datetime rebase mode (options could be "EXCEPTION", "LEGACY", or "CORRECTED").
        write_opts.datetime_rebase_mode = Some("CORRECTED".to_string());

        // Configure int96 rebase mode (options could be "EXCEPTION", "LEGACY", or "CORRECTED").
        write_opts.int96_rebase_mode = Some("LEGACY".to_string());

        df.write()
            .mode(SaveMode::Overwrite)
            .parquet(path, write_opts)
            .await;

        let path = ["/tmp/range_id/"];

        let mut read_opts = ParquetOptions::new();

        read_opts.merge_schema = Some(true);
        read_opts.path_glob_filter = Some("*.parquet".to_string());
        read_opts.recursive_file_lookup = Some(true);

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

        df.clone().show(Some(100), None, None).await;

        let path = "/tmp/text_data/";

        let mut write_opts = TextOptions::new();

        write_opts.whole_text = Some(true);

        // Note that, in order to use write.text(), the dataframe
        // must have only one column else it will throw error.
        // Hence you need to covert all columns into single column.
        df.write()
            .mode(SaveMode::Overwrite)
            .text(path, write_opts)
            .await;

        let path = ["/tmp/text_data/"];

        let mut read_opts = TextOptions::new();

        read_opts.whole_text = Some(true);
        read_opts.line_sep = Some("\n".to_string());
        read_opts.path_glob_filter = Some("*.txt".to_string());
        read_opts.recursive_file_lookup = Some(true);

        let df = spark.read().text(path, read_opts)?;

        let rows = df.clone().collect().await?;

        assert_eq!(rows.num_rows(), 3);
        Ok(())
    }
}
