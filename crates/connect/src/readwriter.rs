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

/// Common file options that are shared across multiple file formats
/// (e.g., CSV, JSON, ORC, Parquet, Text).
///
/// These options allow for file filtering and error handling during
/// file discovery and processing.
///
/// # Fields
///
/// - `path_glob_filter`: Optional glob pattern to filter files by path.
///   This can be used to select specific files within a directory.
///
/// - `recursive_file_lookup`: Whether to recursively search for files in
///   subdirectories. If set to `true`, the reader will search all subdirectories
///   for matching files.
///
/// - `modified_before`: An optional string specifying a cutoff date for filtering
///   files. Files modified after this date will not be included.
///
/// - `modified_after`: An optional string specifying a start date for filtering
///   files. Only files modified after this date will be included.
///
/// - `ignore_corrupt_files`: If set to `true`, the reader will skip corrupt files
///   instead of throwing an error. This is useful in scenarios where some files
///   may be malformed.
///
/// - `ignore_missing_files`: If set to `true`, missing files (e.g., those filtered out by
///   the glob pattern) will be ignored instead of causing an error. This is useful when
///   running in environments where files may be missing intermittently.
#[derive(Debug, Clone)]
pub struct CommonFileOptions {
    pub path_glob_filter: Option<String>,
    pub recursive_file_lookup: Option<bool>,
    pub modified_before: Option<String>,
    pub modified_after: Option<String>,
    pub ignore_corrupt_files: Option<bool>,
    pub ignore_missing_files: Option<bool>,
}

impl Default for CommonFileOptions {
    fn default() -> Self {
        Self {
            path_glob_filter: None,
            recursive_file_lookup: Some(false),
            modified_before: None,
            modified_after: None,
            ignore_corrupt_files: None,
            ignore_missing_files: None,
        }
    }
}

impl ConfigOpts for CommonFileOptions {
    fn to_options(&self) -> HashMap<String, String> {
        let mut options = HashMap::new();

        if let Some(path_glob_filter) = &self.path_glob_filter {
            options.insert("pathGlobFilter".to_string(), path_glob_filter.clone());
        }

        if let Some(recursive_file_lookup) = self.recursive_file_lookup {
            options.insert(
                "recursiveFileLookup".to_string(),
                recursive_file_lookup.to_string(),
            );
        }

        if let Some(modified_before) = &self.modified_before {
            options.insert("modifiedBefore".to_string(), modified_before.clone());
        }

        if let Some(modified_after) = &self.modified_after {
            options.insert("modifiedAfter".to_string(), modified_after.clone());
        }

        if let Some(ignore_corrupt_files) = self.ignore_corrupt_files {
            options.insert(
                "ignoreCorruptFiles".to_string(),
                ignore_corrupt_files.to_string(),
            );
        }

        if let Some(ignore_missing_files) = self.ignore_missing_files {
            options.insert(
                "ignoreMissingFiles".to_string(),
                ignore_missing_files.to_string(),
            );
        }

        options
    }
}

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
#[derive(Debug, Clone, Default)]
pub struct CsvOptions {
    pub schema: Option<String>,
    pub sep: Option<String>,
    pub delimiter: Option<String>,
    pub encoding: Option<String>,
    pub quote: Option<String>,
    pub quote_all: Option<bool>,
    pub escape: Option<String>,
    pub escape_quotes: Option<bool>,
    pub comment: Option<String>,
    pub header: Option<bool>,
    pub infer_schema: Option<bool>,
    pub ignore_leading_white_space: Option<bool>,
    pub ignore_trailing_white_space: Option<bool>,
    pub null_value: Option<String>,
    pub nan_value: Option<String>,
    pub positive_inf: Option<String>,
    pub negative_inf: Option<String>,
    pub date_format: Option<String>,
    pub timestamp_format: Option<String>,
    pub timestamp_ntz_format: Option<String>,
    pub enable_datetime_parsing_fallback: Option<bool>,
    pub max_columns: Option<i32>,
    pub max_chars_per_column: Option<i32>,
    pub max_malformed_log_per_partition: Option<i32>,
    pub mode: Option<String>,
    pub column_name_of_corrupt_record: Option<String>,
    pub multi_line: Option<bool>,
    pub char_to_escape_quote_escaping: Option<String>,
    pub sampling_ratio: Option<f64>,
    pub prefer_date: Option<bool>,
    pub enforce_schema: Option<bool>,
    pub empty_value: Option<String>,
    pub locale: Option<String>,
    pub line_sep: Option<String>,
    pub unescaped_quote_handling: Option<String>,
    pub common: CommonFileOptions,
}

impl CsvOptions {
    pub fn schema(mut self, value: &str) -> Self {
        self.schema = Some(value.to_string());
        self
    }

    pub fn sep(mut self, value: &str) -> Self {
        self.sep = Some(value.to_string());
        self
    }

    pub fn delimiter(mut self, value: &str) -> Self {
        self.delimiter = Some(value.to_string());
        self
    }

    pub fn encoding(mut self, value: &str) -> Self {
        self.encoding = Some(value.to_string());
        self
    }

    pub fn quote(mut self, value: &str) -> Self {
        self.quote = Some(value.to_string());
        self
    }

    pub fn quote_all(mut self, value: bool) -> Self {
        self.quote_all = Some(value);
        self
    }

    pub fn escape(mut self, value: &str) -> Self {
        self.escape = Some(value.to_string());
        self
    }

    pub fn comment(mut self, value: &str) -> Self {
        self.comment = Some(value.to_string());
        self
    }

    pub fn header(mut self, value: bool) -> Self {
        self.header = Some(value);
        self
    }

    pub fn infer_schema(mut self, value: bool) -> Self {
        self.infer_schema = Some(value);
        self
    }

    pub fn ignore_leading_white_space(mut self, value: bool) -> Self {
        self.ignore_leading_white_space = Some(value);
        self
    }

    pub fn ignore_trailing_white_space(mut self, value: bool) -> Self {
        self.ignore_trailing_white_space = Some(value);
        self
    }

    pub fn null_value(mut self, value: &str) -> Self {
        self.null_value = Some(value.to_string());
        self
    }

    pub fn nan_value(mut self, value: &str) -> Self {
        self.nan_value = Some(value.to_string());
        self
    }

    pub fn positive_inf(mut self, value: &str) -> Self {
        self.positive_inf = Some(value.to_string());
        self
    }

    pub fn negative_inf(mut self, value: &str) -> Self {
        self.negative_inf = Some(value.to_string());
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

    pub fn timestamp_ntz_format(mut self, value: &str) -> Self {
        self.timestamp_ntz_format = Some(value.to_string());
        self
    }

    pub fn enable_datetime_parsing_fallback(mut self, value: bool) -> Self {
        self.enable_datetime_parsing_fallback = Some(value);
        self
    }

    pub fn max_columns(mut self, value: i32) -> Self {
        self.max_columns = Some(value);
        self
    }

    pub fn max_chars_per_column(mut self, value: i32) -> Self {
        self.max_chars_per_column = Some(value);
        self
    }

    pub fn max_malformed_log_per_partition(mut self, value: i32) -> Self {
        self.max_malformed_log_per_partition = Some(value);
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

    pub fn multi_line(mut self, value: bool) -> Self {
        self.multi_line = Some(value);
        self
    }

    pub fn char_to_escape_quote_escaping(mut self, value: &str) -> Self {
        self.char_to_escape_quote_escaping = Some(value.to_string());
        self
    }

    pub fn sampling_ratio(mut self, value: f64) -> Self {
        self.sampling_ratio = Some(value);
        self
    }

    pub fn prefer_date(mut self, value: bool) -> Self {
        self.prefer_date = Some(value);
        self
    }

    pub fn enforce_schema(mut self, value: bool) -> Self {
        self.enforce_schema = Some(value);
        self
    }

    pub fn empty_value(mut self, value: &str) -> Self {
        self.empty_value = Some(value.to_string());
        self
    }

    pub fn locale(mut self, value: &str) -> Self {
        self.locale = Some(value.to_string());
        self
    }

    pub fn line_sep(mut self, value: &str) -> Self {
        self.line_sep = Some(value.to_string());
        self
    }

    pub fn unescaped_quote_handling(mut self, value: &str) -> Self {
        self.unescaped_quote_handling = Some(value.to_string());
        self
    }

    pub fn escape_quotes(mut self, value: bool) -> Self {
        self.escape_quotes = Some(value);
        self
    }
}

impl ConfigOpts for CsvOptions {
    fn to_options(&self) -> HashMap<String, String> {
        let mut options: HashMap<String, String> = HashMap::new();

        if let Some(schema) = &self.schema {
            options.insert("schema".to_string(), schema.to_string());
        }

        if let Some(sep) = &self.sep {
            options.insert("sep".to_string(), sep.to_string());
        }

        if let Some(delimiter) = &self.delimiter {
            options.insert("delimiter".to_string(), delimiter.to_string());
        }

        if let Some(encoding) = &self.encoding {
            options.insert("encoding".to_string(), encoding.to_string());
        }

        if let Some(quote) = &self.quote {
            options.insert("quote".to_string(), quote.to_string());
        }

        if let Some(quote_all) = self.quote_all {
            options.insert("quoteAll".to_string(), quote_all.to_string());
        }

        if let Some(escape) = &self.escape {
            options.insert("escape".to_string(), escape.to_string());
        }

        if let Some(comment) = &self.comment {
            options.insert("comment".to_string(), comment.to_string());
        }

        if let Some(header) = self.header {
            options.insert("header".to_string(), header.to_string());
        }

        if let Some(infer_schema) = self.infer_schema {
            options.insert("inferSchema".to_string(), infer_schema.to_string());
        }

        if let Some(ignore_leading_white_space) = self.ignore_leading_white_space {
            options.insert(
                "ignoreLeadingWhiteSpace".to_string(),
                ignore_leading_white_space.to_string(),
            );
        }

        if let Some(ignore_trailing_white_space) = self.ignore_trailing_white_space {
            options.insert(
                "ignoreTrailingWhiteSpace".to_string(),
                ignore_trailing_white_space.to_string(),
            );
        }

        if let Some(null_value) = &self.null_value {
            options.insert("nullValue".to_string(), null_value.to_string());
        }

        if let Some(nan_value) = &self.nan_value {
            options.insert("nanValue".to_string(), nan_value.to_string());
        }

        if let Some(positive_inf) = &self.positive_inf {
            options.insert("positiveInf".to_string(), positive_inf.to_string());
        }

        if let Some(negative_inf) = &self.negative_inf {
            options.insert("negativeInf".to_string(), negative_inf.to_string());
        }

        if let Some(date_format) = &self.date_format {
            options.insert("dateFormat".to_string(), date_format.to_string());
        }

        if let Some(timestamp_format) = &self.timestamp_format {
            options.insert("timestampFormat".to_string(), timestamp_format.to_string());
        }

        if let Some(timestamp_ntz_format) = &self.timestamp_ntz_format {
            options.insert(
                "timestampNTZFormat".to_string(),
                timestamp_ntz_format.to_string(),
            );
        }

        if let Some(enable_datetime_parsing_fallback) = self.enable_datetime_parsing_fallback {
            options.insert(
                "enableDatetimeParsingFallback".to_string(),
                enable_datetime_parsing_fallback.to_string(),
            );
        }

        if let Some(max_columns) = self.max_columns {
            options.insert("maxColumns".to_string(), max_columns.to_string());
        }

        if let Some(max_chars_per_column) = self.max_chars_per_column {
            options.insert(
                "maxCharsPerColumn".to_string(),
                max_chars_per_column.to_string(),
            );
        }

        if let Some(max_malformed_log_per_partition) = self.max_malformed_log_per_partition {
            options.insert(
                "maxMalformedLogPerPartition".to_string(),
                max_malformed_log_per_partition.to_string(),
            );
        }

        if let Some(mode) = &self.mode {
            options.insert("mode".to_string(), mode.to_string());
        }

        if let Some(column_name_of_corrupt_record) = &self.column_name_of_corrupt_record {
            options.insert(
                "columnNameOfCorruptRecord".to_string(),
                column_name_of_corrupt_record.to_string(),
            );
        }

        if let Some(multi_line) = self.multi_line {
            options.insert("multiLine".to_string(), multi_line.to_string());
        }

        if let Some(char_to_escape_quote_escaping) = &self.char_to_escape_quote_escaping {
            options.insert(
                "charToEscapeQuoteEscaping".to_string(),
                char_to_escape_quote_escaping.to_string(),
            );
        }

        if let Some(sampling_ratio) = self.sampling_ratio {
            options.insert("samplingRatio".to_string(), sampling_ratio.to_string());
        }

        if let Some(prefer_date) = self.prefer_date {
            options.insert("preferDate".to_string(), prefer_date.to_string());
        }

        if let Some(enforce_schema) = self.enforce_schema {
            options.insert("enforceSchema".to_string(), enforce_schema.to_string());
        }

        if let Some(empty_value) = &self.empty_value {
            options.insert("emptyValue".to_string(), empty_value.to_string());
        }

        if let Some(locale) = &self.locale {
            options.insert("locale".to_string(), locale.to_string());
        }

        if let Some(line_sep) = &self.line_sep {
            options.insert("lineSep".to_string(), line_sep.to_string());
        }

        if let Some(unescaped_quote_handling) = &self.unescaped_quote_handling {
            options.insert(
                "unescapedQuoteHandling".to_string(),
                unescaped_quote_handling.to_string(),
            );
        }

        if let Some(escape_quotes) = self.escape_quotes {
            options.insert("escapeQuotes".to_string(), escape_quotes.to_string());
        }

        options.extend(self.common.to_options());

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
#[derive(Debug, Clone)]
pub struct JsonOptions {
    pub schema: Option<String>,
    pub compression: Option<String>,
    pub primitives_as_string: Option<bool>,
    pub prefers_decimal: Option<bool>,
    pub allow_comments: Option<bool>,
    pub allow_unquoted_field_names: Option<bool>,
    pub allow_single_quotes: Option<bool>,
    pub allow_numeric_leading_zeros: Option<bool>,
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
    pub allow_non_numeric_numbers: Option<bool>,
    pub time_zone: Option<String>,
    pub timestamp_ntz_format: Option<String>,
    pub enable_datetime_parsing_fallback: Option<bool>,
    pub ignore_null_fields: Option<bool>,
    pub common: CommonFileOptions,
}

impl Default for JsonOptions {
    fn default() -> Self {
        Self {
            schema: None,
            compression: Some("gzip".to_string()),
            primitives_as_string: None,
            prefers_decimal: None,
            allow_comments: None,
            allow_unquoted_field_names: None,
            allow_single_quotes: None,
            allow_numeric_leading_zeros: None,
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
            allow_non_numeric_numbers: None,
            time_zone: None,
            timestamp_ntz_format: None,
            enable_datetime_parsing_fallback: None,
            ignore_null_fields: None,
            common: CommonFileOptions::default(),
        }
    }
}

impl JsonOptions {
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

    pub fn allow_numeric_leading_zeros(mut self, value: bool) -> Self {
        self.allow_numeric_leading_zeros = Some(value);
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

    pub fn allow_non_numeric_numbers(mut self, value: bool) -> Self {
        self.allow_non_numeric_numbers = Some(value);
        self
    }

    pub fn time_zone(mut self, value: &str) -> Self {
        self.time_zone = Some(value.to_string());
        self
    }

    pub fn timestamp_ntz_format(mut self, value: &str) -> Self {
        self.timestamp_ntz_format = Some(value.to_string());
        self
    }

    pub fn enable_datetime_parsing_fallback(mut self, value: bool) -> Self {
        self.enable_datetime_parsing_fallback = Some(value);
        self
    }

    pub fn compression(mut self, value: &str) -> Self {
        self.compression = Some(value.to_string());
        self
    }

    pub fn ignore_null_fields(mut self, value: bool) -> Self {
        self.ignore_null_fields = Some(value);
        self
    }
}

impl ConfigOpts for JsonOptions {
    fn to_options(&self) -> HashMap<String, String> {
        let mut options: HashMap<String, String> = HashMap::new();

        if let Some(schema) = &self.schema {
            options.insert("schema".to_string(), schema.to_string());
        }

        if let Some(compression) = &self.compression {
            options.insert("compression".to_string(), compression.clone());
        }

        if let Some(primitives_as_string) = self.primitives_as_string {
            options.insert(
                "primitivesAsString".to_string(),
                primitives_as_string.to_string(),
            );
        }

        if let Some(prefers_decimal) = self.prefers_decimal {
            options.insert("prefersDecimal".to_string(), prefers_decimal.to_string());
        }

        if let Some(allow_comments) = self.allow_comments {
            options.insert("allowComments".to_string(), allow_comments.to_string());
        }

        if let Some(allow_unquoted_field_names) = self.allow_unquoted_field_names {
            options.insert(
                "allowUnquotedFieldNames".to_string(),
                allow_unquoted_field_names.to_string(),
            );
        }

        if let Some(allow_single_quotes) = self.allow_single_quotes {
            options.insert(
                "allowSingleQuotes".to_string(),
                allow_single_quotes.to_string(),
            );
        }

        if let Some(allow_numeric_leading_zeros) = self.allow_numeric_leading_zeros {
            options.insert(
                "allowNumericLeadingZero".to_string(),
                allow_numeric_leading_zeros.to_string(),
            );
        }

        if let Some(allow_backslash_escaping_any_character) =
            self.allow_backslash_escaping_any_character
        {
            options.insert(
                "allowBackslashEscapingAnyCharacter".to_string(),
                allow_backslash_escaping_any_character.to_string(),
            );
        }

        if let Some(mode) = &self.mode {
            options.insert("mode".to_string(), mode.clone());
        }

        if let Some(column_name_of_corrupt_record) = &self.column_name_of_corrupt_record {
            options.insert(
                "columnNameOfCorruptRecord".to_string(),
                column_name_of_corrupt_record.clone(),
            );
        }

        if let Some(date_format) = &self.date_format {
            options.insert("dateFormat".to_string(), date_format.clone());
        }

        if let Some(timestamp_format) = &self.timestamp_format {
            options.insert("timestampFormat".to_string(), timestamp_format.clone());
        }

        if let Some(multi_line) = self.multi_line {
            options.insert("multiLine".to_string(), multi_line.to_string());
        }

        if let Some(allow_unquoted_control_chars) = self.allow_unquoted_control_chars {
            options.insert(
                "allowUnquotedControlChars".to_string(),
                allow_unquoted_control_chars.to_string(),
            );
        }

        if let Some(line_sep) = &self.line_sep {
            options.insert("lineSep".to_string(), line_sep.clone());
        }

        if let Some(sampling_ratio) = self.sampling_ratio {
            options.insert("samplingRatio".to_string(), sampling_ratio.to_string());
        }

        if let Some(drop_field_if_all_null) = self.drop_field_if_all_null {
            options.insert(
                "dropFieldIfAllNull".to_string(),
                drop_field_if_all_null.to_string(),
            );
        }

        if let Some(encoding) = &self.encoding {
            options.insert("encoding".to_string(), encoding.clone());
        }

        if let Some(locale) = &self.locale {
            options.insert("locale".to_string(), locale.clone());
        }

        if let Some(allow_non_numeric_numbers) = self.allow_non_numeric_numbers {
            options.insert(
                "allowNonNumericNumbers".to_string(),
                allow_non_numeric_numbers.to_string(),
            );
        }

        if let Some(time_zone) = &self.time_zone {
            options.insert("timeZone".to_string(), time_zone.clone());
        }

        if let Some(timestamp_ntz_format) = &self.timestamp_ntz_format {
            options.insert(
                "timestampNTZFormat".to_string(),
                timestamp_ntz_format.clone(),
            );
        }

        if let Some(enable_datetime_parsing_fallback) = self.enable_datetime_parsing_fallback {
            options.insert(
                "enableDatetimeParsingFallback".to_string(),
                enable_datetime_parsing_fallback.to_string(),
            );
        }

        if let Some(ignore_null_fields) = self.ignore_null_fields {
            options.insert(
                "ignoreNullFields".to_string(),
                ignore_null_fields.to_string(),
            );
        }

        options.extend(self.common.to_options());

        options
    }
}

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
#[derive(Debug, Clone)]
pub struct OrcOptions {
    pub compression: Option<String>,
    pub merge_schema: Option<bool>,
    pub common: CommonFileOptions,
}

impl Default for OrcOptions {
    fn default() -> Self {
        OrcOptions {
            compression: Some("snappy".to_string()),
            merge_schema: None,
            common: CommonFileOptions::default(),
        }
    }
}

impl OrcOptions {
    pub fn compression(mut self, value: &str) -> Self {
        self.compression = Some(value.to_string());
        self
    }

    pub fn merge_schema(mut self, value: bool) -> Self {
        self.merge_schema = Some(value);
        self
    }
}

impl ConfigOpts for OrcOptions {
    fn to_options(&self) -> HashMap<String, String> {
        let mut options: HashMap<String, String> = HashMap::new();

        if let Some(compression) = &self.compression {
            options.insert("compression".to_string(), compression.to_string());
        }

        if let Some(merge_schema) = self.merge_schema {
            options.insert("mergeSchema".to_string(), merge_schema.to_string());
        }

        options.extend(self.common.to_options());

        options
    }
}

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
#[derive(Debug, Clone)]
pub struct ParquetOptions {
    pub compression: Option<String>,
    pub merge_schema: Option<bool>,
    pub datetime_rebase_mode: Option<String>,
    pub int96_rebase_mode: Option<String>,
    pub common: CommonFileOptions,
}

impl Default for ParquetOptions {
    fn default() -> Self {
        Self {
            compression: Some("snappy".to_string()),
            merge_schema: None,
            datetime_rebase_mode: None,
            int96_rebase_mode: None,
            common: CommonFileOptions::default(),
        }
    }
}

impl ParquetOptions {
    pub fn compression(mut self, value: &str) -> Self {
        self.compression = Some(value.to_string());
        self
    }

    pub fn merge_schema(mut self, value: bool) -> Self {
        self.merge_schema = Some(value);
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

        if let Some(compression) = &self.compression {
            options.insert("compression".to_string(), compression.to_string());
        }

        if let Some(merge_schema) = self.merge_schema {
            options.insert("mergeSchema".to_string(), merge_schema.to_string());
        }

        if let Some(datetime_rebase_mode) = &self.datetime_rebase_mode {
            options.insert(
                "datetimeRebaseMode".to_string(),
                datetime_rebase_mode.to_string(),
            );
        }

        if let Some(int96_rebase_mode) = &self.int96_rebase_mode {
            options.insert("int96RebaseMode".to_string(), int96_rebase_mode.to_string());
        }

        options.extend(self.common.to_options());

        options
    }
}

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
#[derive(Debug, Clone, Default)]
pub struct TextOptions {
    pub whole_text: Option<bool>,
    pub line_sep: Option<String>,
    pub common: CommonFileOptions,
}

impl TextOptions {
    pub fn whole_text(mut self, value: bool) -> Self {
        self.whole_text = Some(value);
        self
    }

    pub fn line_sep(mut self, value: &str) -> Self {
        self.line_sep = Some(value.to_string());
        self
    }
}

impl ConfigOpts for TextOptions {
    fn to_options(&self) -> HashMap<String, String> {
        let mut options: HashMap<String, String> = HashMap::new();

        if let Some(whole_text) = self.whole_text {
            options.insert("wholeText".to_string(), whole_text.to_string());
        }

        if let Some(line_sep) = &self.line_sep {
            options.insert("lineSep".to_string(), line_sep.to_string());
        }

        options.extend(self.common.to_options());

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

        let mut opts = CsvOptions::default();

        opts.header = Some(true);
        opts.null_value = Some("NULL".to_string());
        opts.sep = Some(";".to_string());
        opts.infer_schema = Some(true);
        opts.encoding = Some("UTF-8".to_string());
        opts.quote = Some("\"".to_string());
        opts.escape = Some("\\".to_string());
        opts.multi_line = Some(false);
        opts.date_format = Some("yyyy-MM-dd".to_string());
        opts.timestamp_format = Some("yyyy-MM-dd'T'HH:mm:ss".to_string());
        opts.ignore_leading_white_space = Some(true);
        opts.ignore_trailing_white_space = Some(true);
        opts.mode = Some("DROPMALFORMED".to_string());

        let df = spark.read().csv(path, opts)?;

        let rows = df.clone().collect().await?;

        assert_eq!(rows.num_rows(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_read_json_with_options() -> Result<(), SparkError> {
        let spark = setup().await;

        let path = ["/opt/spark/work-dir/datasets/employees.json"];

        let mut opts = JsonOptions::default();

        opts.schema = Some("name STRING, salary INT".to_string());
        opts.multi_line = Some(false);
        opts.allow_comments = Some(false);
        opts.allow_unquoted_field_names = Some(false);
        opts.primitives_as_string = Some(false);
        opts.compression = Some("gzip".to_string());
        opts.ignore_null_fields = Some(true);

        let df = spark.read().json(path, opts)?;

        let rows = df.clone().collect().await?;

        assert_eq!(rows.num_rows(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_read_orc_with_options() -> Result<(), SparkError> {
        let spark = setup().await;

        let path = ["/opt/spark/work-dir/datasets/users.orc"];

        let mut opts = OrcOptions::default();

        opts.compression = Some("snappy".to_string());
        opts.merge_schema = Some(true);
        opts.common.path_glob_filter = Some("*.orc".to_string());
        opts.common.recursive_file_lookup = Some(true);

        let df = spark.read().orc(path, opts)?;

        let rows = df.clone().collect().await?;

        assert_eq!(rows.num_rows(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_read_parquet_with_options() -> Result<(), SparkError> {
        let spark = setup().await;

        let path = ["/opt/spark/work-dir/datasets/users.parquet"];

        let mut opts = ParquetOptions::default();

        opts.compression = Some("snappy".to_string());
        opts.common.path_glob_filter = Some("*.parquet".to_string());
        opts.common.recursive_file_lookup = Some(true);

        let df = spark.read().parquet(path, opts)?;

        let rows = df.clone().collect().await?;

        assert_eq!(rows.num_rows(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_dataframe_read_text_with_options() -> Result<(), SparkError> {
        let spark = setup().await;

        let path = ["/opt/spark/work-dir/datasets/people.txt"];

        let mut opts = TextOptions::default();

        // If true, read each file from input path(s) as a single row.
        opts.whole_text = Some(false);
        opts.line_sep = Some("\n".to_string());
        opts.common.path_glob_filter = Some("*.txt".to_string());
        opts.common.recursive_file_lookup = Some(true);

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

        let mut write_opts = CsvOptions::default();

        write_opts.header = Some(true);
        write_opts.null_value = Some("NULL".to_string());

        let _ = df
            .write()
            .mode(SaveMode::Overwrite)
            .csv(path, write_opts)
            .await;

        let path = ["/tmp/csv_with_options_rande_id/"];

        let mut read_opts = CsvOptions::default();

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

        let path = "/tmp/json_with_options_rande_id/";

        let mut write_opts = JsonOptions::default();

        write_opts.multi_line = Some(true);
        write_opts.allow_comments = Some(false);
        write_opts.allow_unquoted_field_names = Some(false);
        write_opts.primitives_as_string = Some(false);

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

        let mut read_opts = OrcOptions::default();

        read_opts.merge_schema = Some(true);
        read_opts.common.path_glob_filter = Some("*.orc".to_string());
        read_opts.common.recursive_file_lookup = Some(true);

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

        let mut write_opts = ParquetOptions::default();

        // Configure datetime rebase mode (options could be "EXCEPTION", "LEGACY", or "CORRECTED").
        write_opts.datetime_rebase_mode = Some("CORRECTED".to_string());

        // Configure int96 rebase mode (options could be "EXCEPTION", "LEGACY", or "CORRECTED").
        write_opts.int96_rebase_mode = Some("LEGACY".to_string());

        let _ = df
            .write()
            .mode(SaveMode::Overwrite)
            .parquet(path, write_opts)
            .await;

        let path = ["/tmp/parquet_with_options_rande_id/"];

        let mut read_opts = ParquetOptions::default();

        read_opts.merge_schema = Some(false);
        read_opts.common.path_glob_filter = Some("*.parquet".to_string());
        read_opts.common.recursive_file_lookup = Some(true);

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

        let mut write_opts = TextOptions::default();

        write_opts.whole_text = Some(true);

        // Note that, in order to use write.text(), the dataframe
        // must have only one column else it will throw error.
        // Hence you need to covert all columns into single column.
        let _ = df
            .write()
            .mode(SaveMode::Overwrite)
            .text(path, write_opts)
            .await;

        let path = ["/tmp/text_with_options_rande_id/"];

        let mut read_opts = TextOptions::default();

        read_opts.whole_text = Some(true);
        read_opts.line_sep = Some("\n".to_string());
        read_opts.common.path_glob_filter = Some("*.txt".to_string());
        read_opts.common.recursive_file_lookup = Some(true);

        let df = spark.read().text(path, read_opts)?;

        let rows = df.clone().collect().await?;

        assert_eq!(rows.num_rows(), 3);
        Ok(())
    }
}
