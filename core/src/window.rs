//! Utility structs for defining a window over a DataFrame

use crate::expressions::{ToExpr, ToLiteralExpr, ToVecExpr};
use crate::plan::sort_order;

use crate::spark;
use crate::spark::expression::window;

/// A window specification that defines the partitioning, ordering, and frame boundaries.
///
/// **Recommended to create a WindowSpec using [Window] and not directly**
#[derive(Debug, Default, Clone)]
pub struct WindowSpec {
    pub partition_spec: Vec<spark::Expression>,
    pub order_spec: Vec<spark::expression::SortOrder>,
    pub frame_spec: Option<Box<window::WindowFrame>>,
}

impl WindowSpec {
    pub fn new(
        partition_spec: Vec<spark::Expression>,
        order_spec: Vec<spark::expression::SortOrder>,
        frame_spec: Option<Box<window::WindowFrame>>,
    ) -> WindowSpec {
        WindowSpec {
            partition_spec,
            order_spec,
            frame_spec,
        }
    }

    pub fn partition_by<I: ToVecExpr>(self, cols: I) -> WindowSpec {
        WindowSpec::new(cols.to_vec_expr(), self.order_spec, self.frame_spec)
    }

    pub fn order_by<I, T>(self, cols: I) -> WindowSpec
    where
        T: ToExpr,
        I: IntoIterator<Item = T>,
    {
        let order_spec = sort_order(cols);

        WindowSpec::new(self.partition_spec, order_spec, self.frame_spec)
    }

    pub fn rows_between(self, start: i64, end: i64) -> WindowSpec {
        let frame_spec = WindowSpec::window_frame(true, start, end);

        WindowSpec::new(self.partition_spec, self.order_spec, frame_spec)
    }

    pub fn range_between(self, start: i64, end: i64) -> WindowSpec {
        let frame_spec = WindowSpec::window_frame(false, start, end);

        WindowSpec::new(self.partition_spec, self.order_spec, frame_spec)
    }

    fn frame_boundary(value: i64) -> Option<Box<window::window_frame::FrameBoundary>> {
        match value {
            0 => {
                let boundary = Some(window::window_frame::frame_boundary::Boundary::CurrentRow(
                    true,
                ));

                Some(Box::new(window::window_frame::FrameBoundary { boundary }))
            }
            i64::MIN => {
                let boundary = Some(window::window_frame::frame_boundary::Boundary::Unbounded(
                    true,
                ));

                Some(Box::new(window::window_frame::FrameBoundary { boundary }))
            }
            _ => {
                // !TODO - I don't like casting this to i32
                // however, the window boundary is expecting an INT and not a BIGINT
                // i64 is a BIGINT (i.e. Long)
                let expr = (value as i32).to_literal_expr();

                let boundary = Some(window::window_frame::frame_boundary::Boundary::Value(
                    Box::new(expr),
                ));

                Some(Box::new(window::window_frame::FrameBoundary { boundary }))
            }
        }
    }

    fn window_frame(row_frame: bool, start: i64, end: i64) -> Option<Box<window::WindowFrame>> {
        let frame_type = match row_frame {
            true => 1,
            false => 2,
        };

        let lower = WindowSpec::frame_boundary(start);
        let upper = WindowSpec::frame_boundary(end);

        Some(Box::new(window::WindowFrame {
            frame_type,
            lower,
            upper,
        }))
    }
}

/// Primary utility struct for defining window in DataFrames
#[derive(Debug, Default, Clone)]
pub struct Window {
    spec: WindowSpec,
}

impl Window {
    /// Creates a new empty [WindowSpec]
    pub fn new() -> Self {
        Window {
            spec: WindowSpec::default(),
        }
    }

    /// Returns 0
    pub fn current_row() -> i64 {
        0
    }

    /// Returns [i64::MAX]
    pub fn unbounded_following() -> i64 {
        i64::MAX
    }

    /// Returns [i64::MIN]
    pub fn unbounded_preceding() -> i64 {
        i64::MIN
    }

    /// Creates a [WindowSpec] with the partitioning defined
    pub fn partition_by<I: ToVecExpr>(mut self, cols: I) -> WindowSpec {
        self.spec = self.spec.partition_by(cols);

        self.spec
    }

    /// Creates a [WindowSpec] with the ordering defined
    pub fn order_by<I, T>(mut self, cols: I) -> WindowSpec
    where
        T: ToExpr,
        I: IntoIterator<Item = T>,
    {
        self.spec = self.spec.order_by(cols);

        self.spec
    }

    /// Creates a [WindowSpec] with the frame boundaries defined, from start (inclusive) to end (inclusive).
    ///
    /// Both start and end are relative from the current row. For example, “0” means “current row”,
    /// while “-1” means one off before the current row, and “5” means the five off after the current row.
    ///
    /// Recommended to use [Window::unbounded_preceding], [Window::unbounded_following], and [Window::current_row]
    /// to specify special boundary values, rather than using integral values directly.
    ///
    /// # Example
    ///
    /// ```
    /// let window = Window::new()
    ///     .partition_by(col("name"))
    ///     .order_by([col("age")])
    ///     .range_between(Window::unbounded_preceding(), Window::current_row());
    ///
    /// let df = df.with_column("rank", rank().over(window.clone()))
    ///     .with_column("min", min("age").over(window));
    /// ```
    pub fn range_between(mut self, start: i64, end: i64) -> WindowSpec {
        self.spec = self.spec.range_between(start, end);

        self.spec
    }

    /// Creates a [WindowSpec] with the frame boundaries defined, from start (inclusive) to end (inclusive).
    ///
    /// Both start and end are relative from the current row. For example, “0” means “current row”,
    /// while “-1” means one off before the current row, and “5” means the five off after the current row.
    ///
    /// Recommended to use [Window::unbounded_preceding], [Window::unbounded_following], and [Window::current_row]
    /// to specify special boundary values, rather than using integral values directly.
    ///
    /// # Example
    ///
    /// ```
    /// let window = Window::new()
    ///     .partition_by(col("name"))
    ///     .order_by([col("age")])
    ///     .rows_between(Window::unbounded_preceding(), Window::current_row());
    ///
    /// let df = df.with_column("rank", rank().over(window.clone()))
    ///     .with_column("min", min("age").over(window));
    /// ```

    pub fn rows_between(mut self, start: i64, end: i64) -> WindowSpec {
        self.spec = self.spec.rows_between(start, end);

        self.spec
    }
}

#[cfg(test)]
mod tests {

    use arrow::{
        array::{ArrayRef, Int32Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use std::sync::Arc;

    use super::*;

    use crate::errors::SparkError;
    use crate::functions::*;
    use crate::SparkSession;
    use crate::SparkSessionBuilder;

    async fn setup() -> SparkSession {
        println!("SparkSession Setup");

        let connection = "sc://127.0.0.1:15002/;user_id=rust_window";

        SparkSessionBuilder::remote(connection)
            .build()
            .await
            .unwrap()
    }

    fn mock_data() -> RecordBatch {
        let id: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 2, 1, 2, 3]));
        let category: ArrayRef = Arc::new(StringArray::from(vec!["a", "a", "a", "b", "b", "b"]));

        RecordBatch::try_from_iter(vec![("id", id), ("category", category)]).unwrap()
    }

    #[tokio::test]
    async fn test_window_over() -> Result<(), SparkError> {
        let spark = setup().await;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![2, 5]));

        let data = RecordBatch::try_from_iter(vec![("name", name), ("age", age)])?;

        let df = spark.create_dataframe(&data)?;

        let window = Window::new()
            .partition_by(col("name"))
            .order_by([col("age")])
            .rows_between(Window::unbounded_preceding(), Window::current_row());

        let res = df
            .with_column("rank", rank().over(window.clone()))
            .with_column("min", min("age").over(window))
            .collect()
            .await?;

        let name: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![2, 5]));
        let rank: ArrayRef = Arc::new(Int32Array::from(vec![1, 1]));
        let min = age.clone();

        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
            Field::new("rank", DataType::Int32, false),
            Field::new("min", DataType::Int64, true),
        ]);

        let expected = RecordBatch::try_new(Arc::new(schema), vec![name, age, rank, min])?;

        assert_eq!(expected, res);

        Ok(())
    }

    #[tokio::test]
    async fn test_window_orderby() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        let df = spark.create_dataframe(&data)?;

        let window = Window::new()
            .partition_by(col("id"))
            .order_by([col("category")]);

        let res = df
            .with_column("row_number", row_number().over(window))
            .collect()
            .await?;

        let id: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2, 3]));
        let category: ArrayRef = Arc::new(StringArray::from(vec!["a", "a", "b", "a", "b", "b"]));
        let row_number: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 1, 2, 1]));

        let expected = RecordBatch::try_from_iter(vec![
            ("id", id),
            ("category", category),
            ("row_number", row_number),
        ])?;

        assert_eq!(expected, res);

        Ok(())
    }

    #[tokio::test]
    async fn test_window_partitionby() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        let df = spark.create_dataframe(&data)?;

        let window = Window::new()
            .partition_by(col("category"))
            .order_by([col("id")]);

        let res = df
            .with_column("row_number", row_number().over(window))
            .collect()
            .await?;

        let id: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 2, 1, 2, 3]));
        let category: ArrayRef = Arc::new(StringArray::from(vec!["a", "a", "a", "b", "b", "b"]));
        let row_number: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 1, 2, 3]));

        let expected = RecordBatch::try_from_iter(vec![
            ("id", id),
            ("category", category),
            ("row_number", row_number),
        ])?;

        assert_eq!(expected, res);

        Ok(())
    }

    #[tokio::test]
    async fn test_window_rangebetween() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        let df = spark.create_dataframe(&data)?;

        let window = Window::new()
            .partition_by(col("category"))
            .order_by([col("id")])
            .range_between(Window::current_row(), 1);

        let res = df
            .with_column("sum", sum("id").over(window))
            .sort([col("id"), col("category")])
            .collect()
            .await?;

        let id: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2, 3]));
        let category: ArrayRef = Arc::new(StringArray::from(vec!["a", "a", "b", "a", "b", "b"]));
        let sum: ArrayRef = Arc::new(Int64Array::from(vec![4, 4, 3, 2, 5, 3]));

        let expected = RecordBatch::try_from_iter_with_nullable(vec![
            ("id", id, false),
            ("category", category, false),
            ("sum", sum, true),
        ])?;

        assert_eq!(expected, res);

        Ok(())
    }

    #[tokio::test]
    async fn test_window_rowsbetween() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        let df = spark.create_dataframe(&data)?;

        let window = Window::new()
            .partition_by(col("category"))
            .order_by([col("id")])
            .rows_between(Window::current_row(), 1);

        let res = df
            .with_column("sum", sum("id").over(window))
            .sort([col("id"), col("category"), col("sum")])
            .collect()
            .await?;

        let id: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2, 3]));
        let category: ArrayRef = Arc::new(StringArray::from(vec!["a", "a", "b", "a", "b", "b"]));
        let sum: ArrayRef = Arc::new(Int64Array::from(vec![2, 3, 3, 2, 5, 3]));

        let expected = RecordBatch::try_from_iter_with_nullable(vec![
            ("id", id, false),
            ("category", category, false),
            ("sum", sum, true),
        ])?;

        assert_eq!(expected, res);

        Ok(())
    }
}
