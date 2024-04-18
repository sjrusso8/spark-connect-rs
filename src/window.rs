use crate::column::Column;
use crate::expressions::ToVecExpr;
use crate::functions::lit;
use crate::spark;
use crate::spark::expression::window;
use crate::utils::sort_order;

#[derive(Debug, Default, Clone)]
pub struct WindowSpec {
    pub partition_spec: Vec<spark::Expression>,
    pub order_spec: Vec<spark::expression::SortOrder>,
    pub frame: Option<Box<spark::expression::window::WindowFrame>>,
}

impl WindowSpec {
    pub fn new(
        partition_spec: Vec<spark::Expression>,
        order_spec: Vec<spark::expression::SortOrder>,
        frame: Option<Box<spark::expression::window::WindowFrame>>,
    ) -> WindowSpec {
        WindowSpec {
            partition_spec,
            order_spec,
            frame,
        }
    }

    #[allow(non_snake_case)]
    pub fn partitionBy<I: ToVecExpr>(self, cols: I) -> WindowSpec {
        WindowSpec::new(cols.to_vec_expr(), self.order_spec, self.frame)
    }

    #[allow(non_snake_case)]
    pub fn orderBy<I>(self, cols: I) -> WindowSpec
    where
        I: IntoIterator<Item = Column>,
    {
        let order = sort_order(cols);

        WindowSpec::new(self.partition_spec, order, self.frame)
    }

    #[allow(non_snake_case)]
    pub fn rowsBetween(self, start: i64, end: i64) -> WindowSpec {
        let frame = WindowSpec::window_frame(true, start, end);

        WindowSpec::new(self.partition_spec, self.order_spec, frame)
    }

    #[allow(non_snake_case)]
    pub fn rangeBetween(self, start: i64, end: i64) -> WindowSpec {
        let frame = WindowSpec::window_frame(false, start, end);

        WindowSpec::new(self.partition_spec, self.order_spec, frame)
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
                let value = lit(value).expression;

                let boundary = Some(window::window_frame::frame_boundary::Boundary::Value(
                    Box::new(value),
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

#[derive(Debug, Default, Clone)]
pub struct Window {
    spec: WindowSpec,
}

impl Window {
    pub fn new() -> Self {
        Window {
            spec: WindowSpec::default(),
        }
    }

    #[allow(non_snake_case)]
    pub fn currentRow() -> i64 {
        0
    }

    #[allow(non_snake_case)]
    pub fn unboundedFollowing() -> i64 {
        i64::MAX
    }

    #[allow(non_snake_case)]
    pub fn unboundedPreceding() -> i64 {
        i64::MIN
    }

    #[allow(non_snake_case)]
    pub fn partitionBy<I: ToVecExpr>(mut self, cols: I) -> WindowSpec {
        self.spec = self.spec.partitionBy(cols);

        self.spec
    }

    #[allow(non_snake_case)]
    pub fn orderBy<I>(mut self, cols: I) -> WindowSpec
    where
        I: IntoIterator<Item = Column>,
    {
        self.spec = self.spec.orderBy(cols);

        self.spec
    }

    #[allow(non_snake_case)]
    pub fn rangeBetween(mut self, start: i64, end: i64) -> WindowSpec {
        self.spec = self.spec.rangeBetween(start, end);

        self.spec
    }

    #[allow(non_snake_case)]
    pub fn rowsBetween(mut self, start: i64, end: i64) -> WindowSpec {
        self.spec = self.spec.rowsBetween(start, end);

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
        let name: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob"]));
        let age: ArrayRef = Arc::new(Int64Array::from(vec![2, 5]));

        RecordBatch::try_from_iter(vec![("name", name), ("age", age)]).unwrap()
    }

    #[tokio::test]
    async fn test_window_over() -> Result<(), SparkError> {
        let spark = setup().await;

        let data = mock_data();

        let df = spark.createDataFrame(&data)?;

        let window = Window::new()
            .partitionBy(col("name"))
            .orderBy([col("age")])
            .rowsBetween(Window::unboundedPreceding(), Window::currentRow());

        let res = df
            .withColumn("rank", rank().over(window.clone()))
            .withColumn("min", min("age").over(window))
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
}
