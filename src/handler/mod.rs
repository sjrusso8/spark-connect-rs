use std::io::Read;

use crate::spark;

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;

use spark::execute_plan_response::{ArrowBatch, Metrics};
use spark::{DataType, ExecutePlanResponse};

#[derive(Debug, Clone)]
pub struct ResponseHandler {
    pub schema: Option<DataType>,
    pub data: Vec<Option<ArrowBatch>>,
    pub metrics: Option<Metrics>,
}

impl Default for ResponseHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl ResponseHandler {
    pub fn new() -> ResponseHandler {
        ResponseHandler {
            schema: None,
            data: vec![],
            metrics: None,
        }
    }

    pub fn handle_response(&mut self, response: &ExecutePlanResponse) -> Result<(), String> {
        if let Some(schema) = response.schema.as_ref() {
            self.schema = Some(schema.clone());
        }
        if let Some(metrics) = response.metrics.as_ref() {
            self.metrics = Some(metrics.clone());
        }
        if let Some(data) = response.response_type.as_ref() {
            match data {
                spark::execute_plan_response::ResponseType::ArrowBatch(batch) => {
                    self.data.push(Some(batch.clone()));
                }
                _ => {
                    return Err("Not implemented".to_string());
                }
            }
        }
        Ok(())
    }

    pub fn records(self) -> Result<Vec<RecordBatch>, ArrowError> {
        let mut accumulator: Vec<Vec<RecordBatch>> = vec![vec![]];
        for batch in self.data.into_iter().flatten() {
            accumulator.push(deserialize(batch)?);
        }

        Ok(accumulator
            .into_iter()
            .flatten()
            .collect::<Vec<RecordBatch>>())
    }
}

#[derive(Debug, Clone)]
struct ArrowBatchReader {
    batch: ArrowBatch,
}

impl Read for ArrowBatchReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Read::read(&mut self.batch.data.as_slice(), buf)
    }
}

fn deserialize(batch: ArrowBatch) -> Result<Vec<RecordBatch>, ArrowError> {
    let wrapper = ArrowBatchReader { batch };
    let reader = StreamReader::try_new(wrapper, None)?;
    let mut rows = Vec::new();
    for record in reader {
        rows.push(record?)
    }
    Ok(rows)
}
