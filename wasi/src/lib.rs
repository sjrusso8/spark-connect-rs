use wasm_bindgen::prelude::*;
use web_sys::console;

use spark_connect_core::{SparkSession, SparkSessionBuilder};

#[allow(unused_imports)]
use spark_connect_core::functions::col;

#[allow(unused_imports)]
use spark_connect_core::dataframe::SaveMode;

use gloo_timers::future::TimeoutFuture;
use spark_connect_core::streaming::{OutputMode, Trigger};
use wasm_bindgen_futures::spawn_local;

#[wasm_bindgen(main)]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Simple example with read and write
    // let spark: SparkSession = SparkSessionBuilder::remote("sc://127.0.0.1:8080/;user_id=wasi")
    //     .build()
    //     .await?;

    // let df = spark
    //     .clone()
    //     .range(None, 1000, 1, Some(16))
    //     .select(col("id").alias("range_id"));
    //
    // let path = "/opt/spark/examples/src/main/rust/employees/";
    //
    // df.write()
    //     .format("csv")
    //     .mode(SaveMode::Overwrite)
    //     .option("header", "true")
    //     .save(path)
    //     .await?;

    // for some reason you can only get one response during a session
    // after the first response, all other ones are 'malformed'
    let spark: SparkSession =
        SparkSessionBuilder::remote("sc://127.0.0.1:8080/;user_id=wasm_stream_example")
            .build()
            .await?;

    let df = spark
        .readStream()
        .format("rate")
        .option("rowsPerSecond", "5")
        .load(None)?;

    let query = df
        .writeStream()
        .format("console")
        .queryName("example_stream")
        .outputMode(OutputMode::Append)
        .trigger(Trigger::ProcessingTimeInterval("1 seconds".to_string()))
        .start(None)
        .await?;

    // prints values to console after waiting 10 seconds
    spawn_local(async {
        for _ in 1..5 {
            console::log_1(&"10 second sleep".into());
            TimeoutFuture::new(10_000).await;

            let val = &query.clone().lastProgress().await.unwrap();
            console::log_1(&val.to_string().into());
        }

        query.stop().await.unwrap();
    });

    Ok(())
}
