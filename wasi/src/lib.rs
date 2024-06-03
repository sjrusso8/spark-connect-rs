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

    let window = web_sys::window().expect("no global `window` exists");
    let document = window.document().expect("should have a document on window");
    let body = document.body().expect("document should have a body");

    let val = document
        .create_element("p")
        .expect("failed to create element `p`");

    val.set_text_content(Some(
        "--------------- Start Spark Structured Streaming Job ---------------",
    ));

    body.append_child(&val).expect("failed to apppend text");

    // prints values to console after waiting 10 seconds
    spawn_local(async move {
        for _ in 1..10 {
            console::log_1(&"3 second sleep".into());
            TimeoutFuture::new(3_000).await;

            let json_val = &query.clone().lastProgress().await.unwrap();

            let val = document
                .create_element("p")
                .expect("failed to create element `p`");

            val.set_text_content(Some(&json_val.to_string()));

            body.append_child(&val).expect("failed to apppend text");

            console::log_1(&json_val.to_string().into());
        }

        query.stop().await.unwrap();
    });

    Ok(())
}
