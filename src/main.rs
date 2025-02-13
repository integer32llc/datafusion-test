#![allow(dead_code, unused_variables)]

use datafusion::{
    common::Result,
    datasource::{
        file_format::{parquet::ParquetFormat, FileFormat},
        listing::ListingOptions,
    },
    execution::{context::TaskContext, object_store::ObjectStoreUrl},
    physical_plan::{coalesce_partitions::CoalescePartitionsExec, ExecutionPlan},
    prelude::*,
};
use futures_util::TryStreamExt;
use log::*;
use object_store::ObjectStore;
use std::{sync::Arc, time::{Duration, Instant}};
use tokio_util::sync::CancellationToken;

const WAIT_BEFORE_CANCEL: Duration = Duration::from_millis(1_000);

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let data_dir = "./data";
    let store = Arc::new(object_store::memory::InMemory::new()) as Arc<dyn ObjectStore>;

    for file in std::fs::read_dir(&data_dir)? {
        let file = file?.path();
        let bytes = std::fs::read(&file)?;

        let path = object_store::path::Path::from(file.display().to_string());
        let payload = object_store::PutPayload::from_bytes(bytes.into());
        store
            .put_opts(&path, payload, object_store::PutOptions::default())
            .await
            .unwrap();
    }
    debug!("Done loading data into in-memory object store");

    let start = Instant::now();
    let token = CancellationToken::new();
    let captured_token = token.clone();

    let join_handle = tokio::spawn(async move {
        debug!("Starting spawned");
        loop {
            let store = Arc::clone(&store);
            tokio::select! {
                biased;
                _ = async move {
                    // If I use blocking sleep here, the program never exits
                    // blocks_too_much().await;

                    // If I use nonblocking sleep here, the program exits in the expected time
                    // yields_enough().await;

                    // If I run a datafusion query....
                    datafusion(store, data_dir).await.unwrap();
                } => {
                    debug!("matched case doing work");
                },
                _ = captured_token.cancelled() => {
                    debug!("Received shutdown request");
                    return;
                },
            }
        }
    });

    debug!("in main, non-blocking sleep");
    tokio::time::sleep(WAIT_BEFORE_CANCEL).await;

    debug!("cancelling thread");
    token.cancel();

    if let Err(e) = join_handle.await {
        debug!("Error waiting for shutdown: {e}");
    }
    debug!("done with main in {:?}; expected to be around {WAIT_BEFORE_CANCEL:?}", start.elapsed());

    Ok(())
}

async fn blocks_too_much() {
    debug!("blocking sleep");
    std::thread::sleep(Duration::from_secs(5));
    debug!("done with blocking sleep");
}

async fn yields_enough() {
    debug!("nonblocking sleep");
    tokio::time::sleep(Duration::from_secs(5)).await;
    debug!("done with nonblocking sleep");
}

async fn datafusion(store: Arc<dyn ObjectStore>, data_dir: &str) -> Result<()> {
    let query = "SELECT distinct \"A\", \"B\", \"C\", \"D\", \"E\" FROM \"test_table\"";
    let file_format = ParquetFormat::default().with_enable_pruning(true);
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(ParquetFormat::default().get_ext());

    let config = SessionConfig::new()
        .with_target_partitions(4)
        .set_bool("datafusion.execution.parquet.pushdown_filters", true);
    let ctx = SessionContext::new_with_config(config);
    let object_store_url = ObjectStoreUrl::parse("test:///").unwrap();
    ctx.register_object_store(object_store_url.as_ref(), store);
    ctx.register_listing_table("test_table", data_dir, listing_options.clone(), None, None)
        .await?;

    debug!("Creating logical plan...");
    let logical_plan = ctx.state().create_logical_plan(query).await?;

    debug!("Creating physical plan...");
    let physical_plan = Arc::new(CoalescePartitionsExec::new(
        ctx.state().create_physical_plan(&logical_plan).await?,
    ));

    debug!("Executing physical plan...");
    let partition = 0;
    let task_context = Arc::new(TaskContext::from(&ctx));
    let stream = physical_plan.execute(partition, task_context)
        .unwrap();

    debug!("Getting results...");
    let results: Vec<_> = stream.try_collect().await?;
    debug!("Got {} record batches", results.len());

    Ok(())
}
