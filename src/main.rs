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
use executor::DedicatedExecutor;
use futures_util::TryStreamExt;
use log::*;
use object_store::ObjectStore;
use std::sync::Arc;

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
    let executor = DedicatedExecutor::new(
        "datafusion",
        tokio::runtime::Builder::new_multi_thread(),
        Default::default(),
    );

    let stream = executor.spawn(async move {
            physical_plan.execute(partition, task_context)
        })
        .await.unwrap().unwrap();

    debug!("Getting results...");
    let results: Vec<_> = stream.try_collect().await?;

    debug!("Got {} record batches", results.len());
    executor.join().await;
    Ok(())
}
