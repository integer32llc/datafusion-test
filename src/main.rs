use datafusion::{
    common::Result,
    datasource::{
        file_format::{parquet::ParquetFormat, FileFormat},
        listing::ListingOptions,
    },
    execution::object_store::ObjectStoreUrl,
    prelude::*,
};
use log::*;
use object_store::ObjectStore;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let data_dir = "/Users/carolnichols/Downloads/smaller-repro/";
    let store = Arc::new(object_store::memory::InMemory::new()) as Arc<dyn ObjectStore>;

    for file in std::fs::read_dir(data_dir)? {
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

    let df = ctx.sql(query).await?;

    debug!("Getting results...");
    let results = df.collect().await?;

    debug!("Got {} record batches", results.len());

    Ok(())
}
