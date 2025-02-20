use arrow::{array::Array, datatypes::DataType, record_batch::RecordBatch};
use datafusion::{
    common::Result,
    datasource::{
        file_format::{parquet::ParquetFormat, FileFormat},
        listing::{ListingOptions, ListingTableUrl},
    },
    execution::{context::TaskContext, object_store::ObjectStoreUrl},
    physical_plan::{coalesce_partitions::CoalescePartitionsExec, ExecutionPlan},
    prelude::*,
    scalar::ScalarValue,
};
use futures_util::TryStreamExt;
use log::*;
use object_store::ObjectStore;
use parquet::arrow::{async_writer::ParquetObjectWriter, AsyncArrowWriter};
use std::{
    iter::repeat_with,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

/// Adjust how many data files are generated
const NUM_FILES: usize = 7;
/// Adjust how many rows are generated in each file
const ROWS_PER_FILE: usize = 5_000_000;
/// Adjust the maximum time to wait and let the datafusion code run, in milliseconds. 1 through
/// this number of milliseconds will be tested.
const MAX_WAIT_TIME: u64 = 60;

fn main() -> Result<()> {
    env_logger::init();

    let data_dir = "data";
    let files_on_disk = find_or_generate_files(data_dir)?;

    let store = Arc::new(object_store::memory::InMemory::new()) as Arc<dyn ObjectStore>;

    debug!("Starting to load data into in-memory object store");
    load_data(Arc::clone(&store), &files_on_disk)?;
    debug!("Done loading data into in-memory object store");

    println!("| Wait time (ms) | Cancel duration (ms) |");
    println!("|----------------|----------------------|");
    for wait_time in 10..=MAX_WAIT_TIME {
        let cancel_duration = run_test(wait_time, Arc::clone(&store), data_dir)?;
        println!("| {wait_time} | {} |", cancel_duration.as_millis());
    }

    Ok(())
}

fn run_test(
    wait_time: u64,
    store: Arc<dyn ObjectStore>,
    data_dir: &'static str,
) -> Result<Duration> {
    let token = CancellationToken::new();
    let captured_token = token.clone();

    let rt = Runtime::new()?;
    rt.spawn(async move {
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

    debug!("in main, sleeping");
    std::thread::sleep(Duration::from_millis(wait_time));

    let start = Instant::now();

    debug!("cancelling thread");
    token.cancel();

    drop(rt);

    let elapsed = start.elapsed();
    debug!("done dropping runtime in {elapsed:?}");

    Ok(elapsed)
}

#[allow(dead_code)]
async fn blocks_too_much() {
    debug!("blocking sleep");
    std::thread::sleep(Duration::from_secs(5));
    debug!("done with blocking sleep");
}

#[allow(dead_code)]
async fn yields_enough() {
    debug!("nonblocking sleep");
    tokio::time::sleep(Duration::from_secs(5)).await;
    debug!("done with nonblocking sleep");
}

async fn datafusion(store: Arc<dyn ObjectStore>, data_dir: &'static str) -> Result<()> {
    let query = "SELECT distinct \"A\", \"B\", \"C\", \"D\", \"E\" FROM \"test_table\"";

    let config = SessionConfig::new()
        .with_target_partitions(4)
        .set_bool("datafusion.execution.parquet.pushdown_filters", true);
    let ctx = SessionContext::new_with_config(config);
    let object_store_url = ObjectStoreUrl::parse("test:///").unwrap();
    ctx.register_object_store(object_store_url.as_ref(), Arc::clone(&store));

    let file_format = ParquetFormat::default().with_enable_pruning(true);
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(ParquetFormat::default().get_ext());

    let table_path = ListingTableUrl::parse(&format!("test:///{data_dir}/"))?;

    ctx.register_listing_table(
        "test_table",
        &table_path,
        listing_options.clone(),
        None,
        None,
    )
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
    let stream = physical_plan.execute(partition, task_context).unwrap();

    debug!("Getting results...");
    let results: Vec<_> = stream.try_collect().await?;
    debug!("Got {} record batches", results.len());

    Ok(())
}

fn find_or_generate_files(data_dir: &'static str) -> Result<Vec<PathBuf>> {
    let files_on_disk = find_files_on_disk(data_dir)?;

    if files_on_disk.is_empty() {
        println!("No data files found, generating (this will take a bit)");
        generate_data(data_dir)?;
        println!("Done generating files");
        let files_on_disk = find_files_on_disk(data_dir)?;

        if files_on_disk.is_empty() {
            panic!("Tried to generate data files but there are still no files on disk");
        } else {
            println!("Using {} files now on disk", files_on_disk.len());
            Ok(files_on_disk)
        }
    } else {
        println!("Using {} files found on disk", files_on_disk.len());
        Ok(files_on_disk)
    }
}

fn find_files_on_disk(data_dir: &'static str) -> Result<Vec<PathBuf>> {
    Ok(std::fs::read_dir(&data_dir)?
        .into_iter()
        .filter_map(|file| {
            let path = file.unwrap().path();
            if path
                .extension()
                .map(|ext| (ext == "parquet"))
                .unwrap_or(false)
            {
                Some(path)
            } else {
                None
            }
        })
        .collect())
}

fn load_data(
    store: Arc<dyn ObjectStore>,
    files_on_disk: &[PathBuf],
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let rt = Runtime::new()?;
    rt.block_on(async {
        for file in files_on_disk {
            let bytes = std::fs::read(&file)?;

            let path = object_store::path::Path::from(file.display().to_string());
            let payload = object_store::PutPayload::from_bytes(bytes.into());
            store
                .put_opts(&path, payload, object_store::PutOptions::default())
                .await?;
        }

        Ok(())
    })
}

fn generate_data(
    data_dir: &'static str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let absolute = std::env::current_dir().unwrap().join(data_dir);
    let store = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(
        absolute,
    )?);

    let columns = [
        ("A", DataType::Float64),
        ("B", DataType::Float64),
        ("C", DataType::Float64),
        ("D", DataType::Boolean),
        ("E", DataType::Utf8),
        ("F", DataType::Utf8),
        ("G", DataType::Utf8),
        ("H", DataType::Utf8),
        ("I", DataType::Utf8),
        ("J", DataType::Utf8),
        ("K", DataType::Utf8),
    ];

    let rt = Runtime::new()?;
    rt.block_on(async {
        for file_num in 1..=NUM_FILES {
            println!("Generating file {file_num} of {NUM_FILES}");
            let data = columns.iter().map(|(column_name, column_type)| {
                let column = random_data(column_type, ROWS_PER_FILE);
                (column_name, column)
            });
            let to_write = RecordBatch::try_from_iter(data).unwrap();
            let path = object_store::path::Path::from(format!("{file_num}.parquet").as_str());
            let object_store_writer = ParquetObjectWriter::new(Arc::clone(&store) as _, path);

            let mut writer = AsyncArrowWriter::try_new(object_store_writer, to_write.schema(), None)?;
            writer.write(&to_write).await?;
            writer.close().await?;
        }

        Ok(())
    })
}

fn random_data(column_type: &DataType, rows: usize) -> Arc<dyn Array> {
    let values = (0..rows).into_iter().map(|_| random_value(column_type));
    ScalarValue::iter_to_array(values).unwrap()
}

fn random_value(column_type: &DataType) -> ScalarValue {
    match column_type {
        DataType::Float64 => ScalarValue::Float64(Some(fastrand::f64())),
        DataType::Boolean => ScalarValue::Boolean(Some(fastrand::bool())),
        DataType::Utf8 => {
            ScalarValue::Utf8(Some(repeat_with(fastrand::alphanumeric).take(10).collect()))
        }
        other => unimplemented!("No random value generation implemented for {other:?}"),
    }
}
