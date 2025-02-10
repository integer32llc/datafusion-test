use datafusion::common::Result;
use object_store::ObjectStore;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let data_dir = "/Users/carolnichols/Downloads/smaller-repro/";
    let store =
        Arc::new(object_store::memory::InMemory::new()) as Arc<dyn ObjectStore>;

    println!("Reading files into memory");
    for file in std::fs::read_dir(data_dir)? {
        let file = file?.path();
        println!("file: {}", file.display());

        let bytes = std::fs::read(&file)?;

        let path = object_store::path::Path::from(file.display().to_string());
        let payload = object_store::PutPayload::from_bytes(bytes.into());
        store
            .put_opts(&path, payload, object_store::PutOptions::default())
            .await
            .unwrap();

    }

    Ok(())
}
