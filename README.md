# Datafusion cancellation problem reproducer

Setup:

Put at least 4 of the sample Parquet files in `data/`.

To run:

```
$ RUST_LOG=debug cargo run
```

In `top`, you should see the datafusion-test binary take 400% CPU for about 8 seconds.
