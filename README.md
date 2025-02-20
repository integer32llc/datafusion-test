# Datafusion cancellation problem reproducer

This binary will:

- Look in `./data/` for `.parquet` files in the format it expects; if it doesn't find any, it will
  generate them for you (the generation will take ~minutes but will only happen if there aren't
  files on disk, so if you leave the files around, subsequent runs of this tool won't take as long)
- Load the on-disk files into an in-memory object store, which is necessary to reproduce the issue
  because reading from the in-memory object store does not `await`
- Run a number of tests from 10ms to `MAX_WAIT_TIME` ms that:
  - Spawns a tokio task that `select!`s between running a datafusion query and waiting on a cancellation token
  - Waits the number of milliseconds configured for this test run
  - Cancels the token
  - Reports how long the cancellation took in whole milliseconds
- Prints out the results from all test runs in a nice markdown table suitable for copying to GitHub

## How to run

To best see the markdown table of results, run without any `RUST_LOG` set:

```
$ cargo run
```

To see just the debug logs from this binary:

```
$ RUST_LOG=datafusion_test=debug cargo run
```

To see all debug logs including those from datafusion:

```
$ RUST_LOG=debug cargo run
```

## Variables to adjust

Near the top of the file are three constants extracted for ease of modification:

```
/// Adjust how many data files are generated
const NUM_FILES: usize = 7;
/// Adjust how many rows are generated in each file
const ROWS_PER_FILE: usize = 5_000_000;
/// Adjust the maximum time to wait and let the datafusion code run, in milliseconds. 1 through
/// this number of milliseconds will be tested.
const MAX_WAIT_TIME: u64 = 50;
```

Note that if you change `NUM_FILES` and/or `ROWS_PER_FILE`, you'll need to delete/move any existing
files in the `./data/` dir so that they get regenerated using the new values.

There are many other changes that could be made to this test (for example, the types of data in the
files, the query, datafusion settings) and of course there's probably ways to more clearly
illustrate the problem with less data. These changes are left as an exercise for the reader.

## Sample results

There will be some variance in the results, due to randomness in the files, randomness in task
running, differences in host system, etc. In general, with files generated using these values:

```
NUM_FILES = 7
ROWS_PER_FILE = 5_000_000
MAX_WAIT_TIME = 60
```

the results show there are some points in the datafusion processing that are not awaiting very
often so that they take significantly more than a millisecond to cancel:

| Wait time (ms) | Cancel duration (ms) |
|----------------|----------------------|
| 10 | 17 |
| 11 | 317 |
| 12 | 311 |
| 13 | 398 |
| 14 | 708 |
| 15 | 308 |
| 16 | 307 |
| 17 | 309 |
| 18 | 352 |
| 19 | 301 |
| 20 | 1688 |
| 21 | 300 |
| 22 | 301 |
| 23 | 300 |
| 24 | 312 |
| 25 | 297 |
| 26 | 1140 |
| 27 | 294 |
| 28 | 293 |
| 29 | 340 |
| 30 | 293 |
| 31 | 298 |
| 32 | 290 |
| 33 | 306 |
| 34 | 289 |
| 35 | 291 |
| 36 | 322 |
| 37 | 285 |
| 38 | 360 |
| 39 | 372 |
| 40 | 283 |
| 41 | 283 |
| 42 | 280 |
| 43 | 278 |
| 44 | 2086 |
| 45 | 376 |
| 46 | 276 |
| 47 | 275 |
| 48 | 276 |
| 49 | 275 |
| 50 | 407 |
| 51 | 271 |
| 52 | 269 |
| 53 | 392 |
| 54 | 266 |
| 55 | 266 |
| 56 | 398 |
| 57 | 266 |
| 58 | 267 |
| 59 | 328 |
| 60 | 327 |