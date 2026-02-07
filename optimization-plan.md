# Optimization Plan - Parallelize OpenAlex Flattening

Create a new standalone script `flatten-openalex-jsonl-parallel.py` that parallelizes the flattening process to significantly reduce processing time.

## Why Multiprocessing?

Python has a Global Interpreter Lock (GIL) that prevents multiple threads from executing Python bytecodes at once. This means that for CPU-intensive tasks like parsing JSON and writing CSVs, **threading** wouldn't provide a significant speedup on a single machine.

**Multiprocessing** bypasses the GIL by creating entirely separate memory spaces and Python interpreters for each task, allowing us to truly use all available CPU cores.

## Proposed Changes

### Parallel script creation

#### [NEW] [flatten-openalex-jsonl-parallel.py](file:///home/ywu47/rag_prototype/openalex-documentation-scripts/flatten-openalex-jsonl-parallel.py)

- **Introduction of Two-Level Parallelism**:
    - **Level 1 (Inter-Entity)**: Use `multiprocessing.Process` to run `flatten_authors`, `flatten_works`, etc., simultaneously.
    - **Level 2 (Intra-Entity)**: Within each entity flattener (e.g., `flatten_authors`), use a `ProcessPoolExecutor`.
    - It creates a "pool" of worker processes (usually matching your CPU core count).
    - It takes the 100+ `.gz` files for an entity and hands them out to workers as they become free.
    - This ensures we are always crunching data on all cores until the entire entity is done.
- **Efficient Disk I/O (Binary Concatenation)**:
    - **The Problem**: We want a single output file (e.g., `authors.csv.gz`) to feed into Postgres. If we use 8 processes to process 100 JSONL files in parallel, and all 8 processes try to write their results into that single `authors.csv.gz` at the same time, the file would get corrupted and performance would tank due to "wait times" (file locking).
    - **The Solution**: 
        1. We give each process its own private, **temporary shard** to write to (e.g., `authors_part1.csv.gz`). Since no two processes share a shard, they can all write at maximum speed without interference.
        2. Once all shards are done, the main process "glues" them together at the binary level. This is nearly instantaneous and results in the final, single file we need.
- **Maintain Compatibility**:
    - The final output file structure and location will remain the same (`csv-files/*.csv.gz`), ensuring that `copy-openalex-csv.sql` continues to work without modifications.

## Verification Plan

### Automated Tests
- Since there are no existing unit tests, I will perform a functional verification:
    1. Run the script in "demo mode" (`OPENALEX_DEMO_FILES_PER_ENTITY=1`).
    2. Check that the `csv-files/` directory is populated with `.csv.gz` files.
    3. Use `gunzip -c csv-files/authors.csv.gz | head -n 5` to verify the header and data format.
    4. Verify that multiple entities are processed (check that both `authors.csv.gz` and `works.csv.gz` are created).

### Manual Verification
- Monitor the CPU usage during execution to ensure multiple cores are being utilized.
- Compare the number of rows in the generated CSVs with a sequential run (on a small subset) if possible.
