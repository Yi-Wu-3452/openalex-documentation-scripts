#!/bin/bash

# Usage with nohup:
# nohup ./openalex-documentation-scripts/load_resilient.sh > load_resilient.log 2>&1 &
#
# To monitor progress:
# tail -f load_resilient.log

set -e

# Configuration
DB_HOST="10.230.100.200"
DB_PORT="5432"
DB_NAME="agentic_fs_research"
DB_USER="ywu47"
export PGPASSWORD='Gakki0611wuyi!'

# Table list to load
TABLES=("topics" "works" "works_authorships" "works_best_oa_locations" "works_biblio" "works_concepts" "works_locations" "works_mesh" "works_open_access" "works_primary_locations" "works_referenced_works" "works_topics")

load_table_resilient() {
    local table=$1
    local csv="csv-files/${table}.csv.gz"
    
    echo "---------------------------------------------------"
    echo "Processing table: $table"
    echo "---------------------------------------------------"
    
    # 1. Clear existing data in the table to avoid duplicates/partials
    podman run --rm -e PGPASSWORD="$PGPASSWORD" postgres:15-alpine psql -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -U "$DB_USER" -c "TRUNCATE openalex.$table;"
    
    # 2. Split and load in chunks of 1 million rows
    # We use temporary files for chunks to ensure speed and reliability
    mkdir -p tmp_chunks
    
    echo "Splitting $csv into chunks..."
    gunzip -c "$csv" | split -l 1000000 --additional-suffix=.csv - tmp_chunks/${table}_chunk_
    
    # Get the header from the first chunk
    header=$(head -n 1 tmp_chunks/${table}_chunk_aa.csv)
    
    for chunk in tmp_chunks/${table}_chunk_*; do
        echo "Loading chunk: $chunk"
        
        # If it's not the first chunk, it won't have the header. 
        # But split includes header only in the first chunk.
        # So we use psql's COPY for each chunk.
        
        if [ "$chunk" == "tmp_chunks/${table}_chunk_aa.csv" ]; then
            # First chunk has the header
            podman run --rm -v $(pwd):/app:Z -e PGPASSWORD="$PGPASSWORD" postgres:15-alpine psql -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -U "$DB_USER" -c "\copy openalex.$table from '/app/$chunk' with (format csv, header true);"
        else
            # Other chunks don't have the header, prepend it or use psql without header
            podman run --rm -v $(pwd):/app:Z -e PGPASSWORD="$PGPASSWORD" postgres:15-alpine psql -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -U "$DB_USER" -c "\copy openalex.$table from '/app/$chunk' with (format csv, header false);"
        fi
        
        if [ $? -ne 0 ]; then
            echo "ERROR: Failed to load chunk $chunk. Skipping to next chunk..."
        else
            rm "$chunk"
        fi
    done
    
    rm -rf tmp_chunks
}

# Main execution
for table in "${TABLES[@]}"; do
    load_table_resilient "$table"
done

echo "All specified tables processed."
