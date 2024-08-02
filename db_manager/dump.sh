#!/bin/bash

# Load .env file
set -a
if [ -f .env ]; then
    source .env
else
    echo ".env file not found!"
    exit 1
fi
set +a

# Check if necessary environment variables are set
if [ -z "$DB_USER" ]; then
    echo "DB_USER is not set in the .env file!"
    exit 1
fi

# Create the directory if it doesn't exist
output_dir="data"
output_file="${output_dir}/box_scores.sql"

if [ ! -d "$output_dir" ]; then
    echo "Output directory does not exist. Creating $output_dir..."
    mkdir -p "$output_dir"
    if [ $? -ne 0 ]; then
        echo "Failed to create directory $output_dir"
        exit 1
    fi
fi

# Dump postgresql database into .sql file
echo "Running pg_dump..."
pg_dump -U "$DB_USER" -d box_scores -f "$output_file"

# Check if pg_dump was successful
if [ $? -eq 0 ]; then
    echo "Database dump successful."
else
    echo "Database dump failed."
    exit 1
fi
