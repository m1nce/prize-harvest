#!/bin/bash

# Load environment variables from .env file
export $(grep -v '^#' .env | xargs)

# Default date range (can be overridden by command-line arguments)
START_YEAR=2014
END_YEAR=2023
NUM_WORKERS=4

# Check for command-line arguments and override default date range if provided
while getopts s:e:w: flag
do
    case "${flag}" in
        s) START_YEAR=${OPTARG};;
        e) END_YEAR=${OPTARG};;
        w) NUM_WORKERS=${OPTARG};;
    esac
done

# Run the Python script with the provided date range and number of workers
python main.py --start_year $START_YEAR --end_year $END_YEAR --num_workers $NUM_WORKERS
