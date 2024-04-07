#!/bin/bash
#
# Generate a list of dates between a start and end date.

start_date="2023-08-28"
end_date=$(date "+%Y-%m-%d")

# Convert the start and end dates to seconds since the epoch
start_sec=$(date -j -f "%Y-%m-%d" "$start_date" "+%s")
end_sec=$(date -j -f "%Y-%m-%d" "$end_date" "+%s")

# Loop from start date to end date, incrementing by one day each time
for ((current_sec=start_sec; current_sec<=end_sec; current_sec+=86400)); do
    # Use 'date' to convert the current seconds since epoch back to a date string
    echo $(date -j -f "%s" "$current_sec" "+%Y-%m-%d")
done
