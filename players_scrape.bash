#!/bin/bash

# Variables for the API request
source config.env
API_ENDPOINT="https://api.balldontlie.io/api/v1/box_scores"
START_DATE="1980-01-01" # Start date
END_DATE=$(date +%F) # Today's date
API_KEY="${BALLDONTLIE_API_KEY}" # Replace with your actual API key if required

# Variables for database connection
DB_USER="postgres"
DB_NAME="prizes"
DB_HOST="localhost"
DB_PORT="5432"

# Function to insert game data
insert_game_data() {
    # Accepts date as an argument
    local game_date=$1

    # Make the API request
    $(curl -s "$API_ENDPOINT?date=$DATE" -H "Authorization: $API_KEY")

    # Check if response is not empty
    if [ -z "$response" ]; then
        echo "No response for date $game_date"
        return
    fi

    # Parse the JSON response and insert data into the database
    echo $response | jq -c '.data[]' | while read -r line; do
        # Extract game and player data...
        # Construct SQL INSERT commands...
        # Execute SQL commands using psql...
    done
}

# Convert start and end dates to seconds since epoch
start_sec=$(date -d "$START_DATE" +%s)
end_sec=$(date -d "$END_DATE" +%s)

# Loop over each day
for (( d=$start_sec; d<=$end_sec; d+=86400 )); do
    # Convert seconds back to a date string
    current_date=$(date -d @$d +%F)

    # Insert data for current date
    insert_game_data $current_date

    # Sleep for 0.1 seconds (600 requests per minute)
    sleep 0.1
done