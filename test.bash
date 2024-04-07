#!/bin/bash

source config.env
API_ENDPOINT="https://api.balldontlie.io/v1/box_scores"
DATE="2024-02-07" # Ensure this is in the correct format expected by the API
API_KEY="${BALLDONTLIE_API_KEY}" # Replace with your actual API key

# Make the API request and store the response
response=$(curl -s "$API_ENDPOINT?date=$DATE" -H "Authorization: $API_KEY")

# Output the raw response for debugging purposes
echo "Raw response:"
echo "$response"

# Try to pretty-print the response with jq and redirect stderr to a variable
pretty_response=$(echo "$response" | jq '.' 2>&1)

# Check if jq produced an error
if [ $? -ne 0 ]; then
    echo "Error trying to parse JSON response:"
    echo "$pretty_response"
    exit 1
fi

# If jq didn't produce an error, save the pretty-printed JSON
echo "$pretty_response" > "response_$DATE.json"
echo "Saved formatted JSON to response_$DATE.json"
