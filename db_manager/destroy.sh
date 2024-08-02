#!/bin/bash

# Load .env file
set -a
source .env
set +a

# PostgreSQL connection details
DB_NAME="box_scores"

# Function to execute a command in PostgreSQL
execute_psql() {
    PGPASSWORD=$DB_PASS psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "$1"
}

# Drop tables if they exist
echo "Dropping existing tables..."
execute_psql "DROP TABLE IF EXISTS team CASCADE;"
execute_psql "DROP TABLE IF EXISTS game CASCADE;"
execute_psql "DROP TABLE IF EXISTS player_game CASCADE;"
execute_psql "DROP TABLE IF EXISTS player_team CASCADE;"
execute_psql "DROP TABLE IF EXISTS team_game CASCADE;"
execute_psql "DROP TABLE IF EXISTS player CASCADE;"
