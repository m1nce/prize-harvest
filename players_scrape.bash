#!/bin/bash
#
# Scrape NBA player and game data from the BallDontLie API and store it in a PostgreSQL database.

source config.env # Load the BALLDONTLIE_API_KEY variable from config.env
API_ENDPOINT="https://api.balldontlie.io/v1/box_scores" # Endpoint for the API
DATE="2024-02-07" # Ensure this is in the correct format expected by the API
API_KEY="${BALLDONTLIE_API_KEY}" # Replace with your actual API key

DB_USER="postgres"
DB_PASS="postgres"
DB_NAME="prizes"
DB_HOST="localhost"
DB_PORT="5432"

#######################################
# Execute an SQL command on the PostgreSQL database.
# Arguments:
#   sql: A string containing the SQL command to be executed.
#######################################
execute_sql() {
    local sql=$1
    PGPASSWORD="$DB_PASS" psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -c "$sql"
}

# Make the API request and store the response
response=$(curl -s "$API_ENDPOINT?date=$DATE" -H "Authorization: $API_KEY")

# Parse the JSON response to extract each game info object
echo "$response" | jq -c '.data[]' | while read -r game_info; do
    # Data for 'game' table
    game_date=$(echo "$game_info" | jq -r '.date')
    home_team_score=$(echo "$game_info" | jq -r '.home_team_score')
    away_team_score=$(echo "$game_info" | jq -r '.visitor_team_score')

    # Data for 'team' table
    home_team_id=$(echo "$game_info" | jq -r '.home_team.id')
    away_team_id=$(echo "$game_info" | jq -r '.visitor_team.id')
    home_team_name=$(echo "$game_info" | jq -r '.home_team.full_name')
    away_team_name=$(echo "$game_info" | jq -r '.visitor_team.full_name')

    # Populate the 'team' table, ignoring duplicates
    execute_sql "INSERT INTO team (team_id, team_name) VALUES ($home_team_id, '$home_team_name') ON CONFLICT (team_id) DO NOTHING;"
    execute_sql "INSERT INTO team (team_id, team_name) VALUES ($away_team_id, '$away_team_name') ON CONFLICT (team_id) DO NOTHING;"

    # Populate the 'game' table
    execute_sql "INSERT INTO game (date, home_team_score, away_team_score, home_team_id, away_team_id) VALUES ('$game_date', $home_team_score, $away_team_score, $home_team_id, $away_team_id);"

    # Get the ID of the newly inserted game
    game_id=$(PGPASSWORD="$DB_PASS" psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -t -A -c "SELECT game_id FROM game WHERE date = '$game_date' AND home_team_id = $home_team_id AND away_team_id = $away_team_id LIMIT 1;")

    # Extract and iterate over the players for the home team
    echo "$game_info" | jq -c '.home_team.players[]' | while read -r game_info; do
        # Data for 'player' table
        player_id=$(echo "$game_info" | jq -r '.player.id')
        first_name=$(echo "$game_info" | jq -r '.player.first_name' | sed "s/'/''/g")
        last_name=$(echo "$game_info" | jq -r '.player.last_name' | sed "s/'/''/g")

        # Populate the 'player' table, ignoring duplicates
        execute_sql "INSERT INTO player (player_id, first_name, last_name, team_id) VALUES ($player_id, '$first_name', '$last_name', '$home_team_id') ON CONFLICT (player_id) DO NOTHING;"

        # Data for 'player_stat' table
        minutes=$(echo "$game_info" | jq -r '.min // 0')
        fgm=$(echo "$game_info" | jq -r '.fgm // 0')
        fga=$(echo "$game_info" | jq -r '.fga // 0')
        fg_pct=$(echo "$game_info" | jq -r '.fg_pct // 0')
        fg3m=$(echo "$game_info" | jq -r '.fg3m // 0')
        fg3a=$(echo "$game_info" | jq -r '.fg3a // 0')
        fg3_pct=$(echo "$game_info" | jq -r '.fg3_pct // 0')
        ftm=$(echo "$game_info" | jq -r '.ftm // 0')
        fta=$(echo "$game_info" | jq -r '.fta // 0')
        ft_pct=$(echo "$game_info" | jq -r '.ft_pct // 0')
        oreb=$(echo "$game_info" | jq -r '.oreb // 0')
        dreb=$(echo "$game_info" | jq -r '.dreb // 0')
        reb=$(echo "$game_info" | jq -r '.reb // 0')
        ast=$(echo "$game_info" | jq -r '.ast // 0')
        stl=$(echo "$game_info" | jq -r '.stl // 0')
        blk=$(echo "$game_info" | jq -r '.blk // 0')
        turnover=$(echo "$game_info" | jq -r '.turnover // 0')
        pf=$(echo "$game_info" | jq -r '.pf // 0')
        pts=$(echo "$game_info" | jq -r '.pts // 0')


        # Populate the 'player_stat' table
        execute_sql "INSERT INTO player_stat (player_id, game_id, minutes, 
        fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, oreb, dreb, 
        reb, ast, stl, blk, turnover, pf, pts) VALUES 
        ($player_id, $game_id, $minutes, $fgm, $fga, $fg_pct, $fg3m, 
        $fg3a, $fg3_pct, $ftm, $fta, $ft_pct, $oreb, $dreb, $reb, $ast, 
        $stl, $blk, $turnover, $pf, $pts) ON CONFLICT (player_id, game_id) DO NOTHING;"
    done

    echo "$game_info" | jq -c '.visitor_team.players[]' | while read -r player; do
        # Data for 'player' table
        player_id=$(echo "$player" | jq -r '.player.id')
        first_name=$(echo "$player" | jq -r '.player.first_name' | sed "s/'/''/g")
        last_name=$(echo "$player" | jq -r '.player.last_name' | sed "s/'/''/g")

        # Populate the 'player' table, ignoring duplicates
        execute_sql "INSERT INTO player (player_id, first_name, last_name, team_id) VALUES ($player_id, '$first_name', '$last_name', '$away_team_id') ON CONFLICT (player_id) DO NOTHING;"

        # Data for 'player_stat' table
        minutes=$(echo "$game_info" | jq -r '.min // 0')
        fgm=$(echo "$game_info" | jq -r '.fgm // 0')
        fga=$(echo "$game_info" | jq -r '.fga // 0')
        fg_pct=$(echo "$game_info" | jq -r '.fg_pct // 0')
        fg3m=$(echo "$game_info" | jq -r '.fg3m // 0')
        fg3a=$(echo "$game_info" | jq -r '.fg3a // 0')
        fg3_pct=$(echo "$game_info" | jq -r '.fg3_pct // 0')
        ftm=$(echo "$game_info" | jq -r '.ftm // 0')
        fta=$(echo "$game_info" | jq -r '.fta // 0')
        ft_pct=$(echo "$game_info" | jq -r '.ft_pct // 0')
        oreb=$(echo "$game_info" | jq -r '.oreb // 0')
        dreb=$(echo "$game_info" | jq -r '.dreb // 0')
        reb=$(echo "$game_info" | jq -r '.reb // 0')
        ast=$(echo "$game_info" | jq -r '.ast // 0')
        stl=$(echo "$game_info" | jq -r '.stl // 0')
        blk=$(echo "$game_info" | jq -r '.blk // 0')
        turnover=$(echo "$game_info" | jq -r '.turnover // 0')
        pf=$(echo "$game_info" | jq -r '.pf // 0')
        pts=$(echo "$game_info" | jq -r '.pts // 0')

        # Populate the 'player_stat' table
        execute_sql "INSERT INTO player_stat (player_id, game_id, minutes, 
        fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, oreb, dreb, 
        reb, ast, stl, blk, turnover, pf, pts) VALUES 
        ($player_id, $game_id, $minutes, $fgm, $fga, $fg_pct, $fg3m, 
        $fg3a, $fg3_pct, $ftm, $fta, $ft_pct, $oreb, $dreb, $reb, $ast, 
        $stl, $blk, $turnover, $pf, $pts) ON CONFLICT (player_id, game_id) DO NOTHING;"
    done
done