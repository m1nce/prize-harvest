#!/bin/bash
#
# Scrape NBA player and game data from the BallDontLie API and store it in a PostgreSQL database.

source config.env # Load the BALLDONTLIE_API_KEY variable from config.env
API_ENDPOINT="https://api.balldontlie.io/v1/box_scores" # Endpoint for the API
DATE="2024-02-07" # Ensure this is in the correct format expected by the API
API_KEY="${BALLDONTLIE_API_KEY}" # Replace with your actual API key

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

    # Extract and iterate over the players for the home team
    echo "$game_info" | jq -c '.home_team.players[]' | while read -r game_info; do
        # Data for 'player' table
        player_id=$(echo "$game_info" | jq -r '.player.id')
        first_name=$(echo "$game_info" | jq -r '.player.first_name')
        last_name=$(echo "$game_info" | jq -r '.player.last_name')

        # Data for 'player_stat' table
        minutes=$(echo "$game_info" | jq -r '.min')
        fgm=$(echo "$game_info" | jq -r '.fgm')
        fga=$(echo "$game_info" | jq -r '.fga')
        fg_pct=$(echo "$game_info" | jq -r '.fg_pct')
        fg3m=$(echo "$game_info" | jq -r '.fg3m')
        fg3a=$(echo "$game_info" | jq -r '.fg3a')
        fg3_pct=$(echo "$game_info" | jq -r '.fg3_pct')
        ftm=$(echo "$game_info" | jq -r '.ftm')
        fta=$(echo "$game_info" | jq -r '.fta')
        ft_pct=$(echo "$game_info" | jq -r '.ft_pct')
        oreb=$(echo "$game_info" | jq -r '.oreb')
        dreb=$(echo "$game_info" | jq -r '.dreb')
        reb=$(echo "$game_info" | jq -r '.reb')
        ast=$(echo "$game_info" | jq -r '.ast')
        stl=$(echo "$game_info" | jq -r '.stl')
        blk=$(echo "$game_info" | jq -r '.blk')
        turnover=$(echo "$game_info" | jq -r '.turnover')
        pf=$(echo "$game_info" | jq -r '.pf')
        pts=$(echo "$game_info" | jq -r '.pts')
    done

    echo "$game_info" | jq -c 'away_team.players[]' | while read -r player; do
        # Data for 'player' table
        player_id=$(echo "$player" | jq -r '.player.id')
        first_name=$(echo "$player" | jq -r '.player.first_name')
        last_name=$(echo "$player" | jq -r '.player.last_name')

        # Data for 'player_stat' table
        minutes=$(echo "$player" | jq -r '.min')
        fgm=$(echo "$player" | jq -r '.fgm')
        fga=$(echo "$player" | jq -r '.fga')
        fg_pct=$(echo "$player" | jq -r '.fg_pct')
        fg3m=$(echo "$player" | jq -r '.fg3m')
        fg3a=$(echo "$player" | jq -r '.fg3a')
        fg3_pct=$(echo "$player" | jq -r '.fg3_pct')
        ftm=$(echo "$player" | jq -r '.ftm')
        fta=$(echo "$player" | jq -r '.fta')
        ft_pct=$(echo "$player" | jq -r '.ft_pct')
        oreb=$(echo "$player" | jq -r '.oreb')
        dreb=$(echo "$player" | jq -r '.dreb')
        reb=$(echo "$player" | jq -r '.reb')
        ast=$(echo "$player" | jq -r '.ast')
        stl=$(echo "$player" | jq -r '.stl')
        blk=$(echo "$player" | jq -r '.blk')
        turnover=$(echo "$player" | jq -r '.turnover')
        pf=$(echo "$player" | jq -r '.pf')
        pts=$(echo "$player" | jq -r '.pts')
    done
done