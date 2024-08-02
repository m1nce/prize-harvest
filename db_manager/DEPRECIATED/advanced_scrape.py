import os
from dotenv import load_dotenv
import requests
import psycopg2
from tqdm import tqdm
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import numpy as np

# Take environment variables from .env.
load_dotenv()

# API Configuration
API_KEY = os.getenv("API_KEY")
API_ENDPOINT = "https://api.balldontlie.io/v1/stats/advanced"

# Set up the headers with the API key.
headers = {
    'Authorization': API_KEY
}

# Database connection details
conn_str = (f"dbname=nba_stats user={os.getenv('DB_USER')} " +
            f"password={os.getenv('DB_PASS')} host={os.getenv('DB_HOST')} " +
            f"port={os.getenv('DB_PORT')}")

# Connect to PostgreSQL server
conn = psycopg2.connect(conn_str)
cursor = conn.cursor()

# Prepare insert queries
player_insert_query = """
INSERT INTO player (
    player_id, first_name, last_name, position, height, weight, jersey_number, 
    college, country, draft_year, draft_round, draft_number, team_id
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (player_id) DO NOTHING;
"""

game_insert_query = """
INSERT INTO game (
    game_id, date, season, postseason, home_team_score, away_team_score, 
    home_team_id, away_team_id
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (game_id) DO NOTHING;
"""

player_game_insert_query = """
INSERT INTO player_game (
    player_id, game_id, pie, pace, assist_percentage, assist_ratio, assist_to_turnover, 
    defensive_rating, defensive_rebound_percentage, effective_field_goal_percentage, 
    net_rating, offensive_rating, offensive_rebound_percentage, true_shooting_percentage, 
    turnover_ratio, usage_percentage
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (player_id, game_id) DO NOTHING;
"""

@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5), retry=retry_if_exception_type(requests.exceptions.RequestException))
def make_request(params):
    response = requests.get(API_ENDPOINT, headers=headers, params=params)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response

def fetch_data(season):
    cur_cursor = None
    while True:
        params = {
            "seasons[]": season,
            "per_page": 100,
        }
        if cur_cursor:
            params["cursor"] = cur_cursor
        try:
            response = make_request(params)
            data = response.json()
            yield data['data'], len(response.content)
            cur_cursor = data['meta'].get('next_cursor', None)
            if not cur_cursor:
                break
        except requests.exceptions.HTTPError as e:
            tqdm.write(f"HTTPError: {e.response.status_code} for URL: {e.response.url}")
            break
        except Exception as e:
            tqdm.write(f"Exception: {str(e)}")
            break

# Function to fetch data from the API and insert into PostgreSQL
def fetch_and_store_data(seasons):
    for season in tqdm(seasons, desc="Seasons", unit="season"):
        data_generator = fetch_data(season)
        for data_page, data_size in tqdm(data_generator, desc=f"Season {season}", unit="page", leave=False):
            total_bytes += data_size
            for record in data_page:
                player = record.pop('player')
                game = record.pop('game')

                # Insert into player table
                cursor.execute(player_insert_query, (
                    player['id'], player['first_name'], player['last_name'], player.get('position', None), 
                    player.get('height', None), player.get('weight', None), player.get('jersey_number', None), 
                    player.get('college', None), player.get('country', None), player.get('draft_year', None), 
                    player.get('draft_round', None), player.get('draft_number', None), player.get('team_id', None)
                ))

                # Insert into game table
                cursor.execute(game_insert_query, (
                    game['id'], game['date'], game['season'], game['postseason'], 
                    game['home_team_score'], game['visitor_team_score'], 
                    game['home_team_id'], game['visitor_team_id']
                ))

                # Insert into player_game table
                cursor.execute(player_game_insert_query, (
                    player['id'], game['id'], 
                    record.get('pie', None), record.get('pace', None), 
                    record.get('assist_percentage', None), record.get('assist_ratio', None), 
                    record.get('assist_to_turnover', None), record.get('defensive_rating', None), 
                    record.get('defensive_rebound_percentage', None), record.get('effective_field_goal_percentage', None), 
                    record.get('net_rating', None), record.get('offensive_rating', None), 
                    record.get('offensive_rebound_percentage', None), record.get('true_shooting_percentage', None), 
                    record.get('turnover_ratio', None), record.get('usage_percentage', None)
                ))

            conn.commit()

# List of seasons to fetch data for
seasons = np.arange(2014, 2023)

# Fetch the data and store it in the database
fetch_and_store_data(seasons)

# Close the connection
cursor.close()
conn.close()

print("------------------------------------")
print("Done!")