import os
import time
from dotenv import load_dotenv
import requests
import psycopg2
from tqdm import tqdm
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from sqlalchemy import create_engine
from queue import Queue, Empty
from threading import Thread, Lock
from get_dates import fetch_and_store_data

# Take environment variables from .env.
load_dotenv()

# API Configuration
API_KEY = os.getenv("API_KEY")
API_ENDPOINT = "https://api.balldontlie.io/v1/box_scores"

# Set up the headers with the API key.
headers = {
    'Authorization': API_KEY
}

# Database connection details
conn_str = (f"dbname=box_scores user={os.getenv('DB_USER')} " +
            f"password={os.getenv('DB_PASS')} host={os.getenv('DB_HOST')} " +
            f"port={os.getenv('DB_PORT')}")

# Connect to PostgreSQL server
conn = psycopg2.connect(conn_str)
cursor = conn.cursor()

# Create an engine instance
engine = create_engine(f'postgresql+psycopg2://{os.getenv("DB_USER")}:{os.getenv("DB_PASS")}@{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/nba_stats')

player_insert_query = """
INSERT INTO player (
    player_id, first_name, last_name, position, height, weight, jersey_number, college, country, draft_year, draft_round, draft_number
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (player_id) DO NOTHING;
"""

game_insert_query = """
INSERT INTO game (
    game_id, date, home_team_score, visitor_team_score, home_team_id, visitor_team_id
) VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (game_id) DO NOTHING;
"""

player_game_insert_query = """
INSERT INTO player_game (
    player_id, game_id, min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, oreb, dreb, reb, ast, stl, blk, turnover, pf, pts
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (player_id, game_id) DO NOTHING;
"""

player_team_insert_query = """
INSERT INTO player_team (
    player_id, team_id
) VALUES (%s, %s)
ON CONFLICT (player_id, team_id) DO NOTHING;
"""

team_game_insert_query = """
INSERT INTO team_game (
    team_id, game_id
) VALUES (%s, %s)
ON CONFLICT (team_id, game_id) DO NOTHING;
"""

# List to store dates that encounter errors
error_dates = []

# Initialize the game_id_counter and a lock for thread safety
game_id_counter = 1
counter_lock = Lock()

@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5), retry=retry_if_exception_type(requests.exceptions.RequestException))
def make_request(params):
    response = requests.get(API_ENDPOINT, headers=headers, params=params)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()

def none_to_zero(value):
    return 0 if value is None else value

def none_to_missing(value):
    return "missing" if value == '' else value

def process_date(date):
    global game_id_counter

    params = {
        "date": date,
    }
    try:
        data = make_request(params)
        player_records = []
        game_records = []
        player_game_records = []
        player_team_records = []
        team_game_records = []

        with counter_lock:
            local_game_id = game_id_counter

        for game in data['data']:
            game_record = (
                local_game_id, game['date'], game['home_team_score'], game['visitor_team_score'],
                game['home_team']['id'], game['visitor_team']['id']
            )
            game_records.append(game_record)

            for team in ['home_team', 'visitor_team']:
                for player in game[team]['players']:
                    player_record = (
                        player['player']['id'], player['player']['first_name'], player['player']['last_name'], 
                        none_to_missing(player['player']['position']), player['player']['height'], player['player']['weight'], 
                        player['player']['jersey_number'], none_to_missing(player['player']['college']), none_to_missing(player['player']['country']),
                        none_to_zero(player['player']['draft_year']), none_to_zero(player['player']['draft_round']), none_to_zero(player['player']['draft_number'])
                    )
                    player_records.append(player_record)

                    min_played = player['min']
                    if min_played is None:
                        min_played = 0
                    else:
                        min_parts = min_played.split(":")
                        min_played = int(min_parts[0]) + int(min_parts[1]) / 60 if len(min_parts) == 2 else 0

                    player_game_record = (
                        player['player']['id'], local_game_id, min_played, 
                        none_to_zero(player['fgm']), none_to_zero(player['fga']), none_to_zero(player['fg_pct']), none_to_zero(player['fg3m']), 
                        none_to_zero(player['fg3a']), none_to_zero(player['fg3_pct']), none_to_zero(player['ftm']), none_to_zero(player['fta']), 
                        none_to_zero(player['ft_pct']), none_to_zero(player['oreb']), none_to_zero(player['dreb']), none_to_zero(player['reb']), 
                        none_to_zero(player['ast']), none_to_zero(player['stl']), none_to_zero(player['blk']), none_to_zero(player['turnover']), 
                        none_to_zero(player['pf']), none_to_zero(player['pts'])
                    )
                    player_game_records.append(player_game_record)

                    player_team_record = (player['player']['id'], game[team]['id'])
                    player_team_records.append(player_team_record)

                team_game_record = (game[team]['id'], local_game_id)
                team_game_records.append(team_game_record)

            with counter_lock:
                game_id_counter += 1
                local_game_id = game_id_counter
        
        return player_records, game_records, player_game_records, player_team_records, team_game_records
    except Exception as e:
        print(f"Error processing date {date}: {e}")
        error_dates.append(date)  # Add date to the error list
        return [], [], [], [], []

def batch_insert(player_records, game_records, player_game_records, player_team_records, team_game_records):
    try:
        if player_records:
            cursor.executemany(player_insert_query, player_records)
        if game_records:
            cursor.executemany(game_insert_query, game_records)
        if player_game_records:
            cursor.executemany(player_game_insert_query, player_game_records)
        if player_team_records:
            cursor.executemany(player_team_insert_query, player_team_records)
        if team_game_records:
            cursor.executemany(team_game_insert_query, team_game_records)
        conn.commit()
    except Exception as e:
        print(f"Error during batch insert: {e}")
        conn.rollback()

def worker(queue, progress_bar):
    while True:
        try:
            date = queue.get_nowait()
            player_records, game_records, player_game_records, player_team_records, team_game_records = process_date(date)
            if player_records or game_records or player_game_records or player_team_records or team_game_records:
                batch_insert(player_records, game_records, player_game_records, player_team_records, team_game_records)
            queue.task_done()
            progress_bar.update(1)
            # Add a delay to respect the rate limit
            time.sleep(1 / ((300 / 60) / num_workers)) # Makes at most 300 requests per minute.
        except Empty:
            break
        except Exception as e:
            print(f"Error in worker: {e}")

# Function to reprocess error dates
def reprocess_error_dates():
    queue = Queue()
    for date in error_dates:
        queue.put(date)
    
    threads = []
    with tqdm(total=len(error_dates), desc="Reprocessing errors") as pbar:
        for _ in range(num_workers):  # Number of worker threads
            t = Thread(target=worker, args=(queue, pbar))
            t.start()
            threads.append(t)
        
        queue.join()

        for t in threads:
            t.join()

    conn.commit()

# Create a queue and add dates
print('Getting dates...')
flattened_dates = fetch_and_store_data(range(2014, 2023))
queue = Queue()
for date in flattened_dates:
    queue.put(date)

# Create and start threads
num_workers = 4
threads = []
with tqdm(total=len(flattened_dates)) as pbar:
    for _ in range(num_workers):  # Number of worker threads
        t = Thread(target=worker, args=(queue, pbar))
        t.start()
        threads.append(t)

    # Wait for all tasks in the queue to be processed
    queue.join()

    # Wait for all threads to finish
    for t in threads:
        t.join()

# Reprocess dates that encountered errors
if error_dates:
    print(f"Reprocessing {len(error_dates)} error dates...")
    reprocess_error_dates()

# Close the connection
cursor.close()
conn.close()

print("------------------------------------")
print("Done!")
