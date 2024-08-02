import time
from queue import Queue, Empty
from threading import Thread, Lock
from tqdm import tqdm
from api import make_request
from database import batch_insert

error_dates = []
game_id_counter = 1
counter_lock = Lock()

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
                local_game_id, game['date'], game['season'], game['home_team_score'], 
                game['visitor_team_score'], game['home_team']['id'], game['visitor_team']['id']
            )
            game_records.append(game_record)

            for team in ['home_team', 'visitor_team']:
                for player in game[team]['players']:
                    player_record = (
                        player['player']['id'], player['player']['first_name'], player['player']['last_name'], 
                        player['player']['position'], player['player']['height'], player['player']['weight'], 
                        player['player']['jersey_number'], player['player']['college'], player['player']['country'],
                        none_to_zero(player['player']['draft_year']), none_to_zero(player['player']['draft_round']),none_to_zero(player['player']['draft_number'])
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

def worker(queue, progress_bar, num_workers):
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
def reprocess_error_dates(num_workers):
    queue = Queue()
    for date in error_dates:
        queue.put(date)
    
    threads = []
    with tqdm(total=len(error_dates), desc="Reprocessing errors") as pbar:
        for _ in range(num_workers):  # Number of worker threads
            t = Thread(target=worker, args=(queue, pbar, num_workers))
            t.start()
            threads.append(t)
        
        queue.join()

        for t in threads:
            t.join()
