import os
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load environment variables from .env file
load_dotenv()

# Database connection details
conn_str = (f"dbname=box_scores user={os.getenv('DB_USER')} " +
            f"password={os.getenv('DB_PASS')} host={os.getenv('DB_HOST')} " +
            f"port={os.getenv('DB_PORT')}")

# Connect to PostgreSQL server
conn = psycopg2.connect(conn_str)
cursor = conn.cursor()

# Create an engine instance
engine = create_engine(f'postgresql+psycopg2://{os.getenv("DB_USER")}:{os.getenv("DB_PASS")}@{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/nba_stats')

# Insert queries
player_insert_query = """
INSERT INTO player (
    player_id, first_name, last_name, position, height, weight, jersey_number, college, country, draft_year, draft_round, draft_number
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (player_id) DO NOTHING;
"""

game_insert_query = """
INSERT INTO game (
    game_id, date, season, home_team_score, visitor_team_score, home_team_id, visitor_team_id
) VALUES (%s, %s, %s, %s, %s, %s, %s)
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

def close_connection():
    cursor.close()
    conn.close()
