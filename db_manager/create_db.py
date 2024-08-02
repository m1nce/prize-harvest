import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql

# Load environment variables from .env
load_dotenv()

# PostgreSQL connection details
admin_conn_str = f"dbname=postgres user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} host={os.getenv('DB_HOST')} port={os.getenv('DB_PORT')}"

# Connect to PostgreSQL as admin
admin_conn = psycopg2.connect(admin_conn_str)
admin_conn.autocommit = True
admin_cursor = admin_conn.cursor()

# Create database if it doesn't exist
database_name = "box_scores"
admin_cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [database_name])
exists = admin_cursor.fetchone()
if not exists:
    admin_cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))
    print(f"Database '{database_name}' created successfully.")
else:
    print(f"Database '{database_name}' already exists.")

admin_cursor.close()
admin_conn.close()

# Connection details for the new database
conn_str = f"dbname={database_name} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} host={os.getenv('DB_HOST')} port={os.getenv('DB_PORT')}"

# Connect to new database
conn = psycopg2.connect(conn_str)
cursor = conn.cursor()

# SQL commands to create tables
create_team_table = """
CREATE TABLE IF NOT EXISTS team (
    team_id INT PRIMARY KEY,
    conference VARCHAR(10),
    division VARCHAR(40),
    city VARCHAR(50),
    name VARCHAR(50),
    full_name VARCHAR(100),
    abbreviation VARCHAR(10)
);
"""

create_player_table = """
CREATE TABLE IF NOT EXISTS player (
    player_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    position VARCHAR(10),
    height VARCHAR(6),
    weight VARCHAR(3),
    jersey_number VARCHAR(10),
    college VARCHAR(50),
    country VARCHAR(50),
    draft_year INT,
    draft_round INT,
    draft_number INT
);
"""

create_game_table = """
CREATE TABLE IF NOT EXISTS game (
    game_id INT PRIMARY KEY,
    date DATE,
    season INT,
    home_team_score INT,
    visitor_team_score INT,
    home_team_id INT,
    visitor_team_id INT,
    FOREIGN KEY (home_team_id) REFERENCES team (team_id),
    FOREIGN KEY (visitor_team_id) REFERENCES team (team_id)
);
"""

create_player_game_table = """
CREATE TABLE IF NOT EXISTS player_game (
    player_id INT,
    game_id INT,
    min FLOAT,
    fgm INT,
    fga INT,
    fg_pct FLOAT,
    fg3m INT,
    fg3a INT,
    fg3_pct FLOAT,
    ftm INT,
    fta INT,
    ft_pct FLOAT,
    oreb INT,
    dreb INT,
    reb INT,
    ast INT,
    stl INT,
    blk INT,
    turnover INT,
    pf INT,
    pts INT,
    PRIMARY KEY (player_id, game_id),
    FOREIGN KEY (player_id) REFERENCES player (player_id),
    FOREIGN KEY (game_id) REFERENCES game (game_id)
);
"""

create_player_team_table = """
CREATE TABLE IF NOT EXISTS player_team (
    player_id int,
    team_id int,
    PRIMARY KEY (player_id, team_id),
    FOREIGN KEY (player_id) REFERENCES player (player_id),
    FOREIGN KEY (team_id) REFERENCES team (team_id)
);
"""

create_team_game_table = """
CREATE TABLE IF NOT EXISTS team_game (
    team_id int,
    game_id int,
    PRIMARY KEY (team_id, game_id),
    FOREIGN KEY (team_id) REFERENCES team (team_id),
    FOREIGN KEY (game_id) REFERENCES game (game_id)
);
"""

# Function to check if table exists
def check_table_exists(table_name):
    cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}');")
    return cursor.fetchone()[0]

# Create tables and print messages
if not check_table_exists('team'):
    cursor.execute(create_team_table)
    print("Table 'team' created successfully.")
else:
    print("Table 'team' already exists.")

if not check_table_exists('player'):
    cursor.execute(create_player_table)
    print("Table 'player' created successfully.")
else:
    print("Table 'player' already exists.")

if not check_table_exists('game'):
    cursor.execute(create_game_table)
    print("Table 'game' created successfully.")
else:
    print("Table 'game' already exists.")

if not check_table_exists('player_game'):
    cursor.execute(create_player_game_table)
    print("Table 'player_game' created successfully.")
else:
    print("Table 'player_game' already exists.")

if not check_table_exists('player_team'):
    cursor.execute(create_player_team_table)
    print("Table 'player_team' created successfully.")
else:
    print("Table 'player_team' already exists.")

if not check_table_exists('team_game'):
    cursor.execute(create_team_game_table)
    print("Table 'team_game' created successfully.")
else:
    print("Table 'team_game' already exists.")

# Commit the transaction
conn.commit()

# CLose the connection
cursor.close()
conn.close()

print("--------------------------------------------")
print("Database and tables created successfully.")