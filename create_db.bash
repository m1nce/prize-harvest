#!/bin/bash
#
# Creates psql database and tables for the NBA data.

# Database credentials
DB_USER="postgres"
DB_HOST="localhost" # default host for PostgreSQL
DB_PORT="5432" # default port for PostgreSQL

# Connect to PostgreSQL and create the 'prizes' database
psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" -d "postgres" -c "CREATE DATABASE prizes;"

# Connect to PostgreSQL and execute the SQL command in the 'prizes' database
psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" -d "prizes" <<-EOSQL
BEGIN;

CREATE TABLE IF NOT EXISTS team (
    team_id SERIAL PRIMARY KEY,
    team_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS player (
    player_id SERIAL PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    team_id INT REFERENCES team(team_id)
);

CREATE TABLE IF NOT EXISTS game (
    game_id SERIAL PRIMARY KEY,
    date DATE,
    home_team_score INT,
    away_team_score INT,
    home_team_id INT REFERENCES team(team_id),
    away_team_id INT REFERENCES team(team_id),
    CONSTRAINT unique_game UNIQUE (date, home_team_id, away_team_id)
);

CREATE TABLE IF NOT EXISTS player_stat (
    player_id INT REFERENCES player(player_id),
    game_id INT REFERENCES game(game_id),
    minutes INT,
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
    PRIMARY KEY (player_id, game_id)
);

CREATE TABLE IF NOT EXISTS team_game (
    team_id INT REFERENCES team(team_id),
    game_id INT REFERENCES game(game_id),
    PRIMARY KEY (team_id, game_id)
);

COMMIT;
EOSQL