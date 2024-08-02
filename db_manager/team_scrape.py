import os
from dotenv import load_dotenv
import requests
import psycopg2
import json

# Take environment variables from .env.
load_dotenv()

# API Configuration
API_KEY = os.getenv("API_KEY")
API_ENDPOINT = "https://api.balldontlie.io/v1/teams"

# Set up the headers with the API key.
headers = {
    'Authorization': API_KEY
}

# Make the GET reqeuest
response = requests.get(API_ENDPOINT, headers=headers)
response_json = response.json()

# PostgreSQL connection details
conn_str = (f"dbname=box_scores user={os.getenv('DB_USER')} " +
            f"password={os.getenv('DB_PASSWORD')} host={os.getenv('DB_HOST')} " +
            f"port={os.getenv('DB_PORT')}")

# Connect to PostgreSQL server
conn = psycopg2.connect(conn_str)
cursor = conn.cursor()

team_insert_query = """
INSERT INTO team (
    team_id, conference, division, city, name, full_name, abbreviation
) VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (team_id) DO NOTHING;
"""

for team in response_json['data']:
    team_tuple = (
        team['id'],
        team.get('conference', None),
        team.get('division', None),
        team.get('city', None),
        team.get('name', None),
        team.get('full_name', None),
        team.get('abbreviation', None)
    )
    cursor.execute(team_insert_query, team_tuple)

# Commit the transaction
conn.commit()

# Close the connection
cursor.close()
conn.close()

print("Teams inserted successfully.")