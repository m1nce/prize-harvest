import os
from dotenv import load_dotenv
import requests
from tqdm import tqdm
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import numpy as np

# Take environment variables from .env.
load_dotenv()

# API Configuration
API_KEY = os.getenv("API_KEY")
API_ENDPOINT = "https://api.balldontlie.io/v1/games"

# Set up the headers with the API key.
headers = {
    'Authorization': API_KEY
}

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

def fetch_and_store_data(seasons):
    dates = np.array([])
    total_bytes = 0 
    for season in tqdm(seasons, desc="Seasons", unit="season"):
        data_generator = fetch_data(season)
        for data_page, data_size in tqdm(data_generator, desc=f"Season {season}", unit="page", leave=False):
            total_bytes += data_size
            for record in data_page:
                game = record.pop('date')
                dates = np.append(dates, game)
    return np.unique(dates)

if __name__ == "__main__":
    seasons = np.arange(2014, 2023)
    dates = fetch_and_store_data(seasons)
    print(dates)