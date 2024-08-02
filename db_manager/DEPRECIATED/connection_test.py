import os
from dotenv import load_dotenv
import requests
from tqdm import tqdm
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, RetryError
import pandas as pd
from sqlalchemy import create_engine
from queue import Queue, Empty
from threading import Thread

# Take environment variables from .env.
load_dotenv()

# API Configuration
API_KEY = os.getenv("API_KEY")
API_ENDPOINT = "https://api.balldontlie.io/v1/box_scores"

# Set up the headers with the API key.
headers = {
    'Authorization': API_KEY
}

params = {
    "date": '1976-12-14',
}


try:
    response = requests.get(API_ENDPOINT, headers=headers, params=params)
    response.raise_for_status()  
    data = response.json()
    print(data)
except Exception as e:
    print(e)