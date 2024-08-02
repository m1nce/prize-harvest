import os
import requests
from dotenv import load_dotenv
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

# Load environment variables from .env file
load_dotenv()

# API Configuration
API_KEY = os.getenv("API_KEY")
API_ENDPOINT = "https://api.balldontlie.io/v1/box_scores"

# Set up the headers with the API key
headers = {
    'Authorization': API_KEY
}

@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5), retry=retry_if_exception_type(requests.exceptions.RequestException))
def make_request(params):
    response = requests.get(API_ENDPOINT, headers=headers, params=params)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()
