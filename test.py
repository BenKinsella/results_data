import os
import requests
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def fetch_sportapi_events_for_date():
    url = f"https://footapi7.p.rapidapi.com/api/tournament/203/season/77142/matches/last/4"
    headers = {
        "X-RapidAPI-Key": os.environ.get("SPORTAPI_KEY"),
        # "X-RapidAPI-Host": api_host
    }
    try:
        logger.info(f"Requesting events from FootAPI")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        events = response.json().get("events", [])
        logger.info(f"Fetched {len(events)} events")
        return events
    except Exception as e:
        logger.error(f"API error fetching events: {e}")
        return []

if __name__ == "__main__":
    # SPORTAPI_KEY = os.environ.get("SPORTAPI_KEY")
    # SPORTAPI_HOST = os.environ.get("SPORTAPI_HOST", "sportapi7.p.rapidapi.com")
    # test_date = "2025-11-01"  # Change to any date you want to test

    events = fetch_sportapi_events_for_date()
    for event in events:
        print(event)
