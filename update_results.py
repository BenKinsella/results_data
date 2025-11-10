#!/usr/bin/env python3
import os
import psycopg2
import requests
import logging
import sys
from datetime import datetime, timedelta

# Set up logging as in your odds scraper
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class ResultsUpdaterSportAPI:
    def __init__(self, database_url, api_key, api_host, tournament_id, start_date, end_date):
        self.database_url = database_url
        self.api_key = api_key
        self.api_host = api_host
        self.tournament_id = tournament_id
        self.start_date = start_date
        self.end_date = end_date
        self.conn = None

    def connect_db(self):
        try:
            self.conn = psycopg2.connect(self.database_url)
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {e}")
            sys.exit(1)

    def close_db(self):
        if self.conn:
            self.conn.close()

    def fetch_pinnacle_events(self):
        try:
            with self.conn.cursor() as cursor:
                cutoff_time = datetime.now(timezone.utc) - timedelta(hours=3)
                cursor.execute("""
                    SELECT DISTINCT event_id, home_team, away_team, starts
                    FROM odds1x2
                    WHERE starts < %s
                """, (cutoff_time,))
                rows = cursor.fetchall()
            return [
                {
                    "event_id": r[0],
                    "home_team": r[1].strip().lower() if r[1] else "",
                    "away_team": r[2].strip().lower() if r[2] else "",
                    "starts": r[3]
                } for r in rows
            ]
        except Exception as e:
            logger.error(f"Error fetching Pinnacle events: {e}")
            return []

    def fetch_sportapi_events_for_date(self, date_str):
        url = f"https://sportapi7.p.rapidapi.com/api/v1/sport/football/scheduled-events/{date_str}"
        headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": self.api_host
        }
        try:
            logger.info(f"Requesting events for {date_str} from SportAPI")
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json().get("events", [])
        except Exception as e:
            logger.error(f"API error fetching events for {date_str}: {e}")
            return []

    def match_and_insert_results(self, pinnacle_events, sportapi_events):
        insert_query = """
            INSERT INTO results (event_id, home_team, away_team, starts, home_score, away_score)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        count = 0
        skipped = 0
        with self.conn.cursor() as cursor:
            for api_event in sportapi_events:
                status_type = api_event.get("status", {}).get("type")
                if status_type != "finished":
                    logger.debug("Skipping unfinished event")
                    skipped += 1
                    continue
                if str(api_event.get("tournament", {}).get("id")) != str(self.tournament_id):
                    logger.debug(f"Skipping non-Premier League event: {api_event.get('tournament', {}).get('id')}")
                    skipped += 1
                    continue
                home_score = api_event.get("homeScore", {}).get("normaltime")
                away_score = api_event.get("awayScore", {}).get("normaltime")
                if home_score is None or away_score is None:
                    logger.debug("Skipping event with missing score")
                    skipped += 1
                    continue
                api_home = api_event.get("homeTeam", {}).get("name", "").strip().lower()
                api_away = api_event.get("awayTeam", {}).get("name", "").strip().lower()
                api_start = datetime.fromtimestamp(api_event.get("startTimestamp"))

                matched = False
                for p_event in pinnacle_events:
                    logger.info(f"Comparing Pinnacle: '{p_event['home_team']}' vs '{p_event['away_team']}' @ {p_event['starts']} with API: '{api_home}' vs '{api_away}' @ {api_start}")
                    if (
                        api_home == p_event["home_team"] and
                        api_away == p_event["away_team"] and
                        abs((api_start.date() - p_event["starts"].date()).days) <= 14
                    ):
                        try:
                            cursor.execute(insert_query, (
                                p_event["event_id"],
                                api_event.get("homeTeam", {}).get("name"),
                                api_event.get("awayTeam", {}).get("name"),
                                api_start,
                                home_score,
                                away_score
                            ))
                            count += 1
                            logger.info(f"Inserted result for event_id {p_event['event_id']}")
                        except Exception as e:
                            logger.error(f"DB insert error: {e}")
                        matched = True
                        break
                if not matched:
                    logger.debug(f"No Pinnacle match found for API event: {api_home} vs {api_away} @ {api_start}")
                    skipped += 1
        try:
            self.conn.commit()
        except Exception as e:
            logger.error(f"DB commit error: {e}")
        logger.info(f"Inserted {count} matches, skipped {skipped} events in this batch.")

    def update(self):
        self.connect_db()
        try:
            pinnacle_events = self.fetch_pinnacle_events()
            cur_date = self.start_date
            while cur_date <= self.end_date:
                date_str = cur_date.strftime('%Y-%m-%d')
                sportapi_events = self.fetch_sportapi_events_for_date(date_str)
                logger.info(f"Processing SportAPI events for {date_str}: found {len(sportapi_events)} events.")
                self.match_and_insert_results(pinnacle_events, sportapi_events)
                cur_date += timedelta(days=1)
        finally:
            self.close_db()

if __name__ == "__main__":
    DATABASE_URL = os.environ["DATABASE_URL"]
    SPORTAPI_KEY = os.environ.get("SPORTAPI_KEY")
    SPORTAPI_HOST = os.environ.get("SPORTAPI_HOST", "sportapi7.p.rapidapi.com")
    TOURNAMENT_ID = 384  # Premier League ID for SportAPI

    start_date = datetime.strptime("2025-11-01", "%Y-%m-%d").date()
    end_date = datetime.strptime("2025-11-01", "%Y-%m-%d").date()

    updater = ResultsUpdaterSportAPI(
        database_url=DATABASE_URL,
        api_key=SPORTAPI_KEY,
        api_host=SPORTAPI_HOST,
        tournament_id=TOURNAMENT_ID,
        start_date=start_date,
        end_date=end_date,
    )
    updater.update()
