import os
import psycopg2
import requests
from datetime import datetime, timedelta

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
        self.conn = psycopg2.connect(self.database_url)

    def close_db(self):
        if self.conn:
            self.conn.close()

    def fetch_past_pinnacle_events(self):
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT event_id, home_team, away_team, starts
                FROM odds1x2
                WHERE starts < NOW()
            """)
            rows = cursor.fetchall()
        # Build a list of dictionaries for easier fuzzy matching
        return [
            {
                "event_id": r[0],
                "home_team": r[1].strip().lower(),
                "away_team": r[2].strip().lower(),
                "starts": r[3]
            } for r in rows
        ]

    def fetch_sportapi_events_for_date(self, date_str):
        url = f"https://sportapi7.p.rapidapi.com/api/v1/sport/football/scheduled-events/{date_str}"
        headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": self.api_host
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get("events", [])

    def match_and_insert_results(self, pinnacle_events, sportapi_events):
        insert_query = """
            INSERT INTO results (home_team, away_team, starts, home_score, away_score)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        count = 0
        with self.conn.cursor() as cursor:
            for event in sportapi_events:
                # Only completed events with valid scores
                status_type = event.get("status", {}).get("type")
                if status_type != "finished":
                    continue
                sportapi_home = event.get("homeTeam", {}).get("name", "").strip().lower()
                sportapi_away = event.get("awayTeam", {}).get("name", "").strip().lower()
                # Must match the tournament
                if str(event.get("tournament", {}).get("id")) != str(self.tournament_id):
                    continue
                # Scores
                home_score = event.get("homeScore", {}).get("normaltime")
                away_score = event.get("awayScore", {}).get("normaltime")
                if home_score is None or away_score is None:
                    continue
                # Start time as datetime
                starts_dt = datetime.fromtimestamp(event.get("startTimestamp"))
                
                # Fuzzy match in Pinnacle odds by teams and within 2 days
                for p_event in pinnacle_events:
                    p_home = p_event["home_team"]
                    p_away = p_event["away_team"]
                    p_starts = p_event["starts"]
                    # Team and time fuzzy check
                    if (
                        sportapi_home == p_home and
                        sportapi_away == p_away and
                        abs((starts_dt.date() - p_starts.date()).days) <= 2
                    ):
                        cursor.execute(insert_query, (
                            event.get("homeTeam", {}).get("name"),
                            event.get("awayTeam", {}).get("name"),
                            starts_dt,
                            home_score,
                            away_score
                        ))
                        count += 1
                        break   # Only match one result per Pinnacle event
        self.conn.commit()
        print(f"Inserted {count} matched finished matches for this batch.")

    def update(self):
        self.connect_db()
        try:
            pinnacle_events = self.fetch_past_pinnacle_events()
            # Process each date from start_date to end_date (inclusive)
            cur_date = self.start_date
            while cur_date <= self.end_date:
                date_str = cur_date.strftime('%Y-%m-%d')
                try:
                    sportapi_events = self.fetch_sportapi_events_for_date(date_str)
                    print(f"Processing SportAPI events for {date_str}: found {len(sportapi_events)} events.")
                    self.match_and_insert_results(pinnacle_events, sportapi_events)
                except Exception as e:
                    print(f"Error fetching or processing events for {date_str}: {e}")
                cur_date += timedelta(days=1)
        finally:
            self.close_db()

if __name__ == "__main__":
    DATABASE_URL = os.environ.get["DATABASE_URL"]
    SPORTAPI_KEY = os.environ.get["SPORTAPI_KEY"]
    SPORTAPI_HOST = os.environ.get("SPORTAPI_HOST", "sportapi7.p.rapidapi.com")
    TOURNAMENT_ID = 384  # Premier League example

    # Define your date range (e.g. last 45 days)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=45)

    updater = ResultsUpdaterSportAPI(
        database_url=DATABASE_URL,
        api_key=SPORTAPI_KEY,
        api_host=SPORTAPI_HOST,
        tournament_id=TOURNAMENT_ID,
        start_date=start_date,
        end_date=end_date,
    )
    updater.update()
