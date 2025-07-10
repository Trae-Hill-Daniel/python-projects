import os
from dotenv import load_dotenv
import requests
import psycopg2
from datetime import datetime
import time

# Load environment variables
load_dotenv()
api_key = os.getenv("RAPIDAPI_KEY")
host = os.getenv("HOST_URL")
base_url = os.getenv("API_BASE_URL")
api_source = os.getenv("API_SOURCE")
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
# 0.24 seconds delay will result in 250 requests per minute.  60 seconds/ 250 request
delay_seconds = 0.24


def fetch_leagues_and_seasons(cursor):
    """Fetch leagues and seasons from the database."""
    try:
        cursor.execute("""
            SELECT
                lam.api_league_id,
                s.season,
                c.standings
            FROM leagues l
                JOIN leagues_api_mapping lam on lam.league_id = l.league_id
                JOIN seasons s on s.league_id = l.league_id
                JOIN coverage c on c.season_id = s.season_id
            WHERE s.current_season = true
        """)
        leagues = cursor.fetchall()
        return [
            {'league_id': league[0], 'season': league[1]}
            for league in leagues
            if league[2] is True   # Ensure the league has standings coverage
        ]
    except Exception as e:
        print(f"Failed to fetch leagues and season: {e}")
        return []


def fetch_standings(api_league_id, season):
    """Fetch standings data from the API for a given league and season."""
    try:
        response = requests.get(
            f"{base_url}/standings",
            headers={
                "x-rapidapi-key": api_key,
                "x-rapidapi-host": host,
            },
            params={
                "league": api_league_id,
                "season": season
            }
        )

        if response.status_code == 200:
            return response.json().get('response', [])
        elif response.status_code == 429:
            print("Rate limit hit, waiting...")
            time.sleep(60)
            return fetch_standings(api_league_id, season)
        else:
            print(f"Failed to fetch data for league ID {api_league_id} for season {
                  season}. Status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"An error occurred while fetching standings: {e}")
        return None


def parse_standings_data(api_response):
    """Parse the API response and extract standings information."""
    standings_informations = []
    for result in api_response:
        league = result.get('league', {})
        standings_groups = league.get('standings', [[]])

        for standings in standings_groups:
            for standing in standings:
                standings_info = {
                    'api_league_id': league.get('id', ''),
                    'season': league.get('season', ''),
                    'country': league.get('country', ''),
                    'api_team_id': standing.get('team', {}).get('id', ''),
                    'position': standing.get('rank', ''),
                    'points': standing.get('points', ''),
                    'goals_difference': standing.get('goalsDiff', ''),
                    'group_name': standing.get('group', ''),
                    'form': standing.get('form', ''),
                    'description': standing.get('description', ''),
                    'matches_played': standing.get('all', {}).get('played', ''),
                    'matches_won': standing.get('all', {}).get('win', ''),
                    'matches_drawn': standing.get('all', {}).get('draw', ''),
                    'matches_lost': standing.get('all', {}).get('lose', ''),
                    'goals_scored': standing.get('all', {}).get('goals', {}).get('for', ''),
                    'goals_conceded': standing.get('all', {}).get('goals', {}).get('against', ''),
                    'home_matches_played': standing.get('home', {}).get('played', ''),
                    'home_matches_won': standing.get('home', {}).get('win', ''),
                    'home_matches_drawn': standing.get('home', {}).get('draw', ''),
                    'home_matches_lost': standing.get('home', {}).get('lose', ''),
                    'home_goals_scored': standing.get('home', {}).get('goals', {}).get('for', ''),
                    'home_goals_conceded': standing.get('home', {}).get('goals', {}).get('against', ''),
                    'away_matches_played': standing.get('away', {}).get('played', ''),
                    'away_matches_won': standing.get('away', {}).get('win', ''),
                    'away_matches_drawn': standing.get('away', {}).get('draw', ''),
                    'away_matches_lost': standing.get('away', {}).get('lose', ''),
                    'away_goals_scored': standing.get('away', {}).get('goals', {}).get('for', ''),
                    'away_goals_conceded': standing.get('away', {}).get('goals', {}).get('against', ''),
                    'api_source': api_source,
                }
                standings_informations.append(standings_info)
    return standings_informations


def upsert_standings_data(cursor, standings_informations):
    """Upsert the standings data into the database."""
    try:
        for rank in standings_informations:
            cursor.execute("""
                SELECT upsert_standings(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                rank['api_league_id'],
                rank['season'],
                rank['country'],
                rank['api_team_id'],
                rank['group_name'],
                rank['position'],
                rank['description'],
                rank['points'],
                rank['matches_played'],
                rank['matches_won'],
                rank['matches_drawn'],
                rank['matches_lost'],
                rank['goals_scored'],
                rank['goals_conceded'],
                rank['goals_difference'],
                rank['home_matches_played'],
                rank['home_matches_won'],
                rank['home_matches_drawn'],
                rank['home_matches_lost'],
                rank['home_goals_scored'],
                rank['home_goals_conceded'],
                rank['away_matches_played'],
                rank['away_matches_won'],
                rank['away_matches_drawn'],
                rank['away_matches_lost'],
                rank['away_goals_scored'],
                rank['away_goals_conceded'],
                rank['form'],
                rank['api_source'],
            ))
    except Exception as e:
        print(f"Failed to upsert standings data: {e}")
        raise


def fetch_and_store_league_standings():
    """Main function to fetch and store league standings."""
    try:
        with psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password) as conn:
            with conn.cursor() as cursor:

                standings_for_leagues = fetch_leagues_and_seasons(
                    cursor)

                all_standings_data = []
                for league in standings_for_leagues:
                    api_response = fetch_standings(
                        league['league_id'], league['season'])
                    if api_response:
                        standings_data = parse_standings_data(api_response)
                        all_standings_data.extend(standings_data)
                    time.sleep(delay_seconds)

                if all_standings_data:
                    upsert_standings_data(cursor, all_standings_data)

                conn.commit()
                print("Data successfully saved")

    except Exception as e:
        print(f"An error occurred: {e}")


# Run the main function
if __name__ == "__main__":
    fetch_and_store_league_standings()
