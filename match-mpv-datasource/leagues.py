import os
from dotenv import load_dotenv
import requests
import psycopg2
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

delay_seconds = 0.24
MAX_RETRIES = 5  # Prevent infinite loops


def fetch_league_data(league_id):
    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = requests.get(
                f"{base_url}/leagues",
                headers={
                    "x-rapidapi-key": api_key,
                    "x-rapidapi-host": host,
                },
                params={"id": league_id, "type": "league", "current": "true"}
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Failed to fetch data for league ID {league_id}: {e}")
            retries += 1
            time.sleep(5)

    print(f"❌ Max retries reached for league ID {league_id}. Skipping...")
    return None


def extract_league_seasons_data(results):
    league_seasons_data = []
    for result in results.get('response', []):
        api_league_id = result.get('league', {}).get('id', '')
        league_name = result.get('league', {}).get('name', '')
        league_type = result.get('league', {}).get('type', '')
        league_logo_url = result.get('league', {}).get('logo', '')
        country = result.get('country', {}).get('name', '')
        country_code = result.get('country', {}).get('code', '')
        country_flag_url = result.get('country', {}).get('flag', '')
        seasons = result.get('seasons', [])

        for season in seasons:
            season_info = {
                'api_league_id': api_league_id,
                'league_name': league_name,
                'league_type': league_type,
                'league_logo_url': league_logo_url,
                'country': country,
                'country_code': country_code,
                'country_flag_url': country_flag_url,
                'season': season.get('year', ''),
                'season_start': season.get('start', ''),
                'season_end': season.get('end', ''),
                'current_season': season.get('current', ''),
                'events': season.get('coverage', {}).get('fixtures', {}).get('events', ''),
                'lineups': season.get('coverage', {}).get('fixtures', {}).get('lineups', ''),
                'statistics_fixtures': season.get('coverage', {}).get('fixtures', {}).get('statistics_fixtures', ''),
                'statistics_players': season.get('coverage', {}).get('fixtures', {}).get('statistics_players', ''),
                'standings': season.get('coverage', {}).get('standings', ''),
                'players': season.get('coverage', {}).get('players', ''),
                'top_scorers': season.get('coverage', {}).get('top_scorers', ''),
                'top_assists': season.get('coverage', {}).get('top_assists', ''),
                'top_cards': season.get('coverage', {}).get('top_cards', ''),
                'injuries': season.get('coverage', {}).get('injuries', ''),
                'predictions': season.get('coverage', {}).get('predictions', ''),
                'odds': season.get('coverage', {}).get('odds', ''),
                'api_source': api_source,
            }
            league_seasons_data.append(season_info)
            print(
                f"✅ {league_name} of {country} has been added for season {season.get('year', 'N/A')}.")

    return league_seasons_data


def save_league_seasons_data_to_db(league_seasons_data):
    try:
        with psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password) as conn:
            with conn.cursor() as cursor:
                for league in league_seasons_data:
                    try:
                        cursor.execute("""
                            SELECT upsert_league_and_season(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            league['api_league_id'],
                            league['league_name'],
                            league['league_type'],
                            league['league_logo_url'],
                            league['country'],
                            league['country_code'],
                            league['country_flag_url'],
                            league['season'],
                            league['season_start'],
                            league['season_end'],
                            league['current_season'],
                            league['events'],
                            league['lineups'],
                            league['statistics_fixtures'],
                            league['statistics_players'],
                            league['standings'],
                            league['players'],
                            league['top_scorers'],
                            league['top_assists'],
                            league['top_cards'],
                            league['injuries'],
                            league['predictions'],
                            league['odds'],
                            league['api_source']
                        ))
                    except Exception as e:
                        print(
                            f"⚠️ Failed to upsert league ID {league['api_league_id']}: {e}")
                        conn.rollback()
                        continue  # ✅ Skip to next

                conn.commit()
                print("💪🏾 League, Seasons and Coverage data successfully saved.")

    except Exception as e:
        print(f"❌ Failed to save league data to PostgreSQL: {e}")


def fetch_and_store_league_ids():

    league_ids = ["39", "40", "61", "78", "88", "94", "135",
                  "140", "144", "169", "179", "207", "203", "235", "253"]

    all_league_seasons_data = []

    for league_id in league_ids:
        league_data = fetch_league_data(league_id)
        if league_data:
            league_seasons_data = extract_league_seasons_data(league_data)
            all_league_seasons_data.extend(league_seasons_data)
            time.sleep(delay_seconds)

    save_league_seasons_data_to_db(all_league_seasons_data)


if __name__ == "__main__":
    fetch_and_store_league_ids()
