
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


def fetch_leagues_for_teams(cursor):
    """Fetch leagues and seasons from the database."""
    leagues_for_teams = []
    try:
        cursor.execute("""
            SELECT lam.api_league_id, s.season
            FROM leagues l
            JOIN leagues_api_mapping lam on lam.league_id = l.league_id
            JOIN seasons s on s.league_id = l.league_id
            WHERE s.current_season = true
        """)
        leagues = cursor.fetchall()

        for league in leagues:
            leagues_for_teams.append(
                {'league_id': league[0], 'season': league[1]})

    except Exception as e:
        print(f"⚠️ Failed to fetch leagues and seasons: {e}")

    return leagues_for_teams


def fetch_team_data_for_league(league):
    """Fetch team data from the API for a given league and season."""
    teams_informations = []
    success = False

    while not success:
        try:
            response = requests.get(
                f"{base_url}/teams",
                headers={
                    "x-rapidapi-key": api_key,
                    "x-rapidapi-host": host,
                },
                params={
                    "league": league['league_id'],
                    "season": league['season']
                }
            )

            if response.status_code == 200:
                results = response.json()
                for result in results.get('response', []):
                    team = result.get('team', {})
                    venue = result.get('venue', {})
                    teams_informations.append({
                        'api_league_id': league['league_id'],
                        'season': league['season'],
                        'api_team_id': team.get('id', ''),
                        'team_name': team.get('name', ''),
                        'team_code': team.get('code', ''),
                        'country': team.get('country', ''),
                        'founded': team.get('founded', ''),
                        'national': team.get('national', ''),
                        'team_logo': team.get('logo', ''),
                        'api_venue_id': venue.get('id', ''),
                        'venue_name': venue.get('name', ''),
                        'address': venue.get('address', ''),
                        'city': venue.get('city', ''),
                        'capacity': venue.get('capacity', ''),
                        'surface': venue.get('surface', ''),
                        'image': venue.get('image', ''),
                        'api_source': api_source,
                    })
                success = True

                print(
                    f"✅ {result.get('team', {})} with venue {venue.get('name', '')} has been added.")

            elif response.status_code == 429:
                print("Rate limit hit, waiting...")
                time.sleep(60)

            else:
                print(f"Failed to fetch data for league ID: {league['league_id']}, season: {
                      league['season']}. Status code: {response.status_code}")
                success = True  # Exit the loop on failure

            time.sleep(delay_seconds)

        except Exception as e:
            print(f"An error occurred: {e}")
            success = True  # Exit the loop on exception

    return teams_informations


def save_team_data_to_db(team_data, cursor):
    """Save the fetched team data to the PostgreSQL database."""
    try:
        for team in team_data:
            try:
                cursor.execute("""
                    SELECT upsert_teams_and_venues(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (team['api_league_id'], team['season'], team['api_team_id'], team['team_name'],
                      team['team_code'], team['country'], team['founded'], team['national'], team['team_logo'],
                      team['api_venue_id'], team['venue_name'], team['address'], team['city'], team['capacity'],
                      team['surface'], team['image'], team['api_source']))

            except Exception as e:
                print(
                    f"⚠️ Failed to upsert team ID {team['api_team_id']} for league ID {team['api_league_id']}. Error: {e}")
                cursor.connection.rollback()
                continue  # ✅ Skip instead of stopping everything

        cursor.connection.commit()
        print("✅ Teams and venue data successfully saved.")

    except Exception as e:
        print(f"❌ Failed to save team data to PostgreSQL: {e}")
        cursor.connection.rollback()


def fetch_and_store_teams():
    """Main function to fetch and store team data."""
    try:
        with psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password) as conn:
            with conn.cursor() as cursor:
                leagues_for_teams = fetch_leagues_for_teams(cursor)
                for league in leagues_for_teams:
                    team_data = fetch_team_data_for_league(league)
                    if team_data:  # ✅ Prevent saving empty data
                        save_team_data_to_db(team_data, cursor)

    except Exception as e:
        print(f"❌ Failed to fetch and store teams: {e}")


if __name__ == "__main__":
    fetch_and_store_teams()
