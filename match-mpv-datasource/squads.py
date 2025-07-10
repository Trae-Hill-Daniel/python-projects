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


def fetch_team_ids(cursor):
    """Fetch team IDs from the database."""
    try:
        cursor.execute("""
            SELECT tam.api_team_id
            FROM teams_api_mapping tam
        """)
        teams = cursor.fetchall()
        return [{'team_id': team[0]} for team in teams]
    except Exception as e:
        print(f"Failed to fetch team ids: {e}")
        return []


def fetch_squad_details(team_ids):
    """Fetch squad details from the API for given team IDs."""
    squad_details = []
    for team in team_ids:
        success = False
        while not success:
            try:
                response = requests.get(
                    f"{base_url}/players/squads",
                    headers={
                        "x-rapidapi-key": api_key,
                        "x-rapidapi-host": host,
                    },
                    params={"team": team['team_id']}
                )
                if response.status_code == 200:
                    results = response.json()
                    for result in results.get('response', []):
                        players = result.get('players', [])
                        for player in players:
                            squad_details.append({
                                'api_team_id': team['team_id'],
                                'api_player_id': player.get('id', ''),
                                'player_name': player.get('name', ''),
                                'age': player.get('age', ''),
                                'shirt_number': player.get('number', ''),
                                'position': player.get('position', ''),
                                'api_source': api_source,
                            })

                    print(
                        f"Squad pulled for Team ID:{team['team_id']} with {len(players)} players")
                    success = True
                elif response.status_code == 429:
                    print("Rate limit hit, waiting...")
                    time.sleep(60)
                else:
                    print(
                        f"Failed to fetch data for Team ID: {team['team_id']}. Status code: {response.status_code}")
                    success = True
                time.sleep(delay_seconds)
            except Exception as e:
                print(f"An error occurred: {e}")
                success = True
    return squad_details


def save_squad_details(squad_details, cursor):
    """Save squad details to the database."""
    try:
        for squad in squad_details:
            try:
                cursor.execute("""
                    SELECT upsert_squads(%s, %s, %s, %s, %s, %s, %s)
                """, (squad['api_team_id'],
                      squad['api_player_id'],
                      squad['player_name'],
                      squad['age'],
                      squad['shirt_number'],
                      squad['position'],
                      squad['api_source']
                      ))
            except Exception as e:
                print(f"Failed to upsert squad data for Team ID: {
                      squad['api_team_id']}. Error: {e}")
                cursor.connection.rollback()
                break  # Exit the loop on failure
        cursor.connection.commit()
        print("Data successfully saved")
    except Exception as e:
        print(f"Failed to save squad data to PostgreSQL: {e}")
        cursor.connection.rollback()


def fetch_and_store_squads():
    """Main function to orchestrate fetching and storing squads."""
    try:
        with psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password) as conn:
            with conn.cursor() as cursor:
                team_ids = fetch_team_ids(cursor)
                squad_details = fetch_squad_details(team_ids)
                save_squad_details(squad_details, cursor)
    except Exception as e:
        print(f"Failed to fetch and store squads: {e}")


# Run the main function
if __name__ == "__main__":
    fetch_and_store_squads()
