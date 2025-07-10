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


def fetch_players_data():
    """
    Fetch player data from the API
    """
    players_information = []
    page = 1

    while True:
        try:
            response = requests.get(
                f"{base_url}/players/profiles",
                headers={
                    "x-rapidapi-key": api_key,
                    "x-rapidapi-host": host,
                },
                params={
                    "page": page,
                }
            )

            if response.status_code == 200:
                results = response.json()
                players = results.get('response', [])
                current_page = results.get('paging', {}).get('current', 1)
                total_pages = results.get('paging', {}).get('total', 1)

                for player_data in players:
                    player_info = player_data.get('player', {})
                    birth_info = player_info.get('birth', {})

                    ply_info = {
                        'api_player_id': player_info.get('id', ''),
                        'player_name': player_info.get('name', ''),
                        'first_name': player_info.get('firstname', ''),
                        'last_name': player_info.get('lastname', ''),
                        'age': player_info.get('age', ''),
                        'birthday': birth_info.get('date', ''),
                        'birth_place': birth_info.get('place', ''),
                        'birth_country': birth_info.get('country', ''),
                        'nationality': player_info.get('nationality', ''),
                        'height': player_info.get('height', ''),
                        'weight': player_info.get('weight', ''),
                        'shirt_number': player_info.get('number', ''),
                        'position': player_info.get('position', ''),
                        'photo': player_info.get('photo', ''),
                        'api_source': api_source,
                    }
                    players_information.append(ply_info)

                print(f"Page {current_page} of {total_pages} completed.")

                if current_page >= total_pages:
                    break  # Exit loop if all pages fetched

                page += 1
                if page % 2 == 1:
                    # Delay for odd-numbered pages
                    time.sleep(delay_seconds)

            elif response.status_code == 429:
                print("Rate limit hit, waiting...")
                time.sleep(60)

            else:
                print(
                    f"Failed to fetch player data. Status code: {response.status_code}, Response: {response.text}")
                break

            # Delay after each successful API call
            time.sleep(delay_seconds)

        except Exception as e:
            print(f"An error occurred while fetching player data: {e}")
            break

    return players_information


def save_player_data_to_db(players_information, cursor):
    """
    Save the fetched player data to the PostgreSQL database.
    """
    try:
        for ply in players_information:
            try:
                cursor.execute("""
                    SELECT upsert_players(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    ply['api_player_id'],
                    ply['player_name'],
                    ply['first_name'],
                    ply['last_name'],
                    ply['age'],
                    ply['birthday'],
                    ply['birth_place'],
                    ply['birth_country'],
                    ply['nationality'],
                    ply['height'],
                    ply['weight'],
                    ply['shirt_number'],
                    ply['position'],
                    ply['photo'],
                    ply['api_source']
                ))

            except Exception as e:
                print(
                    f"Failed to upsert player data for Player ID {ply['api_player_id']}. Error: {e}")
                cursor.connection.rollback()
                raise  # Re-raise the exception to ensure the commit does not happen

        cursor.connection.commit()
        print("Player Data successfully saved to database.")

    except Exception as e:
        print(f"Failed to save player data to the PostgreSQL database: {e}")


def store_players():
    """
    Main function that stores players.
    """
    try:
        # Establish connection and cursor using the 'with' statement
        with psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password) as conn:
            with conn.cursor() as cursor:

                # Fetch players data
                players_information = fetch_players_data()

                # Save the fetched data to the database
                save_player_data_to_db(players_information, cursor)

    except Exception as e:
        print(f"An error occurred: {e}")


# Run the main function
if __name__ == "__main__":
    store_players()
