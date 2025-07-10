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


def fetch_managers_details(team_ids):
    """Fetch managers details from the API for given team IDs."""
    manager_details = []
    for team in team_ids:  # Loop through each team dynamically
        success = False
        # Get the current team ID dynamically
        current_team_id = team['team_id']
        while not success:
            try:
                response = requests.get(
                    f"{base_url}/coachs",
                    headers={
                        "x-rapidapi-key": api_key,
                        "x-rapidapi-host": host,
                    },
                    # Fetch data only for the current team ID
                    params={"team": current_team_id}
                )
                if response.status_code == 200:
                    results = response.json()
                    for result in results.get('response', []):
                        manager_info = {
                            'api_manager_id': result.get('id'),
                            'manager_name': result.get('name', ''),
                            'first_name': result.get('firstname', ''),
                            'last_name': result.get('lastname', ''),
                            'age': result.get('age', ''),
                            'birthday': result.get('birth', {}).get('date', ''),
                            'birth_place': result.get('birth', {}).get('place', ''),
                            'birth_country': result.get('birth', {}).get('country', ''),
                            'nationality': result.get('nationality', ''),
                            'height': result.get('height', ''),
                            'weight': result.get('weight', ''),
                            'photo_url': result.get('photo', ''),
                            'api_source': api_source,
                            'api_team_id': current_team_id,  # Assign the current team ID
                            'team_name': result.get('team', {}).get('name', '')
                        }

                        # Extract and filter career information dynamically
                        careers = result.get('career', [])
                        for career in careers:
                            # Only store career data if it relates to the current team
                            if career.get('team', {}).get('id') == current_team_id:
                                career_info = {
                                    'api_team_id': career.get('team', {}).get('id', ''),
                                    'team_name': career.get('team', {}).get('name', ''),
                                    'start_date': career.get('start', ''),
                                    'end_date': career.get('end', '')
                                }
                                # Add career info to manager info
                                manager_info.update(career_info)
                                manager_details.append(manager_info)
                                print(
                                    f"Manager: {manager_info['manager_name']} | Team: {manager_info['team_name']}")

                    success = True
                elif response.status_code == 429:
                    print("Rate limit hit, waiting...")
                    time.sleep(60)
                else:
                    print(
                        f"Failed to fetch Manager data for team ID: {current_team_id}. Status: {response.status_code}")
                    success = True
                time.sleep(delay_seconds)
            except Exception as e:
                print(f"API error for team {current_team_id}: {e}")
                success = True
    return manager_details


def save_managers_details(manager_details, cursor):
    """Save managers details to the database."""
    try:
        for manager in manager_details:
            try:
                cursor.execute("""
                    SELECT upsert_managers(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    manager['api_manager_id'],
                    manager['manager_name'],
                    manager['first_name'],
                    manager['last_name'],
                    manager['age'],
                    manager['birthday'],
                    manager['birth_place'],
                    manager['birth_country'],
                    manager['nationality'],
                    manager['height'],
                    manager['weight'],
                    manager['photo_url'],
                    manager['api_team_id'],
                    manager['team_name'],
                    manager['start_date'],
                    manager['end_date'],
                    manager['api_source']
                ))
            except Exception as e:
                print(f"Failed to upsert manager data for Team ID: {
                      manager['api_team_id']}. Error: {e}")
                cursor.connection.rollback()
                break  # Exit the loop on failure
        cursor.connection.commit()
        print("Data successfully saved")
    except Exception as e:
        print(f"Failed to save manager data to PostgreSQL: {e}")
        cursor.connection.rollback()


def fetch_and_store_managers():
    """Main function to orchestrate fetching and storing managers."""
    try:
        with psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password) as conn:
            with conn.cursor() as cursor:
                team_ids = fetch_team_ids(cursor)
                manager_details = fetch_managers_details(team_ids)
                save_managers_details(manager_details, cursor)
    except Exception as e:
        print(f"Failed to fetch and store managers: {e}")


# Run the main function
if __name__ == "__main__":
    fetch_and_store_managers()
