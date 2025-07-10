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

# 0.24 seconds delay will result in 250 requests per minute. 60 seconds/250 requests
delay_seconds = 0.24
MAX_RETRIES = 5  # Maximum retries if the request fails


def fetch_country_data():
    """
    Fetch country data from the API and handle rate limiting.
    Returns a list of dictionaries containing country data.
    """
    country_data = []
    success = False
    retries = 0  # Initialize retry count

    while not success and retries < MAX_RETRIES:
        try:
            response = requests.get(
                f"{base_url}/countries",
                headers={
                    "x-rapidapi-key": api_key,
                    "x-rapidapi-host": host,
                }
            )

            if response.status_code == 200:
                results = response.json()
                for result in results.get('response', []):
                    country_data.append({
                        'country': result.get('name', ''),
                        'country_code': result.get('code', ''),
                        'country_flag_url': result.get('flag', '')
                    })
                    # ✅ Prints each country
                    print(f"✅ {result.get('name', '')} has been added.")

                success = True  # Mark success to exit the loop

            elif response.status_code == 429:
                print("⏳ Rate limit hit, retrying in 60 seconds...")
                time.sleep(60)  # Wait and retry

            else:
                print(
                    f"❌ Failed to fetch country data. Status code: {response.status_code}")
                retries += 1  # Increase retry count
                time.sleep(5)  # Small delay before retrying

            time.sleep(delay_seconds)  # Delay after each successful API call

        except Exception as e:
            print(f"⚠️ An error occurred: {e}")
            retries += 1
            time.sleep(5)  # Small delay before retrying

    if not success:
        print("❌ Max retries reached. Could not fetch country data.")

    return country_data


def save_country_data_to_db(country_data, cursor):
    """
    Save the fetched country data to the PostgreSQL database.
    """
    try:
        for country in country_data:
            try:
                cursor.execute("""
                    SELECT upsert_country(%s, %s, %s)
                """, (
                    country['country'],
                    country['country_code'],
                    country['country_flag_url']
                ))
            except Exception as e:
                print(
                    f"⚠️ Failed to upsert country: {country['country']}. Error: {e}")
                cursor.connection.rollback()  # Rollback only the failed query
                continue  # Continue to the next country

        cursor.connection.commit()  # Commit successful ones
        print("💪🏾 Country data successfully saved.")

    except Exception as e:
        print(f"❌ Failed to save country data to PostgreSQL: {e}")
        cursor.connection.rollback()


def fetch_and_store_countries():
    """
    Main function to orchestrate fetching and storing country data.
    """
    try:
        with psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password) as conn:
            with conn.cursor() as cursor:
                country_data = fetch_country_data()
                if country_data:  # Only save if data was fetched
                    save_country_data_to_db(country_data, cursor)
                else:
                    print("⚠️ No country data fetched. Skipping database update.")

    except Exception as e:
        print(f"❌ Failed to fetch and store country data: {e}")


# Run the main function
if __name__ == "__main__":
    fetch_and_store_countries()
