import countries
import leagues
import teams
import players
import squads
import managers
import standings


def run_countries():
    """Runs the countries script and handles any errors."""
    try:
        print("Running countries script...")
        countries.fetch_and_store_countries()  # Call the main function in countries.py
    except Exception as e:
        print(f"Error occurred in countries script: {e}")
        raise  # Rethrow the exception to stop further execution


def run_leagues():
    """Runs the leagues script and handles any errors."""
    try:
        print("Running leagues script...")
        leagues.fetch_and_store_league_ids()  # Call the main function in leagues.py
    except Exception as e:
        print(f"Error occurred in leagues script: {e}")
        raise  # Rethrow the exception to stop further execution


def run_teams():
    """Runs the teams script and handles any errors."""
    try:
        print("Running teams script...")
        teams.fetch_and_store_teams()  # Call the main function in teams.py
    except Exception as e:
        print(f"Error occurred in teams script: {e}")
        raise  # Rethrow the exception to stop further execution


def run_players():
    """Runs the players script and handles any errors."""
    try:
        print("Running players script...")
        players.store_players()  # Call the main function in players.py
    except Exception as e:
        print(f"Error occurred in players script: {e}")
        raise  # Rethrow the exception to stop further execution


def run_squads():
    """Runs the squads script and handles any errors."""
    try:
        print("Running squads script...")
        squads.fetch_and_store_squads()  # Call the main function in squads.py
    except Exception as e:
        print(f"Error occurred in squads script: {e}")
        raise  # Rethrow the exception to stop further execution


def run_managers():
    """Runs the managers script and handles any errors."""
    try:
        print("Running managers script...")
        managers.fetch_and_store_managers()  # Call the main function in managers.py
    except Exception as e:
        print(f"Error occurred in managers script: {e}")
        raise  # Rethrow the exception to stop further execution


def run_standings():
    """Runs the standings script and handles any errors."""
    try:
        print("Running standings script...")
        # Call the main function in standings.py
        standings.fetch_and_store_league_standings()
    except Exception as e:
        print(f"Error occurred in standings script: {e}")
        raise  # Rethrow the exception to stop further execution


def main():
    try:

        # Run countries script
        run_countries()

        # Run leagues script only if countries ran successfully
        run_leagues()

        # Run teams script only if leagues ran successfully
        run_teams()

        # Run players script only if teams ran successfully
        run_players()

        # Run squads script only if players ran successfully
        run_squads()

        # Run managers script only if squads ran successfully
        run_managers()

        # Run standings script only if managers ran successfully
        run_standings()

        print("All scripts ran successfully!")

    except Exception as e:
        print(f"Process terminated due to error: {e}")


if __name__ == "__main__":
    main()
