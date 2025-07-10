# Python Connector for the Captain Up Firehose API

A robust and easy-to-use Python script for connecting to the Captain Up "firehose" websocket. This tool allows you to subscribe to a real-time stream of events from the Captain Up system, enabling powerful integrations and in-house data analysis.

---

## What is the Captain Up Firehose?

The Captain Up firehose is a websocket connection that streams every kind of event within the Captain Up system to you in a safe and durable way. It is the primary method for receiving real-time data about user engagement, achievements, and rewards.

This script provides a simple and reliable foundation for connecting to the firehose, handling authentication, and processing incoming event data.

## Key Features

-   **Real-time Connection:** Establishes a durable websocket connection to the Captain Up event stream.
-   **Handles All Event Types:** Supports both default and data-intensive event streams.
-   **Easy Configuration:** Uses environment variables for secure and simple setup.
-   **Extendable:** Provides a clear structure for adding your own custom logic to process events.
-   **Lightweight:** Built with standard Python libraries like `websockets` and `asyncio`.

## Getting Started

1. **Clone this repository:**

```bash
git clone https://github.com/your-username/captain-up-firehose-python.git
cd captain-up-firehose-python
```

## Configuration

This script requires credentials to authenticate with the Captain Up API. For security, it is highly recommended to use environment variables.

1.  Create a file named `.env` in the root directory of this project.
2.  Add the following key-value pairs to the `.env` file, replacing the placeholder values with your actual credentials provided by Captain Up:

    ```ini
    # .env file
    APP_ID="your_app_id_here"
    APP_SECRET="your_app_secret"
    ```

The script uses the `python-dotenv` library to automatically load these variables when it runs.
