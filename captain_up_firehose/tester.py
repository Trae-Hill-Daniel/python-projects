import os
import asyncio
import websockets
import logging
import json
from dotenv import load_dotenv
from events import (
    handle_reward_claim_event,
    handle_acquiring_assets_event,
    handle_badge_achievement_event,
    handle_receiving_a_message_event,
    handle_level_up_event,
    handle_decayed_currencies_event
)

# Load environment variables
load_dotenv()
APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
FIREHOSE_URL = f"wss://captainup.com/mechanics/v2/firehose/events?app={APP_ID}&secret={APP_SECRET}"

if not APP_ID or not APP_SECRET:
    raise EnvironmentError(
        f"Missing APP_ID or APP_SECRET in environment variables.")

# Logging config
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

ACKNOWLEDGE_AUIDS = []
MAX_ACKNOWLEDGE_AUIDS = 100
HEARTBEAT_INTERVAL = 30  # seconds

EVENT_HANDLERS = {
    'reward.claimed': handle_reward_claim_event,
    'acquire_shop_item': handle_acquiring_assets_event,
    'achieve': handle_badge_achievement_event,
    'captain message': handle_receiving_a_message_event,
    'level_up': handle_level_up_event,
    'captain_decay_points': handle_decayed_currencies_event,
}


async def heartbeat(websocket):
    """Sends a text-based 'ping' message to keep the connection alive."""
    try:
        while True:
            await websocket.send("ping")
            logging.info("Sent 'ping' message")
            await asyncio.sleep(HEARTBEAT_INTERVAL)
    except websockets.exceptions.ConnectionClosedError:
        logging.warning("Connection closed during heartbeat.")
    except Exception as e:
        logging.error(f"Heartbeat error: {e}")


async def acknowledge_auid(websocket, auid):
    """Acknowledges a received AUID by sending it back."""
    if auid:
        ACKNOWLEDGE_AUIDS.append(auid)
        if len(ACKNOWLEDGE_AUIDS) > MAX_ACKNOWLEDGE_AUIDS:
            ACKNOWLEDGE_AUIDS.pop(0)

        try:
            await websocket.send(json.dumps([auid]))
            logging.info(f"Acknowledged AUID: {auid}")
        except Exception as e:
            logging.error(f"Error sending AUID acknowledgment: {e}")


async def handle_events(event_data):
    """Processes individual event based on its type."""
    event_type = event_data.get('type')
    handler = EVENT_HANDLERS.get(event_type)
    if handler:
        try:
            await handler(event_data)
        except (KeyError, TypeError, AttributeError) as e:
            logging.error(
                f"Error processing event {event_type}: {e} - Data: {event_data}")
        except Exception as e:
            logging.error(
                f"UNEXPECTED error in handler for {event_type}: {e} - Data: {event_data}")
    else:
        logging.warning(f"No handler found for event type: {event_type}")


async def handle_message(websocket, message):
    """Parses and processes the message received from Firehose."""
    try:
        data = json.loads(message)
        if data.get("type") == "error":
            logging.error(f"Firehose error: {data}")
            return

        auid = data.get("auid")
        events = data.get("events", [])

        logging.info(f"Received AUID: {auid} with {len(events)} event(s)")

        if auid in ACKNOWLEDGE_AUIDS:
            logging.info("Duplicate AUID received. Re-acknowledging.")
        await acknowledge_auid(websocket, auid)

        for event in events:
            await handle_events(event)

    except json.JSONDecodeError:
        logging.error(f"Failed to parse message (JSONDecodeError): {message}")

    except AttributeError as e:
        logging.error(
            f"Data structure error (AttributeError) for AUID {auid}: {e}")

    except (KeyError, TypeError) as e:
        logging.error(
            f"Data access/type error (Key/TypeError) for AUID {auid}: {e}")

    except websockets.exceptions.WebSocketException as e:
        logging.error(f"WebSocket communication error for AUID {auid}: {e}")

    except Exception as e:
        logging.critical(
            f"UNEXPECTED error handling message for AUID {auid}: {e}", exc_info=True)


async def consumer():
    """Main consumer loop for handling Firehose connection."""
    print(f"Connecting to: {FIREHOSE_URL}")
    while True:
        try:
            async with websockets.connect(
                FIREHOSE_URL,
                ping_interval=None,
                ping_timeout=None
            ) as websocket:
                logging.info("Connected to Captain Up Firehose.")

                # Start heartbeat
                heartbeat_task = asyncio.create_task(heartbeat(websocket))

                try:
                    async for message in websocket:
                        await handle_message(websocket, message)
                except websockets.exceptions.ConnectionClosed as e:
                    logging.warning(f"WebSocket closed: {e}")
                finally:
                    heartbeat_task.cancel()
                    try:
                        await heartbeat_task
                    except asyncio.CancelledError:
                        pass

        except Exception as e:
            logging.error(f"Connection error: {e}")

        logging.info("Reconnecting in 30 seconds...")
        await asyncio.sleep(30)

if __name__ == "__main__":
    try:
        asyncio.run(consumer())
    except KeyboardInterrupt:
        logging.info("Client shut down manually.")
