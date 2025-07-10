import os
import aiofiles
import asyncio
import io
import csv
from datetime import datetime, UTC
import logging


"""
Things to do:
Confirm with LW what to do with the data we get. CSV file (If this, need to detail the paths)? Database? Webhook (if this need to research)?
Create a Utiles file - We repeat a number of things in each handle for example User details
"""

# CSV File Paths
reward_claim_file_path = ''
acquiring_assets_file_path = ''
badge_achiement_file_path = ''
message_file_path = ''
level_up_file_path = ''
decayed_currencies_file_path = ''
tier_kept_file_path = ''


# Reward claim event
async def handle_reward_claim_event(event_data):
    """Handles the 'reward.claimed' event."""
    try:
        processing_timestamp = datetime.now(UTC).isoformat()
        event_id = event_data.get('_id')

        reward_data = event_data.get('data', {})

        reward_type = reward_data.get('reward_type')
        reward_name = reward_data.get('name')
        reward_description = reward_data.get('description')
        user = reward_data.get('user')
        event_timestamp = reward_data.get('timestamp')

        # User data
        user_data = reward_data.get('user_data', {})
        user_name = user_data.get('name')
        user_app_id = user_data.get('id')
        current_currencies = user_data.get('current_currencies', {})
        current_points = current_currencies.get('points')
        current_gems = current_currencies.get('gems')

        # Nested reward details
        reward_details = reward_data.get('data', {})
        reward_id = reward_details.get('id')
        reward_amount = reward_details.get('amount')
        claim_type = reward_details.get('claim_type')

        # Custom properties
        properties = reward_details.get('properties', {})
        reward_custom_property = properties.get('reward_custom_property')

        if user and reward_id:
            logging.info(
                f"Logged reward claim: {reward_name} (ID: {reward_id}, Type: {reward_type}, Amount: {reward_amount}) "
                f"for user: {user} (Name: {user_name}) - Claim Type: {claim_type}"
            )

        else:
            logging.warning(
                f"Could not log reward claimed - missing user or reward_id. Data: {event_data}")

    except Exception as e:
        logging.error(
            f"Unexpected error inside handle_reward_claim_event: {e}", exc_info=True)


# Acquiring Assets

async def handle_acquiring_assets_event(event_data):
    """Handles the 'acquire_shop_item' event."""
    try:
        processing_timestamp = datetime.now(UTC).isoformat()
        event_id = event_data.get('_id')

        shop_item_data = event_data.get('data', {})

        event_timestamp = shop_item_data.get('timestamp')
        user = shop_item_data.get('user')

        # Currencies (Cost)
        currencies_spent = shop_item_data.get('currencies', {})
        coins_spent = currencies_spent.get('coins', {}).get('amount')

        # User data
        user_data = shop_item_data.get('user_data', {})
        user_name = user_data.get('name')
        user_app_id = user_data.get('id')
        current_currencies = user_data.get('current_currencies', {})
        current_points = current_currencies.get('points')
        current_gems = current_currencies.get('gems')

        # Nested asset details
        asset_details = shop_item_data.get('data', {})
        asset_id = asset_details.get('id')
        asset_name = asset_details.get('name')
        asset_type = asset_details.get('asset_type')
        acquired_on = asset_details.get('acquired_on')
        amount_acquired = asset_details.get('amount')

        if user and asset_id:
            logging.info(
                f"Logged asset acquisition: {amount_acquired} x {asset_name} (ID: {asset_id}) "
                f"for user: {user} (Name: {user_name}) - Cost: {coins_spent} coins."
            )

        else:
            logging.warning(
                f"Could not log asset acquisition - missing user or asset_id. Data: {event_data}")

    except Exception as e:
        logging.error(
            f"Unexpected error inside handle_acquiring_assets_event: {e}", exc_info=True)


# Badge achievement event
async def handle_badge_achievement_event(event_data):
    """Handles the 'achieve' event for badges."""
    try:
        processing_timestamp = datetime.now(UTC).isoformat()

        event_timestamp = event_data.get('timestamp')
        user = event_data.get('user')
        user_data = event_data.get('user_data', {})
        currencies = event_data.get('currencies', {})

        badge_data = event_data.get('data', {})

        # Access custom property
        custom_property = badge_data.get('custom', {}).get('custom_property')

        # Badge details from badge_data
        badge_name = badge_data.get('name')
        badge_id = badge_data.get('id')
        times_completed = badge_data.get('times_completed')

        # Currency details from currencies
        points_amount = currencies.get('points', {}).get('amount')
        points_amount_received = currencies.get(
            'points', {}).get('amount_received')
        points_multiplier = currencies.get('points', {}).get('multiplier')
        coins_amount = currencies.get('coins', {}).get('amount')
        coins_amount_received = currencies.get(
            'coins', {}).get('amount_received')
        coins_multiplier = currencies.get('coins', {}).get('multiplier')

        # User details from user_data
        user_name = user_data.get('name')
        user_app_id = user_data.get('id')
        current_points = user_data.get('current_currencies', {}).get('points')
        current_coins = user_data.get('current_currencies', {}).get('coins')

        if user and badge_name:
            logging.info(
                f"Logged achieved badge: {badge_name} (user:{user}) - Points: {points_amount_received}, Coins: {coins_amount_received}")

            # async with csv_lock: # Ensure only one task writes at a time
            #     try:
            #         # Check file existence asynchronously
            #         file_exists = False
            #         try:
            #              async with aiofiles.open(badge_achievement_file_path, 'r') as f:
            #                  # If we can open for read, it exists. Check if it has content.
            #                  first_line = await f.readline()
            #                  file_exists = bool(first_line)
            #         except FileNotFoundError:
            #              file_exists = False

            #         # Open for append asynchronously
            #         async with aiofiles.open(badge_achievement_file_path, 'a', newline='', encoding='utf-8') as afp:
            #             # Use io.StringIO to buffer the CSV output before writing to file
            #             output = io.StringIO()
            #             writer = csv.writer(output)

            #             header = ['processing_timestamp', 'event_timestamp', 'user', 'user_name', 'user_app_id', 'badge_name', 'badge_id', 'times_completed', 'points_amount', 'points_amount_received', 'points_multiplier', 'coins_amount', 'coins_amount_received', 'coins_multiplier', 'current_points', 'current_coins', 'custom_property']
            #             row_data = [processing_timestamp, event_timestamp, user, user_name, user_app_id, badge_name, badge_id, times_completed, points_amount, points_amount_received, points_multiplier, coins_amount, coins_amount_received, coins_multiplier, current_points, current_coins, custom_property]

            #             if not file_exists:
            #                 writer.writerow(header)

            #             writer.writerow(row_data)

            #             # Get the string from StringIO and write it asynchronously
            #             await afp.write(output.getvalue())

            #         logging.info(f"Logged achievement event to CSV: user={user}, badge_id={badge_id}, badge={badge_name}")

            #     except Exception as e:
            #         logging.error(f"Error writing achievement to CSV: {e}", exc_info=True)

        else:
            logging.warning(
                f"Could not log badge achievement - missing user or badge name. Data: {event_data}")

    except Exception as e:
        logging.error(
            f"Unexpected error inside handle_badge_achievement_event: {e}", exc_info=True)


# Receiving a Message
async def handle_receiving_a_message_event(event_data):
    """Handles the 'captain message' event."""
    try:
        processing_timestamp = datetime.now(UTC).isoformat()
        event_id = event_data.get('_id')

        # The 'user' is at the top level of the event data.
        user = event_data.get('user')

        # The message data is within the 'data' key.
        message_data = event_data.get('data', {})

        # Top level within event_data
        event_timestamp = event_data.get('timestamp')

        # Currencies (if any)
        currencies = event_data.get('currencies', {})
        points_received = currencies.get('points', {}).get('amount_received')
        coins_received = currencies.get('coins', {}).get('amount_received')

        # User data
        user_data = event_data.get('user_data', {})
        user_name = user_data.get('name')
        user_app_id = user_data.get('id')
        current_currencies = user_data.get('current_currencies', {})
        current_points = current_currencies.get('points')
        current_gems = current_currencies.get('gems')

        # Message details
        message_id = message_data.get('id')
        message_name = message_data.get('name')
        message_title = message_data.get('title')
        message_short_content = message_data.get('short_content')
        message_content = message_data.get('content')  # HTML
        message_image = message_data.get('preset_image')

        # Custom properties
        custom_data = message_data.get('custom', {})
        message_custom_property = custom_data.get('custom_property')

        if user and message_id:
            logging.info(
                f"Logged captain message received: '{message_title}' (ID: {message_id}) "
                f"for user: {user} (Name: {user_name}) "
                f"- Points: {points_received}, Coins: {coins_received}"
            )

        else:
            logging.warning(
                f"Could not log message - missing user or message_id. Data: {event_data}")

    except Exception as e:
        logging.error(
            f"Unexpected error inside handle_receiving_a_message_event: {e}", exc_info=True)


# Level Up
async def handle_level_up_event(event_data):
    """Handles the 'level_up' event."""
    try:
        processing_timestamp = datetime.now(UTC).isoformat()
        event_id = event_data.get('_id')

        level_up_data = event_data.get('data', {})

        event_timestamp = level_up_data.get('timestamp')

        user = level_up_data.get('user')

        # Currencies (Reward/Change)
        currencies_change = level_up_data.get('currencies', {})
        points_change = currencies_change.get(
            'points', {}).get('amount_received')

        # User data
        user_data = level_up_data.get('user_data', {})
        user_name = user_data.get('name')
        user_app_id = user_data.get('id')
        current_currencies = user_data.get('current_currencies', {})
        current_points = current_currencies.get('points')
        current_gems = current_currencies.get('gems')

        # Nested level details
        level_details = level_up_data.get('data', {})
        level_id = level_details.get('id')
        level_name = level_details.get('name')
        level_num = level_details.get('level_num')

        if user and level_name:
            logging.info(
                f"Logged level up: User {user} (Name: {user_name}) reached Level {level_num} "
                f"'{level_name}' (ID: {level_id}). Points change: {points_change}."
            )

        else:
            logging.warning(
                f"Could not log level up - missing user or level_name. Data: {event_data}")

    except Exception as e:
        logging.error(
            f"Unexpected error inside handle_level_up_event: {e}", exc_info=True)

# Decayed currencies


async def handle_decayed_currencies_event(event_data):
    """Handles the 'captain_decay_points' event."""
    try:
        processing_timestamp = datetime.now(UTC).isoformat()

        event_id = event_data.get('_id')
        user = event_data.get('user')
        app_id = event_data.get('app')

        # Main data block
        decay_points_data = event_data.get('data', {})
        event_timestamp = decay_points_data.get('timestamp')

        # Currencies (Decayed amount)
        currencies_decayed = decay_points_data.get('currencies', {})
        points_decayed = currencies_decayed.get('points', {}).get('amount')

        # User data
        user_data = decay_points_data.get('user_data', {})
        user_name = user_data.get('name')
        user_app_id = user_data.get('id')
        current_currencies = user_data.get('current_currencies', {})
        points_after_decay = current_currencies.get('points')

        if user:
            logging.info(
                f"Logged decayed currency: {expired_points_amount}, {expired_coins_amount} (user:{user})")

        else:
            logging.warning(
                f"Could not log decayed currency - missing user. Data: {event_data}")

    except Exception as e:
        logging.error(
            f"Unexpected error inside captain_decay_points: {e}", exc_info=True)


async def handle_tier_kept_event(event_data):
    """Handles the 'tier_kept' event."""
    try:
        processing_timestamp = datetime.now(UTC).isoformat()

        event_id = event_data.get('_id')
        user = event_data.get('user')

    except Exception as e:
        logging.error(
            f"Unexpected error inside handle_tier_kept_event: {e}", exc_info=True)
