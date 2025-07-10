import os
import asyncio
import logging
from consumer import consumer


if __name__ == "__main__":
    try:
        asyncio.run(consumer())
    except KeyboardInterrupt:
        logging.info("Client shut down manually.")
