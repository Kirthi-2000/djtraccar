import asyncio
import logging
import os

import aiohttp

from pytraccar import ApiClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler()],
)


async def test() -> None:
    """Example usage of pytraccar."""
    async with aiohttp.ClientSession(
        cookie_jar=aiohttp.CookieJar(unsafe=True)
    ) as client_session:
        # Specify the Traccar demo server URL without the protocol prefix
        client = ApiClient(
            host="46.101.24.212",
            username="traccar123@gmail.com",  # Traccar demo server username
            password="traccar@123",  # Traccar demo server password
            client_session=client_session,
        )

        # Get device information
        devices = await client.get_devices()
        logging.info("Device information:")
        for device in devices:
            logging.info(
                "Device ID: %s, Name: %s, Unique ID: %s",
                device["id"],
                device["name"],
                device["uniqueId"],
            )

        # Get location information
        locations = await client.get_positions()
        logging.info("Location information:")
        for location in locations:
            device_id = location.get("deviceId")
            latitude = location.get("latitude")
            longitude = location.get("longitude")
            time = location.get("time")
            if device_id and latitude and longitude and time:
                logging.info(
                    "Device ID: %s, Latitude: %s, Longitude: %s, Time: %s",
                    device_id,
                    latitude,
                    longitude,
                    time,
                )
            else:
                logging.warning(
                    "Location information: %s", location
                )


asyncio.run(test())
