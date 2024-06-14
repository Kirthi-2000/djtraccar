import asyncio
import logging
import aiomysql
import aiohttp
from pytraccar import ApiClient
import json
from typing import Any, TypedDict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler()],
)

class ServerModel(TypedDict):
    id: int
    registration: bool
    readonly: bool
    deviceReadonly: bool
    limitCommands: bool
    map: str | None
    bingKey: str | None
    mapUrl: str | None
    poiLayer: str | None
    latitude: float
    longitude: float
    zoom: int
    twelveHourFormat: bool
    version: str
    forceSettings: bool
    coordinateFormat: str | None
    attributes: dict[str, Any]
    openIdEnabled: bool
    openIdForce: bool

class PositionModel(TypedDict):
    id: int
    deviceId: int
    protocol: str
    deviceTime: str
    fixTime: str
    serverTime: str
    outdated: bool
    valid: bool
    latitude: float
    geofenceIds: list[int] | None
    longitude: float
    altitude: int
    speed: int
    course: int
    address: str | None
    accuracy: int
    network: dict[str, Any] | None
    attributes: dict[str, Any]

async def connect_to_database():
    conn = await aiomysql.connect(
        host='localhost',
        port=3306,
        user='root',
        password='Kiruthi@12',
        db='pytraccar',
        autocommit=True
    )
    return conn

async def create_tables(conn):
    async with conn.cursor() as cur:
        await cur.execute("""
            CREATE TABLE IF NOT EXISTS servers (
                id INT PRIMARY KEY,
                registration BOOLEAN,
                readonly BOOLEAN,
                deviceReadonly BOOLEAN,
                limitCommands BOOLEAN,
                map VARCHAR(255),
                bingKey VARCHAR(255),
                mapUrl VARCHAR(255),
                poiLayer VARCHAR(255),
                latitude DECIMAL(10, 8),
                longitude DECIMAL(11, 8),
                zoom INT,
                twelveHourFormat BOOLEAN,
                version VARCHAR(255),
                forceSettings BOOLEAN,
                coordinateFormat VARCHAR(255),
                attributes JSON,
                openIdEnabled BOOLEAN,
                openIdForce BOOLEAN
            )
        """)
        await cur.execute("""
            CREATE TABLE IF NOT EXISTS devices (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                unique_id VARCHAR(255),
                status VARCHAR(255),
                disabled BOOLEAN,
                lastUpdate DATETIME,
                positionId INT,
                groupId INT,
                phone VARCHAR(255),
                model VARCHAR(255),
                contact VARCHAR(255),
                category VARCHAR(255),
                attributes JSON
            )
        """)
        await cur.execute("""
            CREATE TABLE IF NOT EXISTS location_information (
                id INT AUTO_INCREMENT PRIMARY KEY,
                device_id INT,
                protocol VARCHAR(255),
                server_time DATETIME,
                device_time DATETIME,
                fix_time DATETIME,
                outdated BOOLEAN,
                valid BOOLEAN,
                latitude DECIMAL(10, 8) NOT NULL,
                longitude DECIMAL(11, 8) NOT NULL,
                altitude DECIMAL(10, 2) NOT NULL,
                speed DECIMAL(10, 2),
                course DECIMAL(10, 2),
                accuracy DECIMAL(10, 2),
                FOREIGN KEY (device_id) REFERENCES devices(id)
            )
        """)

async def insert_server_info(conn, server):
    async with conn.cursor() as cur:
        try:
            logging.info(f"Inserting server info: {server}")
            await cur.execute("""
                INSERT INTO servers (
                    id, registration, readonly, deviceReadonly, limitCommands, map, bingKey, mapUrl,
                    poiLayer, latitude, longitude, zoom, twelveHourFormat, version, forceSettings,
                    coordinateFormat, attributes, openIdEnabled, openIdForce
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                server["id"], server["registration"], server["readonly"], server["deviceReadonly"],
                server["limitCommands"], server["map"], server["bingKey"], server["mapUrl"],
                server["poiLayer"], server["latitude"], server["longitude"], server["zoom"],
                server.get("twelveHourFormat", False), server["version"], server["forceSettings"],
                server["coordinateFormat"], json.dumps(server["attributes"]),
                server["openIdEnabled"], server["openIdForce"]
            ))
        except aiomysql.IntegrityError as e:
            if e.args[0] == 1062:  # Duplicate entry error code
                logging.warning(f"Server {server['id']} already exists in the database. Skipping insertion.")
            else:
                logging.error(f"Failed to insert server {server['id']}: {e}")
        except Exception as e:
            logging.error(f"Failed to insert server {server['id']}: {e}")


async def insert_device_info(conn, device):
    async with conn.cursor() as cur:
        try:
            await cur.execute("""
                INSERT INTO devices (
                    id, name, unique_id, status, disabled, lastUpdate, positionId, groupId,
                    phone, model, contact, category, attributes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                device["id"], device["name"], device["uniqueId"], device["status"], device["disabled"],
                device["lastUpdate"].replace('T', ' ').replace('Z', '') if device["lastUpdate"] else None,
                device["positionId"], device["groupId"], device["phone"],
                device["model"], device["contact"], device["category"], json.dumps(device["attributes"])
            ))
        except aiomysql.IntegrityError as e:
            if e.args[0] == 1062:  # Duplicate entry error code
                logging.warning(f"Device {device['id']} already exists in the database. Skipping insertion.")
            else:
                logging.error(f"Failed to insert device {device['id']}: {e}")
        except Exception as e:
            logging.error(f"Failed to insert device {device['id']}: {e}")

async def insert_location_info(conn, location):
    async with conn.cursor() as cur:
        try:
            latitude = location.get('latitude')
            longitude = location.get('longitude')

            # Ensure latitude and longitude are valid numbers
            if latitude is None or longitude is None:
                logging.error(f"Latitude or Longitude is None. Location: {location}")
                return

            if not isinstance(latitude, (int, float)) or not isinstance(longitude, (int, float)):
                logging.error(f"Invalid latitude or longitude values: {latitude}, {longitude}")
                return

            logging.info(f"Inserting location: {json.dumps(location)}")

            await cur.execute("""
                INSERT INTO location_information 
                (device_id, protocol, server_time, device_time, fix_time, outdated, valid, latitude, longitude, altitude, speed, course, accuracy) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                location['deviceId'],
                location['protocol'],
                location['serverTime'].replace('T', ' ').replace('.000+00:00', '') if location['serverTime'] else None,
                location['deviceTime'].replace('T', ' ').replace('.000+00:00', '') if location['deviceTime'] else None,
                location['fixTime'].replace('T', ' ').replace('.000+00:00', '') if location['fixTime'] else None,
                location['outdated'],
                location['valid'],
                latitude,
                longitude,
                location['altitude'],
                location.get('speed', None),
                location.get('course', None),
                location.get('accuracy', None)
            ))

            logging.info(f"Successfully inserted location data for device ID: {location['deviceId']}")

        except aiomysql.IntegrityError as e:
            if e.args[0] == 1062:  # Duplicate entry error code
                logging.warning(f"Location data for device {location['deviceId']} already exists in the database. Skipping insertion.")
            else:
                logging.error(f"Failed to insert location data for device {location['deviceId']}: {e}")
        except Exception as e:
            logging.error(f"Failed to insert location data for device {location['deviceId']}: {e}")

async def main():
    conn = await connect_to_database()
    await create_tables(conn)

    async with aiohttp.ClientSession() as session:
        api = ApiClient(username='traccar123@gmail.com', password='traccar@123', url='http://46.101.24.212:8082/api', host='46.101.24.212', client_session=session)

        try:
            # Replace these method calls with the correct ones
            server: ServerModel = await api.get_server()  # Assuming the method is get_server()
            await insert_server_info(conn, server)

            devices: list[dict[str, Any]] = await api.get_devices()  # Assuming the method is get_devices()
            for device in devices:
                await insert_device_info(conn, device)

            positions: list[PositionModel] = await api.get_positions()  # Assuming the method is get_positions()
            for position in positions:
                await insert_location_info(conn, position)
        except aiohttp.ClientError as e:
            logging.error(f"HTTP request failed: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    asyncio.run(main())
