import json
import logging
import aiomysql
import asyncio

logging.basicConfig(level=logging.INFO)

async def create_pool():
    return await aiomysql.create_pool(
        host='localhost',
        port=3306,
        user='root',
        password='Kiruthi@12',
        db='pytraccar',
        autocommit=True
    )

def format_datetime(dt_str):
    return dt_str.replace('T', ' ').split('.')[0] if dt_str else None

async def device_exists(conn, device_id):
    async with conn.cursor() as cur:
        await cur.execute("SELECT COUNT(*) FROM devices WHERE id = %s", (device_id,))
        (count,) = await cur.fetchone()
        return count > 0

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
                format_datetime(device["lastUpdate"]),
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

            # Check if the device exists
            if not await device_exists(conn, location['deviceId']):
                logging.error(f"Device {location['deviceId']} does not exist. Skipping insertion.")
                return

            await cur.execute("""
                INSERT INTO location_information 
                (device_id, protocol, server_time, device_time, fix_time, outdated, valid, latitude, longitude, altitude, speed, course, accuracy) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                location['deviceId'],
                location['protocol'],
                format_datetime(location['serverTime']),
                format_datetime(location['deviceTime']),
                format_datetime(location['fixTime']),
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
            if e.args[0] == 1452:  # Foreign key constraint fails
                logging.error(f"Foreign key constraint failed for device {location['deviceId']}. Ensure the device ID exists in the devices table.")
            elif e.args[0] == 1062:  # Duplicate entry error code
                logging.warning(f"Location data for device {location['deviceId']} already exists in the database. Skipping insertion.")
            else:
                logging.error(f"Failed to insert location data for device {location['deviceId']}: {e}")
        except Exception as e:
            logging.error(f"Failed to insert location data for device {location['deviceId']}: {e}")

async def main():
    pool = await create_pool()

    async with pool.acquire() as conn:
        device = {
            "id": 885,
            "name": "Device 885",
            "uniqueId": "unique-885",
            "status": "online",
            "disabled": False,
            "lastUpdate": "2024-06-14T10:32:41.000+00:00",
            "positionId": 1,
            "groupId": 1,
            "phone": "1234567890",
            "model": "Model 885",
            "contact": "contact@example.com",
            "category": "vehicle",
            "attributes": {}
        }

        location = {
            "id": 1849398,
            "attributes": {
                "status": "0100000000",
                "odometer": 276613,
                "rssi": 17,
                "sat": 17,
                "power": 12.6,
                "statusExtended": "20100",
                "adc1": 0.11,
                "distance": 0.0,
                "totalDistance": 0.0,
                "motion": False
            },
            "deviceId": 885,
            "protocol": "upro",
            "serverTime": "2024-06-14T10:32:41.000+00:00",
            "deviceTime": "2024-06-14T16:02:39.000+00:00",
            "fixTime": "2024-06-14T16:02:39.000+00:00",
            "outdated": False,
            "valid": True,
            "latitude": 11.216295,
            "longitude": 77.80542666666666,
            "altitude": 175.0,
            "speed": 0.0,
            "course": 60.0,
            "address": None,
            "accuracy": 0.0,
            "network": None,
            "geofenceIds": None
        }

        await insert_device_info(conn, device)
        await insert_location_info(conn, location)

    pool.close()
    await pool.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
