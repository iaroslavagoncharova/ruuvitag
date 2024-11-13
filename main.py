import asyncio
from bleak import BleakScanner
from ruuvitag_sensor.decoder import get_decoder
from pymongo import MongoClient
from datetime import datetime, timezone, timedelta
import schedule

# List to hold the RuuviTag data temporarily
ruuvi_data_list = []

# MongoDB connection setup
def get_db_connection():
    try:
        client = MongoClient("mongodb+srv://slava2:Kirpichik4@cluster0.f8iqe.mongodb.net/devicehub?retryWrites=true&w=majority&appName=Cluster0")
        db = client["devicehub"]
        return db
    except Exception as e:
        print("Error connecting to MongoDB:", e)
        return None

# Insert data into MongoDB
def insert_ruuvitag_data(data):
    db = get_db_connection()
    if db is None:
        print("Error connecting to MongoDB.")
        return
    
    try:
        ruuvi_collection = db["ruuvis"]
        data_with_timestamp = {**data, "timestamp": datetime.now(timezone.utc)}
        ruuvi_collection.insert_one(data_with_timestamp)
        print("Data inserted successfully into MongoDB.")
    except Exception as e:
        print("Error inserting data into MongoDB:", e)

# Update device data in MongoDB
def perform_device_data_update(data):
    db = get_db_connection()
    if db is None:
        return

    data_without_mac = {k: v for k, v in data["data"].items() if k != "mac"}

    try:
        devices_collection = db["devices"]
        # Update only the `data` field for the document where `name` is "RuuviTag"
        devices_collection.update_one(
            {"name": "RuuviTag"},
            {"$set": {"data": data_without_mac, "last_updated": datetime.now(timezone.utc)}}
        )
        print("Data updated successfully in MongoDB 'devices' collection.")
    except Exception as e:
        print("Error updating data in MongoDB 'devices' collection:", e)

# Decode RuuviTag data
def parse_ruuvi_data(data):
    hex_data = data.hex()
    decoder = get_decoder(hex_data)
    if decoder:
        return decoder.decode_data(hex_data)
    return None

# Callback to handle detected devices
def detection_callback(device, advertisement_data):
    if device.name == "Ruuvi 524A":
        print(f"Found RuuviTag: {device.address}")
        manufacturer_data = advertisement_data.manufacturer_data
        if manufacturer_data:
            for ad_type, data in manufacturer_data.items():
                sensor_data = parse_ruuvi_data(data)
                if sensor_data:
                    # Extract only the required fields (temperature, humidity, pressure, and mac)
                    ruuvi_data = {
                        "data": {
                            "humidity": sensor_data.get("humidity"),
                            "temperature": sensor_data.get("temperature"),
                            "pressure": sensor_data.get("pressure"),
                            "mac": device.address
                        }
                    }
                    # Add the extracted data to the list
                    ruuvi_data_list.append(ruuvi_data)
                    print(f"Parsed sensor data: {ruuvi_data}")
        else:
            print("No manufacturer data found for RuuviTag.")

# Insert data to ruuvis
def collect_and_insert_data():
    if ruuvi_data_list:
        for data in ruuvi_data_list.copy():
            insert_ruuvitag_data(data)
        ruuvi_data_list.clear()

# Update the device data in devices
def update_device_data():
    if ruuvi_data_list:
        for data in ruuvi_data_list.copy():
            perform_device_data_update(data)
        ruuvi_data_list.clear()

# BLE scanning function
async def continuous_scan():
    scanner = BleakScanner(detection_callback=detection_callback)
    print("Starting continuous scanning for RuuviTag data...")
    await scanner.start()
    try:
        while True:
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        await scanner.stop()

# Set up scheduling for data collection
schedule.every(10).seconds.do(update_device_data)
schedule.every(1).minutes.do(collect_and_insert_data)

# Main function to run BLE scanning and scheduling
async def main():
    asyncio.create_task(continuous_scan())
    
    while True:
        schedule.run_pending()
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
