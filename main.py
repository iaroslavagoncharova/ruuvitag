import asyncio
from bleak import BleakScanner
from ruuvitag_sensor.decoder import get_decoder
import requests
import json
from datetime import datetime, timezone, timedelta
import schedule

# List to hold the RuuviTag data temporarily
ruuvi_data_list = []

NODE_SERVER_URL = "http://localhost:3000"

# Send data to the Node.js server
def send_data_to_node_server(data):
    try:
        url = NODE_SERVER_URL + "/api/v1/ruuvi"
        headers = {"Content-Type": "application/json"}
        data_with_timestamp = {**data, "timestamp": datetime.now(timezone.utc).isoformat()}
        response = requests.post(url, headers=headers, data=json.dumps(data_with_timestamp))
        response.raise_for_status()
        print("Data sent successfully to Node.js server.")
    except requests.exceptions.RequestException as e:
        print("Error sending data to Node.js server:", e)

# Update device data in MongoDB
def perform_device_data_update(data):
    # Remove the `mac` field from the `data` object
    data_without_mac = {k: v for k, v in data["data"].items() if k != "mac"}

    url = NODE_SERVER_URL + "/api/v1/devices"

    try:
        # Get the list of devices from the Node.js server
        response = requests.get(url)
        response.raise_for_status()
        devices = response.json()

        # Find the relevant device
        for device in devices:
            if device["name"] == "RuuviTag":
                device_id = device["_id"]
                break
        else:
            print("Device not found in MongoDB 'devices' collection.")
            return
        
        # Update the device's `data` field
        url = NODE_SERVER_URL + f"/api/v1/devices/recent/{device_id}"
        payload = {
            "data": data_without_mac,
        }
        response = requests.put(
            url,
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload)
        )
        response.raise_for_status()
        
        print("Data updated successfully in MongoDB 'devices' collection.")
    except requests.exceptions.RequestException as e:
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
            # insert_ruuvitag_data(data)
            send_data_to_node_server(data)
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
schedule.every(10).minutes.do(collect_and_insert_data)

# Main function to run BLE scanning and scheduling
async def main():
    asyncio.create_task(continuous_scan())
    
    while True:
        schedule.run_pending()
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
