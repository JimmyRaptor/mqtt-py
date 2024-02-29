import os
from dotenv import load_dotenv
import logging
import paho.mqtt.client as mqtt
import cbor2
import requests
from datetime import datetime

headers = {"Content-Type": "application/json"}
count = 9
# configure logging
logging.basicConfig(
    filename="countMessage.log",
    filemode="a",
    format="%(asctime)s - %(message)s",
    level=logging.INFO,
)

# Load environment variables
load_dotenv()

# Configuration
mqtt_url = os.getenv("MQTT_URL")
mqtt_username = os.getenv("MQTT_USERNAME")
mqtt_password = os.getenv("MQTT_PASSWORD")
post_url = os.getenv("URL")  # 定义目标 URL


# MQTT callbacks
def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT Broker")
    client.subscribe("/pk/telemetry/#")


def on_message(client, userdata, msg):
    global count
    try:
        data = cbor2.loads(msg.payload)
        data = dict(data)
        data_id = msg.topic.split("/")[3]
        data["id"] = data_id
        response = requests.post(post_url, json=data, headers=headers)
        count += 1
        print(datetime.now(), count)
        print("Data sent with response:", response.status_code)

    except Exception as err:
        print(f"CBOR parsing error: {err}")


# Connect to the MQTT broker
def connect_and_subscribe_to_mqtt():
    client = mqtt.Client()
    client.username_pw_set(username=mqtt_username, password=mqtt_password)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(mqtt_url, 1883, 60)
    client.loop_forever()


if __name__ == "__main__":
    connect_and_subscribe_to_mqtt()
