import time
import json
import random
import uuid
import threading
import paho.mqtt.client as mqtt
from datetime import datetime
import configparser
import argparse

# Função para simular a leitura dos sensores
def get_sensor_data(sensor_type):
    if sensor_type == "accelerometer":
        return {
            "x": round(random.uniform(-10.0, 10.0), 2),
            "y": round(random.uniform(-10.0, 10.0), 2),
            "z": round(random.uniform(-10.0, 10.0), 2)
        }
    elif sensor_type == "gyroscope":
        return {
            "x": round(random.uniform(-500.0, 500.0), 2),
            "y": round(random.uniform(-500.0, 500.0), 2),
            "z": round(random.uniform(-500.0, 500.0), 2)
        }

# Função para conectar ao broker MQTT
def connect_mqtt():
    client = mqtt.Client()

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print(f"Failed to connect, return code {rc}")

    client.on_connect = on_connect
    client.connect("test.mosquitto.org", 1883, 60)  # Substitua pelo endereço do seu broker MQTT
    return client

# Função para publicar os dados dos sensores
def publish_sensor_data(client, machine_id, sensor_id, sensor_type, interval):
    while True:
        sensor_data = get_sensor_data(sensor_type)
        timestamp = datetime.utcnow().isoformat() + "Z"
        message = {
            "timestamp": timestamp,
            "value": sensor_data
        }
        client.publish(f"/sensors/{machine_id}/{sensor_id}", json.dumps(message))
        print(f"Published {sensor_type} data: {message}")
        time.sleep(interval)

# Função para publicar a mensagem inicial
def publish_initial_message(client, machine_id, sensors, initial_message_interval):
    while True:
        message = {
            "machine_id": machine_id,
            "sensors": sensors
        }
        client.publish("/sensor_monitors", json.dumps(message))
        print(f"Published initial message: {message}")
        time.sleep(initial_message_interval)

# Função para carregar configurações de um arquivo
def load_config(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

# Função principal
def main():
    parser = argparse.ArgumentParser(description='DataCollector Configuration')
    parser.add_argument('--config', type=str, default='config.ini', help='Path to configuration file')
    args = parser.parse_args()

    config = load_config(args.config)
    machine_id = str(uuid.uuid4())
    initial_message_interval = config.getint('DEFAULT', 'InitialMessageInterval', fallback=60)

    sensors = []
    client = connect_mqtt()
    client.loop_start()

    for section in config.sections():
        sensor_id = section
        sensor_type = config[section]['SensorType']
        interval = 0.5  # Ajusta o intervalo para 0.5 segundos
        sensors.append({
            "sensor_id": sensor_id,
            "data_type": sensor_type,
            "data_interval": interval
        })
        threading.Thread(target=publish_sensor_data, args=(client, machine_id, sensor_id, sensor_type, interval)).start()

    threading.Thread(target=publish_initial_message, args=(client, machine_id, sensors, initial_message_interval)).start()

if __name__ == "__main__":
    main()