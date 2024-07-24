import asyncio
import json
from datetime import datetime, timedelta
from dateutil import parser
import pytz
import paho.mqtt.client as mqtt

# Fuso horário de Brasília
tz = pytz.timezone('America/Sao_Paulo')

# Banco de dados de séries temporais simulado
class TimeSeriesDB:
    def __init__(self):
        self.data = {}

    async def write(self, metric_path, value, timestamp):
        if metric_path not in self.data:
            self.data[metric_path] = []
        self.data[metric_path].append((timestamp, value))
        print(f"Persisted data at {metric_path}: {value} at {timestamp}")

    async def read(self, metric_path):
        return self.data.get(metric_path, [])

db = TimeSeriesDB()
inactive_threshold = 10  # Limite para alarme de inatividade
sensor_last_seen = {}

new_loop = asyncio.new_event_loop()
asyncio.set_event_loop(new_loop)

def on_message(client, userdata, msg):
    asyncio.run_coroutine_threadsafe(handle_message(client, msg), new_loop)

async def handle_message(client, msg):
    global sensor_last_seen
    topic = msg.topic
    payload = msg.payload.decode()
    print(f"[{datetime.now(tz)}] Message received on topic {topic}: {payload}")

    if topic == "/sensor_monitors":
        try:
            data = json.loads(payload)
            machine_id = data['machine_id']
            sensors = data['sensors']
            for sensor in sensors:
                sensor_id = sensor['sensor_id']
                sensor_topic = f"/sensors/{machine_id}/{sensor_id}"
                client.subscribe(sensor_topic, qos=0)
                sensor_last_seen[f"{machine_id}.{sensor_id}"] = datetime.now(tz)
                print(f"[{datetime.now(tz)}] Subscribed to {sensor_topic}")
        except (json.JSONDecodeError, KeyError) as e:
            print(f"[{datetime.now(tz)}] Failed to process /sensor_monitors message: {e}")
    else:
        try:
            parts = topic.split('/')
            if len(parts) != 4:
                raise ValueError(f"Unexpected topic format: {topic}")
            _, machine_id, sensor_id = parts[1:4]
        except ValueError:
            print(f"[{datetime.now(tz)}] Unexpected topic format: {topic}")
            return

        try:
            data = json.loads(payload)
            timestamp = data['timestamp']
            value = data['value']

            # Corrigir timestamp com 'Z' e offset de fuso horário
            if 'Z' in timestamp:
                timestamp = timestamp.replace('Z', '')
            timestamp_brasilia = parser.isoparse(timestamp).astimezone(tz)

            metric_path = f"{machine_id}.{sensor_id}"
            await db.write(metric_path, value, timestamp_brasilia)
            print(f"[{datetime.now(tz)}] Data written for {metric_path}")

            sensor_last_seen[metric_path] = datetime.now(tz)
            await process_data(machine_id, sensor_id, value)
        except (json.JSONDecodeError, KeyError) as e:
            print(f"[{datetime.now(tz)}] Failed to decode JSON payload or missing data: {e}")
        except Exception as e:
            print(f"[{datetime.now(tz)}] Error processing message: {e}")

async def process_data(machine_id, sensor_id, value):
    metric_path = f"{machine_id}.{sensor_id}"
    previous_values = await db.read(metric_path)
    if previous_values:
        values = [v[1] for v in previous_values[-9:]] + [value]
        moving_average = sum(values) / len(values)
        await db.write(metric_path, moving_average, datetime.now(tz))
        print(f"[{datetime.now(tz)}] Processed data for {metric_path}: {moving_average}")

async def check_inactivity():
    global sensor_last_seen
    while True:
        now = datetime.now(tz)
        for metric_path, last_seen in sensor_last_seen.items():
            if last_seen.tzinfo is None:
                last_seen = tz.localize(last_seen)
            if now - last_seen > timedelta(seconds=inactive_threshold):
                alarm_path = f"{metric_path}.alarms.inactive"
                await db.write(alarm_path, 1, now)
                print(f"[{datetime.now(tz)}] Alarm triggered for {metric_path}: INACTIVE")
        await asyncio.sleep(1)

async def connect_and_subscribe():
    client = mqtt.Client()

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print(f"[{datetime.now(tz)}] Connected to MQTT Broker!")
            client.subscribe("/sensor_monitors", qos=0)
            print(f"[{datetime.now(tz)}] Subscribed to /sensor_monitors")
        else:
            print(f"[{datetime.now(tz)}] Failed to connect, return code {rc}")

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect("test.mosquitto.org", 1883, keepalive=30)
    client.loop_start()

    return client

async def main():
    client = await connect_and_subscribe()

    try:
        asyncio.create_task(check_inactivity())
        await asyncio.gather(*asyncio.all_tasks())
    except KeyboardInterrupt:
        print(f"[{datetime.now(tz)}] Encerrando...")
    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    new_loop.run_until_complete(main())