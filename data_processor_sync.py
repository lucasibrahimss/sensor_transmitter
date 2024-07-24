import paho.mqtt.client as mqtt
import json
from datetime import datetime, timedelta
import time

# Simulação de um banco de dados de séries temporais
class TimeSeriesDB:
    def __init__(self):
        self.data = {}

    def write(self, metric_path, value, timestamp):
        if metric_path not in self.data:
            self.data[metric_path] = []
        self.data[metric_path].append((timestamp, value))

    def read(self, metric_path):
        return self.data.get(metric_path, [])

# Inicialização do banco de dados de séries temporais
db = TimeSeriesDB()

# Alarme de inatividade - configuração
inactive_threshold = 10  # 10 vezes o intervalo de tempo esperado
sensor_last_seen = {}

# Função callback para quando o cliente recebe uma mensagem do broker
def on_message(client, userdata, msg):
    global sensor_last_seen
    topic = msg.topic
    payload = msg.payload.decode()

    if topic == "/sensor_monitors":
        try:
            data = json.loads(payload)
            machine_id = data['machine_id']
            sensors = data['sensors']
            for sensor in sensors:
                sensor_id = sensor['sensor_id']
                sensor_topic = f"/sensors/{machine_id}/{sensor_id}"
                client.subscribe(sensor_topic, qos=0)
                sensor_last_seen[f"{machine_id}.{sensor_id}"] = datetime.utcnow()
                print(f"Subscribed to {sensor_topic}")
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Failed to process /sensor_monitors message: {e}")
    else:
        try:
            # Dividir o tópico e verificar o número de partes
            parts = topic.split('/')
            if len(parts) != 4:
                raise ValueError(f"Unexpected topic format: {topic}")
            _, machine_id, sensor_id = parts[1:4]
        except ValueError:
            print(f"Unexpected topic format: {topic}")
            return

        try:
            data = json.loads(payload)
            timestamp = data['timestamp']
            value = data['value']

            # Persistir dados
            metric_path = f"{machine_id}.{sensor_id}"
            db.write(metric_path, value, timestamp)

            # Atualizar o timestamp de última atividade para o alarme de inatividade
            sensor_last_seen[metric_path] = datetime.utcnow()

            # Processar dados (por exemplo, média móvel)
            print(f"{machine_id}/{sensor_id}/{value}")
            process_data(machine_id, sensor_id, value)
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Failed to decode JSON payload or missing data: {e}")
        except Exception as e:
            print(f"Error processing message: {e}")

def process_data(machine_id, sensor_id, value):
    # Exemplo de processamento: cálculo de média móvel
    metric_path = f"{machine_id}.{sensor_id}"
    previous_values = db.read(f"{machine_id}.{sensor_id}")
    values = [v[1] for v in previous_values[-9:]] + [value]
    moving_average = sum(values) / len(values)
    db.write(metric_path, moving_average, datetime.utcnow())
    print(f"Processed data for {metric_path}: = {moving_average}")

def check_inactivity():
    global sensor_last_seen
    now = datetime.utcnow()
    for metric_path, last_seen in sensor_last_seen.items():
        # Verifica se os dados do sensor foram recebidos dentro do limite
        if now - last_seen > timedelta(seconds=inactive_threshold):
            alarm_path = f"{metric_path}.alarms.inactive"
            db.write(alarm_path, 1, now)
            print(f"Alarm triggered for {metric_path}: INACTIVE")

# Função para conectar ao broker MQTT e subscrever aos tópicos
def connect_and_subscribe():
    client = mqtt.Client()

    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            print("Connected to MQTT Broker!")
            client.subscribe("/sensor_monitors", qos=0)
        else:
            print(f"Failed to connect, return code {rc}")

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect("test.mosquitto.org", 1883, keepalive=60)
    client.loop_start()

    return client

if __name__ == "__main__":
    client = connect_and_subscribe()
    try:
        while True:
            check_inactivity()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Encerrando...")
    finally:
        client.loop_stop()
        client.disconnect()