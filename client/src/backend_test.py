from flask import Flask, render_template
from flask_cors import CORS
from flask_socketio import SocketIO
from tinymq_module import Client
import threading

app = Flask(__name__)

# Habilitar CORS para todos los orígenes
CORS(app)

# Crear la instancia de SocketIO permitiendo conexiones de todos los orígenes
socketio = SocketIO(app, cors_allowed_origins="*") 

client = None

# Lista para almacenar los tópicos y los últimos mensajes
sensor_data = {}

# Función para crear un cliente y conectar al broker
def create_client_and_connect():
    global client
    client_id = "Casa Inteligente"
    client = Client(client_id)
    
    if client.connect():
        print(f"Cliente '{client_id}' conectado al broker.")
        return client
    else:
        print("Error al conectar al broker.")
        return None

# Función para manejar los mensajes de TinyMQ (sincrónica)
def on_message(topic, message):
    message_bytes = bytes(message)
    content = message_bytes.decode('utf-8')
    sensor_data[topic] = content
    socketio.emit('sensor_update', {topic: content})

# Función para suscribirse a los tópicos
def subscribe_to_topics():
    topics = []
    while True:
        topic = input("Introduce un tópico para suscribirte (o 'fin' para terminar): ")
        if topic.lower() == 'fin':
            break
        topics.append(topic)

    for topic in topics:
        client.subscribe(topic, on_message)
        print(f"Cliente suscrito a '{topic}'.")

    threading.Thread(target=socketio.run, args=(app,), kwargs={'host': '0.0.0.0', 'port': 5000}).start()

# Ruta principal para renderizar la interfaz web
@app.route('/')
def index():
    return render_template('index.html')

# Función principal para inicializar el backend
def main():
    global client
    client = create_client_and_connect()

    if client:
        subscribe_to_topics()

if __name__ == "__main__":
    main()
