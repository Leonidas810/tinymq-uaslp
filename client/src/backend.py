# TinyMQ Client Example
import tinymq_module

client_id = "Kris"
topic_to_subscribe = "test_topic"

client = tinymq_module.Client(client_id, "localhost", 1505)

if client.connect():
    print(f"[TinyMQ] Conectado al broker como '{client_id}'")
else:
    print("[TinyMQ] Error al conectar al broker")
    exit(1)

def on_message(topic, message):
    message_bytes = bytes(message)
    print(f"[TinyMQ] Mensaje recibido en '{topic}': {message_bytes.decode('utf-8')}")

client.subscribe(topic_to_subscribe, on_message)

try:
    while True:
        client.poll()
except KeyboardInterrupt:
    print("[TinyMQ] Cerrando conexión...")
    client.disconnect()