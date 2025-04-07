import tinymq_module

def on_message_received(topic, message):
    message_bytes = bytes(message)
    print(f"Mensaje recibido en el tema '{topic}': {message_bytes.decode('utf-8')}")

def test_client():
    client = tinymq_module.Client("test_client2")
    assert not client.is_connected()
    
    client.connect()
    client.subscribe("test_topic", on_message_received)
    client.publish("test_topic", "Me he suscrito a este tema")

    input("Presiona Enter para desconectarse...")
    client.disconnect()

if __name__ == "__main__":
    test_client()

