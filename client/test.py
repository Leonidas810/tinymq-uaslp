import tinymq_module

def test_client():
    client = tinymq_module.Client("test_client")
    assert not client.is_connected()
    
    client.connect()
    client.subscribe("test_topic", lambda topic, message: print(f"Received on {topic}: {message}"))
    client.publish("test_topic", "Felipe es gei?")
    input("Presiona Enter para desconectarse...")
    client.disconnect()

if __name__ == "__main__":
    test_client()

