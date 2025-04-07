import tinymq_module

client = tinymq_module.Client("test_client2")
assert not client.is_connected()
    
client.connect()
client.subscribe("test_topic", lambda topic, message: print(f"Received on {topic}: {message}"))
client.publish("test_topic", "Si Felipe es gei")
input("Presiona Enter para desconectarse...")
client.disconnect()



