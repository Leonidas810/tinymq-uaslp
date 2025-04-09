import tinymq_module

# Crea una instancia del Broker
broker = tinymq_module.Broker(port=1505, thread_pool_size=4)

print("Iniciando el broker...")
broker.start()

input("Presiona Enter para detener el broker...")

broker.stop()
print("Broker detenido.")
