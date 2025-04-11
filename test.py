import sys
import os

# Agrega la ruta del módulo al PYTHONPATH
client_build_path = "/home/leonardo/tinymq-uaslp/build"
if client_build_path not in sys.path:
    sys.path.append(client_build_path)

import tinymq_module as broker

# Preguntar por el puerto y los threads, con valores por defecto
def input_con_default(prompt, default):
    user_input = input(f"{prompt} (presiona Enter para usar {default}): ").strip()
    return int(user_input) if user_input.isdigit() else default

puerto = input_con_default("Ingresa el puerto", 1505)
threads = input_con_default("Ingresa el número de hilos", 4)

# Crear la instancia del broker
broker_instance = broker.Broker(port=puerto, thread_pool_size=threads)

print(f"Iniciando el broker en el puerto {puerto} con {threads} hilos...")
broker_instance.start()

input("Presiona Enter para detener el broker...")

broker_instance.stop()
print("Broker detenido.")
