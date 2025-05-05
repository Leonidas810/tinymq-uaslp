import json
import time
import datetime
import threading
import traceback
import signal
import sys
from tinymq_module import Client, print_message, MessageType
from supabase import create_client

class StorageService:
    """
    Servicio de almacenamiento para TinyMQ.
    Recibe mensajes directamente del broker y los almacena en Supabase.
    """

    def __init__(self):
        # Conexión a Supabase
        try:
            print_message("Storage", "Conectando a Supabase en nube...", MessageType.INFO)
            # Credenciales de Supabase
            supabase_url = "https://jackjknfpmxpkxqeaisr.supabase.co"
            supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImphY2tqa25mcG14cGt4cWVhaXNyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDYxNjU2MzIsImV4cCI6MjA2MTc0MTYzMn0.KeOWc-05ajBQgnRy-LkBagKKPKitWsh8yUhq4qZ33Ng" 
            self.supabase = create_client(supabase_url, supabase_key)
            print_message("Storage", "✅ Conexión exitosa a Supabase", MessageType.SUCCESS)
            self._initialize_db()
        except Exception as e:
            print_message("Storage", f"❌ Error al conectar a Supabase: {e}", MessageType.ERROR)
            raise

        # Cliente TinyMQ
        self.client = Client("storage_service")

        # Control
        self.running = False
        self.messages_stored = 0

    def _initialize_db(self):
        """Inicializa las tablas de usuarios, tópicos, suscripciones y mensajes"""
        try:
            # Verificamos que las tablas existan consultando una entrada
            
            # Verificamos si podemos acceder a las tablas
            self.supabase.table("users").select("id").limit(1).execute()
            self.supabase.table("topics").select("id").limit(1).execute()
            self.supabase.table("subscriptions").select("user_id").limit(1).execute()
            self.supabase.table("messages").select("id").limit(1).execute()
            
            print_message("Storage", "✅ Tablas verificadas", MessageType.SUCCESS)
        except Exception as e:
            print_message("Storage", f"❌ Error al verificar tablas: {e}", MessageType.ERROR)
            print_message("Storage", "ℹ️ Asegúrate de haber creado las tablas en Supabase Studio", MessageType.INFO)
            raise

    def start(self):
        """Inicia el servicio de almacenamiento"""
        if self.running:
            return False

        print_message("Storage", "Iniciando servicio de almacenamiento...", MessageType.INFO)

        # Verificar conexión a Supabase
        try:
            # Una simple consulta para verificar que la conexión funciona
            result = self.supabase.table("users").select("id").limit(1).execute()
            print_message("Storage", "✅ Conexión a Supabase verificada", MessageType.SUCCESS)
        except Exception as e:
            print_message("Storage", f"❌ Error al verificar conexión: {e}", MessageType.ERROR)
            return False

        # Conectar al broker
        print_message("Storage", "Conectando al broker TinyMQ...", MessageType.INFO)
        if not self.client.connect():
            print_message("Storage", "❌ Error al conectar al broker", MessageType.ERROR)
            return False

        print_message("Storage", "✅ Conectado al broker TinyMQ", MessageType.SUCCESS)
        self.running = True

        # Suscribirse ÚNICAMENTE al tópico especial donde el broker enviará mensajes para almacenamiento
        print_message("Storage", "Suscribiendo al tópico de almacenamiento...", MessageType.INFO)
        if self.client.subscribe("_broker/storage", self.on_broker_message):
            print_message("Storage", "✅ Suscripción exitosa al tópico de almacenamiento", MessageType.SUCCESS)
        else:
            print_message("Storage", "❌ Error al suscribirse", MessageType.ERROR)
            self.client.disconnect()
            self.running = False
            return False

        # Suscribirse al tópico para consultas de historial 
        if self.client.subscribe("_storage/query/#", self.on_query):
            print_message("Storage", "✅ Suscripción exitosa al tópico de consultas", MessageType.SUCCESS)
        else:
            print_message("Storage", "❌ Error al suscribirse a consultas", MessageType.ERROR)

        # Iniciar thread para polling
        self.poll_thread = threading.Thread(target=self.poll_loop)
        self.poll_thread.daemon = True
        self.poll_thread.start()

        # Notificar al broker que el servicio de almacenamiento está listo
        self.client.publish("_storage/status", json.dumps({
            "status": "active",
            "timestamp": time.time()
        }))

        print_message("Storage", "✅ Servicio de almacenamiento iniciado", MessageType.SUCCESS)
        return True

    def stop(self):
        """Detiene el servicio"""
        if not self.running:
            return

        self.running = False

        # Notificar al broker que el servicio se está deteniendo
        try:
            self.client.publish("_storage/status", json.dumps({
                "status": "stopping",
                "timestamp": time.time()
            }))
        except:
            pass

        # Desconectar del broker
        self.client.disconnect()

        print_message("Storage", "✅ Servicio detenido", MessageType.SUCCESS)

    def poll_loop(self):
        """Bucle simple para polling"""
        db_check_counter = 0

        while self.running:
            # Procesar mensajes recibidos
            self.client.poll()

            # Verificar conexión a Supabase periódicamente (cada 5 segundos)
            db_check_counter += 1
            if db_check_counter >= 500:
                db_check_counter = 0
                try:
                    self.supabase.table("users").select("id").limit(1).execute()
                except Exception as e:
                    print_message("Storage", f"⚠️ Error de conexión a Supabase: {e}", MessageType.WARNING)
            
            time.sleep(0.01)  # Reducir uso de CPU

    def on_broker_message(self, topic, message):
        print_message("Storage", f"📥 Mensaje recibido del broker para '{topic}'", MessageType.INFO)

        try:
            # Asegurarse que message es bytes
            if isinstance(message, list):
                message = bytes(message)
            elif isinstance(message, str):
                message = message.encode('utf-8')

            # Decodificar el mensaje
            decoded_message = message.decode('utf-8')

            # Cargar JSON
            message_data = json.loads(decoded_message)

            original_topic = message_data.get("topic")
            original_message = message_data.get("message")
            sender_username = message_data.get("sender", "anonymous")

            # No procesar mensajes del sistema (empiezan con _)
            if original_topic.startswith("_"):
                return

            print_message(
                "Storage", 
                f"📥 Mensaje recibido del broker para '{original_topic}'", 
                MessageType.INFO
            )

            # Guardar en base de datos
            self.store_message(original_topic, original_message, sender_username)

        except json.JSONDecodeError:
            print_message("Storage", "❌ Error: mensaje con formato JSON inválido", MessageType.ERROR)
        except Exception as e:
            print_message("Storage", f"❌ Error al procesar mensaje del broker: {e}", MessageType.ERROR)

    def store_message(self, topic, message_str, sender_username="anonymous"):
        """Almacena un mensaje en Supabase usando el nuevo esquema"""
        try:
            # Buscar o crear usuario
            user_result = self.supabase.table("users").select("id").eq("username", sender_username).execute()
            
            if user_result.data and len(user_result.data) > 0:
                sender_id = user_result.data[0]['id']
            else:
                # Crear usuario
                user_data = {"username": sender_username, "password_hash": ""}
                new_user = self.supabase.table("users").insert(user_data).execute()
                sender_id = new_user.data[0]['id']

            # Buscar o crear tópico
            topic_result = self.supabase.table("topics").select("id").eq("name", topic).execute()
            
            if topic_result.data and len(topic_result.data) > 0:
                topic_id = topic_result.data[0]['id']
            else:
                # Crear tópico
                topic_data = {"name": topic, "created_by": sender_id}
                new_topic = self.supabase.table("topics").insert(topic_data).execute()
                topic_id = new_topic.data[0]['id']

            # Insertar mensaje
            message_data = {
                "topic_id": topic_id,
                "sender_id": sender_id,
                "content": message_str
            }
            
            new_message = self.supabase.table("messages").insert(message_data).execute()
            message_id = new_message.data[0]['id']
            
            self.messages_stored += 1

            message_preview = message_str[:30] + "..." if len(message_str) > 30 else message_str
            print_message("Storage", 
                f"✅ Mensaje #{self.messages_stored} (ID:{message_id}) guardado en '{topic}': {message_preview}", 
                MessageType.SUCCESS)
            return True

        except Exception as e:
            print_message("Storage", f"❌ Error al almacenar mensaje: {e}", MessageType.ERROR)
            print_message("Storage", traceback.format_exc(), MessageType.ERROR)
            return False

    def on_query(self, topic, message):
        print_message("Storage", f"on_query ejecutado para topic: {topic}", MessageType.INFO)
        try:
            if not topic.startswith("_storage/query/"):
                return
            client_id = topic[len("_storage/query/"):]
            # Convierte la lista a bytes si es necesario
            if isinstance(message, list):
                message = bytes(message)
            query_data = json.loads(message.decode('utf-8'))
            query_data["topic"] = client_id
            action = query_data.get("action")
            if action == "query":
                self.handle_query(client_id, query_data)
            elif action == "topics":
                self.handle_topics_request(client_id)
            else:
                print_message("Storage", f"⚠️ Acción desconocida: {action}", MessageType.WARNING)
        except json.JSONDecodeError:
            print_message("Storage", "❌ Solicitud con formato JSON inválido", MessageType.ERROR)
        except Exception as e:
            print_message("Storage", f"❌ Error al procesar consulta: {e}", MessageType.ERROR)
            print_message("Storage", traceback.format_exc(), MessageType.ERROR)

    def handle_query(self, client_id, query_data):
        """Maneja una consulta de historial de mensajes"""
        topic_name = query_data.get("topic")
        limit = query_data.get("limit", 100)
        # Nuevo: campos solicitados por el usuario
        fields = query_data.get("fields", None)

        print_message("Storage", f"📊 Consulta de historial para tópico: {topic_name}", MessageType.INFO)

        try:
            # Obtener el ID del tópico
            topic_result = self.supabase.table("topics").select("id").eq("name", topic_name).execute()
            if not topic_result.data or len(topic_result.data) == 0:
                print_message("Storage", f"⚠️ No se encontró el tópico '{topic_name}'", MessageType.INFO)
                self.send_response(client_id, {
                    "status": "success",
                    "topic": topic_name,
                    "messages": []
                })
                return

            topic_id = topic_result.data[0]['id']

            # Determinar los campos a seleccionar
            if fields and isinstance(fields, list) and len(fields) > 0:
                select_fields = []
                for f in fields:
                    if f == "sender_id":
                        select_fields.append("sender_id")
                    elif f == "content":
                        select_fields.append("content")
                    elif f == "sent_at":
                        select_fields.append("sent_at")
                    elif f == "sender_username":
                        select_fields.append("users(username)")
                    elif f == "topic_name":
                        select_fields.append("topics(name)")
                    # Puedes agregar más campos personalizados aquí
                select_str = ",".join(select_fields)
            else:
                # Por defecto
                select_str = "content,sender_id,sent_at"

            # Consulta a Supabase
            query = self.supabase.table("messages").select(select_str).eq("topic_id", topic_id).order("sent_at", desc=True).limit(limit).execute()

            messages = []
            if query.data:
                for row in query.data:
                    msg = {}
                    # Solo incluir los campos solicitados
                    for key in row:
                        msg[key] = row[key]
                    # Si el usuario pidió sender_username o topic_name
                    if "users" in row and isinstance(row["users"], dict):
                        msg["sender_username"] = row["users"].get("username")
                    if "topics" in row and isinstance(row["topics"], dict):
                        msg["topic_name"] = row["topics"].get("name")
                    messages.append(msg)

                print_message("Storage", f"✅ Se encontraron {len(messages)} mensajes para '{topic_name}'", MessageType.SUCCESS)
            else:
                print_message("Storage", f"⚠️ No se encontraron mensajes para '{topic_name}'", MessageType.INFO)

            self.send_response(client_id, {
                "status": "success",
                "topic": topic_name,
                "messages": messages
            })

        except Exception as e:
            print_message("Storage", f"❌ Error al consultar historial: {e}", MessageType.ERROR)
            self.send_response(client_id, {
                "status": "error",
                "message": str(e)
            })
    # ...existing code...

    def handle_topics_request(self, client_id):
        """Maneja una solicitud de lista de tópicos disponibles"""
        try:
            topics_result = self.supabase.table("topics").select("name").execute()
            
            topics = []
            if topics_result.data:
                topics = [row["name"] for row in topics_result.data]
            
            if topics:
                print_message("Storage", f"✅ Se encontraron {len(topics)} tópicos disponibles", MessageType.SUCCESS)
            else:
                print_message("Storage", "⚠️ No se encontraron tópicos con mensajes", MessageType.INFO)
            
            self.send_response(client_id, {
                "status": "success",
                "topics": topics
            })
        except Exception as e:
            print_message("Storage", f"❌ Error al obtener tópicos: {e}", MessageType.ERROR)
            self.send_response(client_id, {
                "status": "error",
                "message": str(e)
            })

    def send_response(self, client_id, response_data):
        print_message("Storage", f"Enviando respuesta desde storage service a '{client_id}'", MessageType.INFO)
        response_topic = f"_storage/response/{client_id}"
        response_json = json.dumps(response_data)
        success = self.client.publish(response_topic, response_json)
        if success:
            print_message("Storage", f"✅ Respuesta enviada a '{response_topic}'", MessageType.SUCCESS)
        else:
            print_message("Storage", f"❌ Error al enviar respuesta a '{response_topic}'", MessageType.ERROR)

# Controlador principal
def signal_handler(sig, frame):
    print_message("Storage", "⚠️ Señal de interrupción recibida", MessageType.WARNING)
    if service:
        service.stop()
    sys.exit(0)

if __name__ == "__main__":
    # Registrar manejador de señales
    signal.signal(signal.SIGINT, signal_handler)
    service = None

    try:
        # Iniciar servicio
        service = StorageService()
        if service.start():
            print_message("Storage", "✅ Servicio iniciado. Presiona Ctrl+C para detener.", MessageType.INFO)
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        if service:
            service.stop()
    except Exception as e:
        print_message("Storage", f"❌ Error fatal: {e}", MessageType.ERROR)
        if service:
            service.stop()
        sys.exit(1)