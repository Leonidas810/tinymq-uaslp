import json
import time
import datetime
import threading
import traceback
import psycopg2
import psycopg2.extras
from tinymq_module import Client, print_message, MessageType
import signal
import sys

class StorageService:
    """
    Servicio de almacenamiento para TinyMQ.
    Recibe mensajes directamente del broker y los almacena en PostgreSQL.
    """
    
    def __init__(self):
        # Conexión a PostgreSQL
        try:
            print_message("Storage", "Conectando a PostgreSQL en localhost...", MessageType.INFO)
            self.conn = psycopg2.connect("dbname=tinymq user=postgres password=12345 host=localhost")
            print_message("Storage", "✅ Conexión exitosa a PostgreSQL", MessageType.SUCCESS)
            self._initialize_db()
        except Exception as e:
            print_message("Storage", f"❌ Error al conectar a PostgreSQL: {e}", MessageType.ERROR)
            raise
        
        # Cliente TinyMQ
        self.client = Client("storage_service")
        
        # Control
        self.running = False
        self.messages_stored = 0
    
    def _initialize_db(self):
        """Inicializa la tabla de mensajes"""
        try:
            with self.conn.cursor() as cur:
                # Crear tabla simple de mensajes
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS messages (
                        id SERIAL PRIMARY KEY,
                        topic TEXT NOT NULL,
                        message TEXT NOT NULL,
                        timestamp INTEGER NOT NULL,
                        readable_time TEXT NOT NULL
                    );
                """)
                
                # Índice básico
                cur.execute("CREATE INDEX IF NOT EXISTS idx_topic ON messages(topic);")
                
                self.conn.commit()
                print_message("Storage", "✅ Tabla de mensajes creada/verificada", MessageType.SUCCESS)
        except Exception as e:
            print_message("Storage", f"❌ Error al inicializar base de datos: {e}", MessageType.ERROR)
            raise
    
    def start(self):
        """Inicia el servicio de almacenamiento"""
        if self.running:
            return False
            
        print_message("Storage", "Iniciando servicio de almacenamiento...", MessageType.INFO)
        
        # Verificar conexión a PostgreSQL
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
                if cur.fetchone()[0] != 1:
                    raise Exception("Verificación de conexión fallida")
            print_message("Storage", "✅ Conexión a PostgreSQL verificada", MessageType.SUCCESS)
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
        
        # Cerrar conexión a PostgreSQL
        if self.conn:
            self.conn.close()
            
        print_message("Storage", "✅ Servicio detenido", MessageType.SUCCESS)
    
    def poll_loop(self):
        """Bucle simple para polling"""
        poll_counter = 0
        db_check_counter = 0
        
        while self.running:
            # Procesar mensajes recibidos
            self.client.poll()
            
            # Verificar conexión a PostgreSQL periódicamente (cada 5 segundos)
            db_check_counter += 1
            if db_check_counter >= 500:
                db_check_counter = 0
                try:
                    with self.conn.cursor() as cur:
                        cur.execute("SELECT 1")
                except Exception as e:
                    print_message("Storage", f"⚠️ Error de conexión a PostgreSQL: {e}", MessageType.WARNING)
                    try:
                        self.conn = psycopg2.connect("dbname=tinymq user=postgres password=12345 host=localhost")
                        print_message("Storage", "✅ Reconexión exitosa a PostgreSQL", MessageType.SUCCESS)
                    except Exception as e2:
                        print_message("Storage", f"❌ Error al reconectar: {e2}", MessageType.ERROR)
            
            # Mostrar señal de vida periódicamente (cada 30 segundos)
            # poll_counter += 1
            # if poll_counter >= 3000:
            #     poll_counter = 0
            #     print_message("Storage", 
            #                  f"📊 Servicio activo, mensajes almacenados: {self.messages_stored}", 
            #                  MessageType.INFO)
            
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

            # No procesar mensajes del sistema (empiezan con _)
            if original_topic.startswith("_"):
                return

            print_message(
                "Storage", 
                f"📥 Mensaje recibido del broker para '{original_topic}'", 
                MessageType.INFO
            )

            # Guardar en base de datos
            self.store_message(original_topic, original_message)

        except json.JSONDecodeError:
            print_message("Storage", "❌ Error: mensaje con formato JSON inválido", MessageType.ERROR)
        except Exception as e:
            print_message("Storage", f"❌ Error al procesar mensaje del broker: {e}", MessageType.ERROR)

    
    def store_message(self, topic, message_str):
        """Almacena un mensaje en PostgreSQL"""
        try:
            # Timestamp y tiempo legible
            timestamp = int(time.time())
            readable_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Insertar en PostgreSQL
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO messages (topic, message, timestamp, readable_time)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                """, (topic, message_str, timestamp, readable_time))
                
                # Obtener ID del mensaje insertado
                result = cur.fetchone()
                if not result:
                    print_message("Storage", "❌ Error: La inserción no devolvió un ID", MessageType.ERROR)
                    return False
                
                message_id = result[0]
                self.conn.commit()
                
                # Actualizar contador
                self.messages_stored += 1
                
                # Vista previa del mensaje
                message_preview = message_str[:30] + "..." if len(message_str) > 30 else message_str
                
                # Mostrar confirmación
                print_message("Storage", 
                            f"✅ Mensaje #{self.messages_stored} (ID:{message_id}) guardado en '{topic}': {message_preview}", 
                            MessageType.SUCCESS)
                
                return True
                
        except Exception as e:
            print_message("Storage", f"❌ Error al almacenar mensaje: {e}", MessageType.ERROR)
            print_message("Storage", traceback.format_exc(), MessageType.ERROR)
            
            # Intentar hacer rollback
            try:
                self.conn.rollback()
            except:
                pass
                
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
        topic = query_data.get("topic")
        limit = query_data.get("limit", 100)
        
        print_message("Storage", f"📊 Consulta de historial para tópico: {topic}", MessageType.INFO)
        
        try:
            messages = []
            
            # Consultar PostgreSQL
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                if topic:
                    # Consultar un tópico específico
                    query = """
                        SELECT topic, message, timestamp, readable_time
                        FROM messages 
                        WHERE topic = %s
                        ORDER BY timestamp DESC
                        LIMIT %s
                    """
                    cur.execute(query, (topic, limit))
                    
                    rows = cur.fetchall()
                    if rows:
                        messages = [dict(row) for row in rows]
                        print_message("Storage", f"✅ Se encontraron {len(messages)} mensajes para '{topic}'", 
                                    MessageType.SUCCESS)
                    else:
                        messages = []
                        print_message("Storage", f"⚠️ No se encontraron mensajes para '{topic}'", 
                                    MessageType.INFO)
                else:
                    # Obtener todos los tópicos disponibles
                    cur.execute("SELECT DISTINCT topic FROM messages")
                    topics = [row[0] for row in cur.fetchall()]
                    messages = {"topics": topics}
                    print_message("Storage", f"✅ Se encontraron {len(topics)} tópicos con mensajes", 
                                MessageType.SUCCESS)
            
            # Enviar respuesta
            self.send_response(client_id, {
                "status": "success",
                "topic": topic,
                "messages": messages
            })
            
        except Exception as e:
            print_message("Storage", f"❌ Error al consultar historial: {e}", MessageType.ERROR)
            
            self.send_response(client_id, {
                "status": "error",
                "message": str(e)
            })
    
    def handle_topics_request(self, client_id):
        """Maneja una solicitud de lista de tópicos disponibles"""
        try:
            with self.conn.cursor() as cur:
                # Obtener lista de tópicos únicos
                cur.execute("SELECT DISTINCT topic FROM messages")
                topics = [row[0] for row in cur.fetchall()]
                
                if topics:
                    print_message("Storage", f"✅ Se encontraron {len(topics)} tópicos disponibles", 
                                MessageType.SUCCESS)
                else:
                    print_message("Storage", "⚠️ No se encontraron tópicos con mensajes", 
                                MessageType.INFO)
                
                # Enviar respuesta
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
        print_message("Storage", f"Enviando respuesta desde storage service a '{client_id}'", MessageType.INFO) #!borrar

        """Envía una respuesta a un cliente específico"""
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
            
            # Mantener el proceso vivo
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