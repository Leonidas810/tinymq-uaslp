import json
import time
import threading
import sys
from tinymq_module import Client, print_message, MessageType

class StorageClient:
    """
    Cliente para acceder al servicio de almacenamiento.
    Esta clase envuelve al cliente TinyMQ y proporciona métodos para acceder al historial.
    """
    
    def __init__(self, client=None):
        """
        Inicializa el cliente de almacenamiento
        
        Args:
            client: Un cliente TinyMQ existente, o None para crear uno nuevo
        """
        # Tópicos especiales - definir antes de cualquier uso
        self.STORAGE_QUERY_PREFIX = "_storage/query/" 
        self.STORAGE_RESPONSE_PREFIX = "_storage/response/"
        self.STORAGE_STATUS = "_storage/status"
        
        self.client = client
        self._own_client = (client is None)
        
        if self._own_client:
            print_message("StorageClient", "Creando nuevo cliente TinyMQ", MessageType.INFO)
            self.client = Client()
            self.client.connect()
        else:
            print_message("StorageClient", "Usando cliente TinyMQ existente", MessageType.INFO)
        
        # Inicializar estructuras de datos antes de configurar handlers
        self._pending_requests = {}
        self._response_events = {}
        self._responses = {}
        
        # Verificar conexión
        if not self.client.is_connected():
            print_message("StorageClient", "⚠️ El cliente TinyMQ no está conectado", MessageType.WARNING)
        
        # Configurar los handlers después de inicializar todo
        self._setup_response_handlers()
    
    def _setup_response_handlers(self):
        if not self.client.is_connected():
            print_message("StorageClient", "❌ No se pudo configurar manejador: cliente no conectado", MessageType.ERROR)
            return False

        try:
            # Suscribirse al wildcard de respuestas
            print_message("StorageClient", "Suscribiendo al tópico de respuesta...", MessageType.INFO)
            result = self.client.subscribe(self.STORAGE_RESPONSE_PREFIX + "#", self._on_response)
            if result:
                print_message("StorageClient", "✅ Suscripción exitosa al tópico de respuesta", MessageType.SUCCESS)
            else:
                print_message("StorageClient", "❌ Falló la suscripción al tópico de respuesta", MessageType.ERROR)
            return result
        except Exception as e:
            print_message("StorageClient", f"❌ Error al configurar manejador: {e}", MessageType.ERROR)
            import traceback
            traceback.print_exc()
            return False
    
    def _on_response(self, topic, message):
        """Recibe las respuestas del servicio de almacenamiento"""
        try:
            print_message("StorageClient", f"[DEBUG] _on_response llamado con topic={topic}, message={message}", MessageType.INFO)
            # Extraer el ID del cliente/request desde el tópico de respuesta
            request_id = None

            if topic.startswith(self.STORAGE_RESPONSE_PREFIX):
                request_id = topic[len(self.STORAGE_RESPONSE_PREFIX):]
            print_message("StorageClient", f"[DEBUG] request_id extraído: {request_id}", MessageType.INFO)

            # Convierte la lista a bytes si es necesario
            if isinstance(message, list):
                print_message("StorageClient", "[DEBUG] message es list, convirtiendo a bytes", MessageType.INFO)
                message = bytes(message)
            elif isinstance(message, str):
                print_message("StorageClient", "[DEBUG] message es str, convirtiendo a bytes", MessageType.INFO)
                message = message.encode('utf-8')

            # Decodificar respuesta
            message_str = message.decode('utf-8')
            print_message("StorageClient", f"[DEBUG] message_str decodificado: {message_str}", MessageType.INFO)
            response_data = json.loads(message_str)
            print_message("StorageClient", f"[DEBUG] response_data decodificado: {response_data}", MessageType.INFO)

            print_message("StorageClient", f"📬 Respuesta recibida en {topic}, ID: {request_id}", MessageType.INFO)

            if request_id:
                # Guardar la respuesta con el ID del request
                self._responses[request_id] = response_data

                # Notificar al solicitante si existe el evento
                if request_id in self._response_events:
                    self._response_events[request_id].set()
                    print_message("StorageClient", f"✅ Notificación enviada para {request_id}", MessageType.SUCCESS)
                else:
                    print_message("StorageClient", f"⚠️ No hay evento para {request_id}", MessageType.WARNING)
            else:
                print_message("StorageClient", "⚠️ No se pudo extraer el ID del request", MessageType.WARNING)

        except json.JSONDecodeError:
            print_message("StorageClient", f"❌ JSON inválido: {message}", MessageType.ERROR)
        except Exception as e:
            print_message("StorageClient", f"❌ Error al procesar respuesta: {e}", MessageType.ERROR)
            import traceback
            traceback.print_exc()

    def get_topics(self, timeout=5.0):
        """
        Obtiene la lista de tópicos disponibles en el historial
        
        Args:
            timeout: Tiempo de espera máximo en segundos
            
        Returns:
            list: Lista de nombres de tópicos
        """
        print_message("StorageClient", "Solicitando lista de tópicos disponibles...", MessageType.INFO)
        
        request_data = {
            "action": "topics"
        }
        
        # Enviar solicitud
        request_json = json.dumps(request_data)
        success = self.client.publish(self.STORAGE_QUERY_PREFIX, request_json)
        
        if not success:
            print_message("StorageClient", "❌ Error al enviar solicitud", MessageType.ERROR)
            return []
        
        # Esperar respuesta
        response = self._wait_for_response(self.STORAGE_QUERY_PREFIX, timeout)
        
        if response and response.get("status") == "success":
            topics = response.get("topics", [])
            print_message("StorageClient", f"✅ {len(topics)} tópicos disponibles", MessageType.SUCCESS)
            return topics
        else:
            error = response.get("message", "Error desconocido") if response else "Tiempo de espera agotado"
            print_message("StorageClient", f"❌ Error al obtener tópicos: {error}", MessageType.ERROR)
            return []
        
    def get_messages(self, topic=None, limit=100, from_date=None, to_date=None, timeout=10.0):
        """
        Consulta mensajes del historial para un tópico específico.
        
        Args:
            topic (str): Nombre del tópico a consultar.
            limit (int): Máximo número de mensajes a recuperar.
            from_date (str): Fecha inicial (opcional).
            to_date (str): Fecha final (opcional).
            timeout (float): Tiempo máximo de espera para la respuesta.
            
        Returns:
            list: Lista de mensajes si éxito, o lista vacía si error.
        """
        if not topic:
            print_message("StorageClient", "❌ Tópico no especificado", MessageType.ERROR)
            return []
        
        print_message("StorageClient", f"Solicitando mensajes para tópico: '{topic}'", MessageType.INFO)
        
        request_data = {
            "action": "query",
            "topic": topic,
            "limit": limit,
            "from_date": from_date,
            "to_date": to_date
        }
        
        # Serializar la solicitud
        request_json = json.dumps(request_data)
        
        # Publicar solicitud a _storage/query/<topic>
        query_topic = f"{self.STORAGE_QUERY_PREFIX}{topic}"
        success = self.client.publish(query_topic, request_json)
        
        if not success:
            print_message("StorageClient", "❌ Error al enviar solicitud", MessageType.ERROR)
            return []
        
        # Crear evento para esperar respuesta
        request_id = topic  # Solo el ID, no el tópico completo
        self._response_events[request_id] = threading.Event()


        # Esperar la respuesta
        response = self._wait_for_response(request_id, timeout)
        
        if response and response.get("status") == "success":
            messages = response.get("messages", [])
            print_message("StorageClient", f"✅ Recibidos {len(messages)} mensajes para '{topic}'", MessageType.SUCCESS)
            return messages
        else:
            error = response.get("message", "Error desconocido") if response else "Tiempo de espera agotado"
            print_message("StorageClient", f"❌ Error al consultar mensajes: {error}", MessageType.ERROR)
            return []

    
    def _wait_for_response(self, request_id, timeout):
        """
        Espera una respuesta del servicio de almacenamiento

        Args:
            request_id: ID de la solicitud (por ejemplo, el nombre del tópico)
            timeout: Tiempo de espera máximo en segundos

        Returns:
            dict: Datos de la respuesta o None si hay timeout
        """
        print_message("StorageClient", f"[DEBUG] Esperando evento para request_id={request_id}", MessageType.INFO)
        # Verificar que tengamos un evento
        if request_id not in self._response_events:
            print_message("StorageClient", f"❌ Sin evento para el ID {request_id}", MessageType.ERROR)
            return None

        time.sleep(0.1)

        print_message("StorageClient", f"⏳ Esperando respuesta para {request_id} (timeout: {timeout}s)...", MessageType.INFO)

        start_time = time.time()
        dots = 0

        while time.time() - start_time < timeout:
            self.client.poll()

            if int(time.time() - start_time) > dots:
                dots = int(time.time() - start_time)
                sys.stdout.write("." if dots % 5 != 0 else f" {dots}s ")
                sys.stdout.flush()

            if self._response_events[request_id].is_set():
                elapsed = time.time() - start_time
                print_message("StorageClient", f"✅ Recibida respuesta después de {elapsed:.1f}s", MessageType.SUCCESS)

                response = self._responses.get(request_id)
                print_message("StorageClient", f"[DEBUG] Respuesta obtenida para {request_id}: {response}", MessageType.INFO)

                # Limpiar
                del self._response_events[request_id]
                if request_id in self._responses:
                    del self._responses[request_id]

                return response

            time.sleep(0.01)

        print_message("StorageClient", f"⚠️ Timeout después de {timeout}s", MessageType.WARNING)

        # Timeout
        if request_id in self._response_events:
            del self._response_events[request_id]
        return None  
    
    def check_service_available(self, timeout=2.0):
        """
        Comprueba si el servicio de almacenamiento está disponible
        
        Args:
            timeout: Tiempo máximo de espera en segundos
            
        Returns:
            bool: True si el servicio está disponible
        """
        print_message("StorageClient", "Verificando disponibilidad del servicio de almacenamiento...", MessageType.INFO)
        
        # Publicar un mensaje de ping
        ping_id = f"ping_{time.time()}"
        
        # Crear evento para esperar respuesta
        ping_received = threading.Event()
        
        # Handler para la respuesta de ping
        def on_status(topic, message):
            ping_received.set()
        
        # Suscribirse al status
        self.client.subscribe(self.STORAGE_STATUS, on_status)
        
        # Enviar ping
        self.client.publish(self.STORAGE_STATUS, json.dumps({
            "action": "ping",
            "id": ping_id,
            "timestamp": time.time()
        }))
        
        # Esperar respuesta
        start_time = time.time()
        while time.time() - start_time < timeout:
            self.client.poll()
            
            if ping_received.is_set():
                self.client.unsubscribe(self.STORAGE_STATUS)
                print_message("StorageClient", "✅ Servicio de almacenamiento disponible", MessageType.SUCCESS)
                return True
            
            time.sleep(0.01)
        
        # Timeout
        self.client.unsubscribe(self.STORAGE_STATUS)
        print_message("StorageClient", "❌ Servicio de almacenamiento no disponible", MessageType.ERROR)
        return False
    
    def close(self):
        """Cierra el cliente y libera recursos"""
        if self._own_client and self.client.is_connected():
            print_message("StorageClient", "Cerrando cliente...", MessageType.INFO)
            self.client.disconnect()
            print_message("StorageClient", "✅ Cliente cerrado", MessageType.SUCCESS)
