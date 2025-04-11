import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import time
from tinymq_module import Client

class TinyMQApp:
    def __init__(self, root):
        self.root = root
        self.root.title("TinyMQ Client")
        self.root.geometry("700x500")
        self.root.configure(bg="#f0f0f0")
        
        self.client = Client("tkinter_client")
        self.is_connected = False
        self.subscribed_topics = set()
        
        self.create_widgets()
        
        # Iniciar hilo para polling
        self.running = True
        self.poll_thread = threading.Thread(target=self.poll_messages)
        self.poll_thread.daemon = True
        self.poll_thread.start()
    
    def create_widgets(self):
        # Panel principal con pestañas
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Estilos personalizados
        style = ttk.Style()
        style.configure('TButton', font=('Helvetica', 10))
        style.configure('TLabel', font=('Helvetica', 10))
        style.configure('Big.TButton', font=('Helvetica', 12))
        
        # === Pestaña de conexión ===
        connection_tab = ttk.Frame(notebook)
        notebook.add(connection_tab, text="Conexión")
        
        # Frame de conexión
        conn_frame = ttk.LabelFrame(connection_tab, text="Estado de Conexión")
        conn_frame.pack(fill="x", padx=10, pady=10)
        
        # Estado de conexión
        self.status_var = tk.StringVar(value="Desconectado")
        status_label = ttk.Label(conn_frame, textvariable=self.status_var, font=('Helvetica', 14, 'bold'))
        status_label.pack(pady=10)
        
        # Botones de conexión
        btn_frame = ttk.Frame(conn_frame)
        btn_frame.pack(pady=10, fill="x")
        
        self.connect_btn = ttk.Button(btn_frame, text="Conectar", 
                                     command=self.connect_to_broker, style='Big.TButton')
        self.connect_btn.pack(side="left", padx=10, expand=True, fill="x")
        
        self.disconnect_btn = ttk.Button(btn_frame, text="Desconectar", 
                                        command=self.disconnect_from_broker, 
                                        state="disabled", style='Big.TButton')
        self.disconnect_btn.pack(side="left", padx=10, expand=True, fill="x")
        
        # === Pestaña de publicación ===
        publish_tab = ttk.Frame(notebook)
        notebook.add(publish_tab, text="Publicar")
        
        # Frame para publicar
        pub_frame = ttk.LabelFrame(publish_tab, text="Publicar Mensaje")
        pub_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Campo de tópico
        topic_frame = ttk.Frame(pub_frame)
        topic_frame.pack(fill="x", pady=5)
        
        ttk.Label(topic_frame, text="Tópico:").pack(side="left", padx=5)
        self.pub_topic_entry = ttk.Entry(topic_frame)
        self.pub_topic_entry.pack(side="left", padx=5, expand=True, fill="x")
        # aceptar enter
        self.pub_topic_entry.bind('<Return>', lambda event: self.pub_message_text.focus_set())

        
        # Campo de mensaje
        msg_frame = ttk.Frame(pub_frame)
        msg_frame.pack(fill="both", expand=True, pady=5)
        
        ttk.Label(msg_frame, text="Mensaje:").pack(anchor="nw", padx=5)
        self.pub_message_text = scrolledtext.ScrolledText(msg_frame, height=8)
        self.pub_message_text.pack(fill="both", expand=True, padx=5, pady=5)
        self.pub_message_text.bind('<Return>', lambda event: self.publish_message())

        
        # Botón de publicación
        ttk.Button(pub_frame, text="Publicar Mensaje", 
                  command=self.publish_message, style='Big.TButton').pack(pady=10)
        
        # === Pestaña de suscripciones ===
        subscribe_tab = ttk.Frame(notebook)
        notebook.add(subscribe_tab, text="Suscripciones")
        
        # Frame izquierdo para suscripciones
        sub_frame = ttk.LabelFrame(subscribe_tab, text="Gestión de Suscripciones")
        sub_frame.pack(side="left", fill="both", expand=True, padx=10, pady=10)
        
        # Campo de tópico para suscripción
        sub_topic_frame = ttk.Frame(sub_frame)
        sub_topic_frame.pack(fill="x", pady=5)
        
        ttk.Label(sub_topic_frame, text="Tópico:").pack(side="left", padx=5)
        self.sub_topic_entry = ttk.Entry(sub_topic_frame)
        self.sub_topic_entry.pack(side="left", padx=5, expand=True, fill="x")
        self.sub_topic_entry.bind('<Return>', lambda event: self.subscribe_to_topic())
        
        # Botones de suscripción
        btn_sub_frame = ttk.Frame(sub_frame)
        btn_sub_frame.pack(fill="x", pady=10)
        
        ttk.Button(btn_sub_frame, text="Suscribirse", 
                  command=self.subscribe_to_topic).pack(side="left", padx=5, expand=True, fill="x")
        ttk.Button(btn_sub_frame, text="Cancelar Suscripción", 
                  command=self.unsubscribe_from_topic).pack(side="left", padx=5, expand=True, fill="x")
        
        # Lista de tópicos suscritos
        ttk.Label(sub_frame, text="Tópicos Suscritos:").pack(anchor="w", padx=5, pady=5)
        self.topics_listbox = tk.Listbox(sub_frame, height=10)
        self.topics_listbox.pack(fill="both", expand=True, padx=5, pady=5)
        
        # === Pestaña de mensajes ===
        messages_tab = ttk.Frame(notebook)
        notebook.add(messages_tab, text="Mensajes Recibidos")
        
        # Área de mensajes
        msg_area_frame = ttk.LabelFrame(messages_tab, text="Mensajes")
        msg_area_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        self.message_area = scrolledtext.ScrolledText(msg_area_frame)
        self.message_area.pack(fill="both", expand=True, padx=5, pady=5)
        self.message_area.config(state="disabled")  # Solo lectura
        
        # Botón para limpiar mensajes
        ttk.Button(msg_area_frame, text="Limpiar Mensajes", 
                  command=self.clear_messages).pack(pady=5)
    
    def connect_to_broker(self):
        if self.client.connect():
            self.is_connected = True
            self.status_var.set("Conectado")
            self.connect_btn.config(state="disabled")
            self.disconnect_btn.config(state="normal")
            self.add_message("Sistema", "Conectado al broker")
        else:
            messagebox.showerror("Error de Conexión", 
                               "No se pudo conectar al broker. Asegúrate de que esté en ejecución.")
    
    def disconnect_from_broker(self):
        self.client.disconnect()
        self.is_connected = False
        self.status_var.set("Desconectado")
        self.connect_btn.config(state="normal")
        self.disconnect_btn.config(state="disabled")
        self.add_message("Sistema", "Desconectado del broker")
    
    def publish_message(self):
        if not self.is_connected:
            messagebox.showwarning("No conectado", 
                                "Debes conectarte al broker primero.")
            return
        
        topic = self.pub_topic_entry.get().strip()
        message = self.pub_message_text.get("1.0", "end-1c").strip()
        
        if not topic:
            messagebox.showwarning("Tópico vacío", 
                                "Por favor, ingresa un tópico.")
            return
        
        if not message:
            messagebox.showwarning("Mensaje vacío", 
                                "Por favor, ingresa un mensaje.")
            return
        
        if self.client.publish(topic, message):
            self.add_message("Enviado", f"Tópico: {topic}\nMensaje: {message}")
            # Limpiar solo el mensaje, no el tópico
            self.pub_message_text.delete("1.0", tk.END)
        else:
            messagebox.showerror("Error", "No se pudo publicar el mensaje.")
    
    def subscribe_to_topic(self):
        if not self.is_connected:
            messagebox.showwarning("No conectado", 
                                "Debes conectarte al broker primero.")
            return
        
        topic = self.sub_topic_entry.get().strip()
        
        if not topic:
            messagebox.showwarning("Tópico vacío", 
                                "Por favor, ingresa un tópico.")
            return
        
        if topic in self.subscribed_topics:
            messagebox.showinfo("Ya suscrito", 
                              f"Ya estás suscrito al tópico '{topic}'.")
            return
        
        if self.client.subscribe(topic, self.on_message):
            self.subscribed_topics.add(topic)
            self.topics_listbox.insert(tk.END, topic)
            self.add_message("Sistema", f"Suscrito al tópico: {topic}")
        else:
            messagebox.showerror("Error", "No se pudo suscribir al tópico.")
    
    def unsubscribe_from_topic(self):
        if not self.is_connected:
            messagebox.showwarning("No conectado", 
                                "Debes conectarte al broker primero.")
            return
        
        selection = self.topics_listbox.curselection()
        
        if not selection:
            topic = self.sub_topic_entry.get().strip()
            if not topic:
                messagebox.showwarning("Selección vacía", 
                                    "Selecciona un tópico de la lista o ingresa uno.")
                return
        else:
            topic = self.topics_listbox.get(selection[0])
        
        if topic not in self.subscribed_topics:
            messagebox.showwarning("No suscrito", 
                                f"No estás suscrito al tópico '{topic}'.")
            return
        
        if self.client.unsubscribe(topic):
            self.subscribed_topics.remove(topic)
            # Actualizar listbox
            self.topics_listbox.delete(0, tk.END)
            for t in self.subscribed_topics:
                self.topics_listbox.insert(tk.END, t)
            self.add_message("Sistema", f"Cancelada suscripción al tópico: {topic}")
        else:
            messagebox.showerror("Error", "No se pudo cancelar la suscripción.")
    
    def on_message(self, topic, message):
        msg_str = bytes(message).decode('utf-8')
        # Debemos usar after() porque este callback podría venir de otro hilo
        self.root.after(0, lambda: self.add_message("Recibido", 
                                                   f"Tópico: {topic}\nMensaje: {msg_str}"))
    
    def add_message(self, source, content):
        timestamp = time.strftime("%H:%M:%S")
        self.message_area.config(state="normal")
        self.message_area.insert(tk.END, f"[{timestamp}] [{source}]\n{content}\n\n")
        self.message_area.see(tk.END)  # Desplazar al final
        self.message_area.config(state="disabled")
    
    def clear_messages(self):
        self.message_area.config(state="normal")
        self.message_area.delete("1.0", tk.END)
        self.message_area.config(state="disabled")
    
    def poll_messages(self):
        while self.running:
            if self.is_connected:
                self.client.poll()
            time.sleep(0.1)
    
    def on_closing(self):
        self.running = False
        if self.is_connected:
            self.client.disconnect()
        self.root.destroy()

def main():
    root = tk.Tk()
    app = TinyMQApp(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()

if __name__ == "__main__":
    main()