#!/usr/bin/env python3
# Instalar sudo apt install python3-tk

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import time
import json
from tinymq_module import Client

class TinyMQApp:
    def __init__(self, root):
        self.root = root
        self.root.title("TinyMQ Client")
        self.root.geometry("800x600")
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
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill="both", expand=True, padx=10, pady=10)
        
        style = ttk.Style()
        style.configure('TButton', font=('Helvetica', 10))
        style.configure('TLabel', font=('Helvetica', 10))
        style.configure('Big.TButton', font=('Helvetica', 12))
        
        # === Pestaña de conexión ===
        connection_tab = ttk.Frame(notebook)
        notebook.add(connection_tab, text="Conexión")
        
        conn_frame = ttk.LabelFrame(connection_tab, text="Estado de Conexión")
        conn_frame.pack(fill="x", padx=10, pady=10)
        
        self.status_var = tk.StringVar(value="Desconectado")
        status_label = ttk.Label(conn_frame, textvariable=self.status_var, font=('Helvetica', 14, 'bold'))
        status_label.pack(pady=10)
        
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
        
        pub_frame = ttk.LabelFrame(publish_tab, text="Publicar Mensaje")
        pub_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        topic_frame = ttk.Frame(pub_frame)
        topic_frame.pack(fill="x", pady=5)
        
        ttk.Label(topic_frame, text="Tópico:").pack(side="left", padx=5)
        self.pub_topic_entry = ttk.Entry(topic_frame)
        self.pub_topic_entry.pack(side="left", padx=5, expand=True, fill="x")
        self.pub_topic_entry.bind('<Return>', lambda event: self.pub_message_text.focus_set())

        msg_frame = ttk.Frame(pub_frame)
        msg_frame.pack(fill="both", expand=True, pady=5)
        
        ttk.Label(msg_frame, text="Mensaje:").pack(anchor="nw", padx=5)
        self.pub_message_text = scrolledtext.ScrolledText(msg_frame, height=8)
        self.pub_message_text.pack(fill="both", expand=True, padx=5, pady=5)
        self.pub_message_text.bind('<Return>', lambda event: self.publish_message())

        ttk.Button(pub_frame, text="Publicar Mensaje", 
                  command=self.publish_message, style='Big.TButton').pack(pady=10)
        
        # === Pestaña de suscripciones ===
        subscribe_tab = ttk.Frame(notebook)
        notebook.add(subscribe_tab, text="Suscripciones")
        
        sub_frame = ttk.LabelFrame(subscribe_tab, text="Gestión de Suscripciones")
        sub_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        sub_topic_frame = ttk.Frame(sub_frame)
        sub_topic_frame.pack(fill="x", pady=5)
        
        ttk.Label(sub_topic_frame, text="Tópico:").pack(side="left", padx=5)
        self.sub_topic_entry = ttk.Entry(sub_topic_frame)
        self.sub_topic_entry.pack(side="left", padx=5, expand=True, fill="x")
        self.sub_topic_entry.bind('<Return>', lambda event: self.subscribe_to_topic())
        
        btn_sub_frame = ttk.Frame(sub_frame)
        btn_sub_frame.pack(fill="x", pady=10)
        
        ttk.Button(btn_sub_frame, text="Suscribirse", 
                  command=self.subscribe_to_topic).pack(side="left", padx=5, expand=True, fill="x")
        ttk.Button(btn_sub_frame, text="Cancelar Suscripción", 
                  command=self.unsubscribe_from_topic).pack(side="left", padx=5, expand=True, fill="x")
        
        ttk.Label(sub_frame, text="Tópicos Suscritos:").pack(anchor="w", padx=5, pady=5)
        self.topics_listbox = tk.Listbox(sub_frame, height=10)
        self.topics_listbox.pack(fill="both", expand=True, padx=5, pady=5)
        
        # === Pestaña de mensajes modificada para incluir historial ===
        messages_tab = ttk.Frame(notebook)
        notebook.add(messages_tab, text="Mensajes e Historial")
        
        messages_paned = ttk.PanedWindow(messages_tab, orient=tk.VERTICAL)
        messages_paned.pack(fill="both", expand=True, padx=10, pady=10)
        
        msg_area_frame = ttk.LabelFrame(messages_paned, text="Mensajes en tiempo real")
        messages_paned.add(msg_area_frame, weight=1)
        
        self.message_area = scrolledtext.ScrolledText(msg_area_frame)
        self.message_area.pack(fill="both", expand=True, padx=5, pady=5)
        self.message_area.config(state="disabled")
        
        ttk.Button(msg_area_frame, text="Limpiar Mensajes", 
                  command=self.clear_messages).pack(pady=5)
        
        hist_frame = ttk.LabelFrame(messages_paned, text="Historial de mensajes")
        messages_paned.add(hist_frame, weight=1)
        
        hist_controls = ttk.Frame(hist_frame)
        hist_controls.pack(fill="x", pady=5)
        
        ttk.Label(hist_controls, text="Tópico:").pack(side="left", padx=5)
        self.hist_topic_var = tk.StringVar()
        self.hist_topic_combo = ttk.Combobox(hist_controls, textvariable=self.hist_topic_var, state="readonly")
        self.hist_topic_combo.pack(side="left", padx=5, fill="x", expand=True)
        
        ttk.Label(hist_controls, text="Límite:").pack(side="left", padx=5)
        self.hist_limit_var = tk.StringVar(value="20")
        hist_limit_combo = ttk.Combobox(hist_controls, textvariable=self.hist_limit_var, 
                                       values=["10", "20", "50", "100"], width=5, state="readonly")
        hist_limit_combo.pack(side="left", padx=5)
        
        ttk.Button(hist_controls, text="Cargar Historial", 
                  command=self.load_history).pack(side="left", padx=5)
        ttk.Button(hist_controls, text="Refrescar Tópicos", 
                  command=self.refresh_topics).pack(side="left", padx=5)
        
        self.history_area = scrolledtext.ScrolledText(hist_frame, height=10)
        self.history_area.pack(fill="both", expand=True, padx=5, pady=5)
        self.history_area.config(state="disabled")
        
        ttk.Button(hist_frame, text="Limpiar Historial", 
                  command=self.clear_history).pack(pady=5)
    
    def connect_to_broker(self):
        if self.client.connect():
            self.is_connected = True
            self.status_var.set("Conectado")
            self.connect_btn.config(state="disabled")
            self.disconnect_btn.config(state="normal")
            self.add_message("Sistema", "Conectado al broker")
            self.root.after(1000, self.refresh_topics)
        else:
            messagebox.showerror("Error de Conexión", 
                               "No se pudo conectar al broker. Asegúrate de que esté en ejecución.")
    
    def disconnect_from_broker(self):
        self.client.disconnect()
        self.is_connected = False
        self.status_var.set("Desconectado")
        self.connect_btn.config(state="normal")
        self.disconnect_btn.config(state="disabled")
        self.subscribed_topics.clear()
        self.topics_listbox.delete(0, tk.END)
        self.hist_topic_combo['values'] = []
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
            self.refresh_topics()
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
            self.topics_listbox.delete(0, tk.END)
            for t in self.subscribed_topics:
                self.topics_listbox.insert(tk.END, t)
            self.add_message("Sistema", f"Cancelada suscripción al tópico: {topic}")
            self.refresh_topics()
        else:
            messagebox.showerror("Error", "No se pudo cancelar la suscripción.")
    
    def on_message(self, topic, message):
        msg_str = bytes(message).decode('utf-8')
        self.root.after(0, lambda: self.add_message("Recibido", 
                                                  f"Tópico: {topic}\nMensaje: {msg_str}"))
    
    def add_message(self, source, content):
        timestamp = time.strftime("%H:%M:%S")
        self.message_area.config(state="normal")
        self.message_area.insert(tk.END, f"[{timestamp}] [{source}]\n{content}\n\n")
        self.message_area.see(tk.END)
        self.message_area.config(state="disabled")
    
    def clear_messages(self):
        self.message_area.config(state="normal")
        self.message_area.delete("1.0", tk.END)
        self.message_area.config(state="disabled")
    
    def refresh_topics(self):
        if not self.is_connected:
            messagebox.showwarning("No conectado", 
                                 "Debes conectarte al broker primero.")
            return
        
        try:
            if not self.subscribed_topics:
                return
            topics_list = list(self.subscribed_topics)
            self.hist_topic_combo['values'] = topics_list
            if topics_list and not self.hist_topic_var.get():
                self.hist_topic_combo.current(0)
            self.add_message("Sistema", f"Lista de tópicos actualizada: {len(topics_list)} tópicos disponibles")
        except Exception as e:
            messagebox.showerror("Error", f"Error al refrescar tópicos: {e}")
    
    def load_history(self):
        if not self.is_connected:
            messagebox.showwarning("No conectado", 
                                 "Debes conectarte al broker primero.")
            return
        
        topic = self.hist_topic_var.get()
        if not topic:
            messagebox.showwarning("Tópico no seleccionado", 
                                 "Por favor, selecciona un tópico.")
            return
        
        try:
            limit = int(self.hist_limit_var.get())
        except ValueError:
            limit = 20
        
        self.history_area.config(state="normal")
        self.history_area.delete("1.0", tk.END)
        self.history_area.insert(tk.END, "Cargando historial...\n")
        self.history_area.config(state="disabled")
        self.root.update()
        
        threading.Thread(target=self._load_history_thread, 
                        args=(topic, limit), 
                        daemon=True).start()
    
    def _load_history_thread(self, topic, limit):
        try:
            messages = self.client.get_history(topic, limit=limit)
            print(f"[DEBUG] Mensajes recibidos para {topic}: {messages}")
            self.root.after(0, lambda: self._update_history_area(topic, messages))
        except Exception as e:
            import traceback
            error_info = str(e)
            if not error_info:
                error_info = f"Excepción de tipo {type(e).__name__}"
            print(f"Error al cargar historial: {error_info}")
            traceback.print_exc()
            self.root.after(0, lambda err=error_info: self._show_history_error(err))

    def _update_history_area(self, topic, messages):
        self.history_area.config(state="normal")
        self.history_area.delete("1.0", tk.END)
        if not messages:
            self.history_area.insert(tk.END, f"No hay mensajes históricos para el tópico '{topic}'.\n")
        else:
            self.history_area.insert(tk.END, f"=== Historial de mensajes para '{topic}' (últimos {len(messages)}) ===\n\n")
            for msg in messages:
                if isinstance(msg, dict):
                    fecha = msg.get("readable_time", "")
                    mensaje = msg.get("message", "")
                    self.history_area.insert(tk.END, f"[{fecha}] {mensaje}\n\n")
                else:
                    self.history_area.insert(tk.END, f"{msg}\n\n")
        self.history_area.see("1.0")
        self.history_area.config(state="disabled")
    
    def _show_history_error(self, error_message):
        try:
            if not self.running or not self.root.winfo_exists():
                return
            self.history_area.config(state="normal")
            self.history_area.delete("1.0", tk.END)
            self.history_area.insert(tk.END, f"Error al cargar historial: {error_message}\n")
            self.history_area.config(state="disabled")
            if self.running and self.root.winfo_exists():
                messagebox.showerror("Error", f"No se pudo cargar el historial: {error_message}")
        except Exception:
            pass
    
    def clear_history(self):
        self.history_area.config(state="normal")
        self.history_area.delete("1.0", tk.END)
        self.history_area.config(state="disabled")
    
    def poll_messages(self):
        while self.running:
            if self.is_connected:
                try:
                    self.client.poll()
                except Exception as e:
                    print(f"Error en poll: {e}")
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