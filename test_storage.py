#!/usr/bin/env python3
import psycopg2

# Conectar a la base de datos
conn = psycopg2.connect("dbname=tinymq user=postgres password=12345 host=localhost")

with conn.cursor() as cur:
    # Mostrar todos los tópicos
    cur.execute("SELECT DISTINCT topic FROM messages")
    topics = [row[0] for row in cur.fetchall()]
    
    print(f"Tópicos disponibles ({len(topics)}):")
    for topic in topics:
        print(f"  - '{topic}'")
    
    # Para cada tópico, mostrar cantidad de mensajes
    for topic in topics:
        cur.execute("SELECT COUNT(*) FROM messages WHERE topic = %s", (topic,))
        count = cur.fetchone()[0]
        print(f"  '{topic}': {count} mensajes")
        
        # Mostrar algunos mensajes de ejemplo
        cur.execute("""
            SELECT message, readable_time 
            FROM messages 
            WHERE topic = %s 
            ORDER BY timestamp DESC 
            LIMIT 3
        """, (topic,))
        
        examples = cur.fetchall()
        if examples:
            print("    Ejemplos:")
            for msg, time in examples:
                print(f"    - [{time}] {msg[:50]}...")
        
        print()

conn.close()