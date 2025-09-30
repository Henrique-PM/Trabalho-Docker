import os
import json
import asyncio
import websockets
import time
import sqlite3
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka1:9092,kafka2:9093,kafka3:9094").split(",")
TOPIC = os.getenv("TOPIC", "dados-sensores")
GROUP_ID = os.getenv("GROUP_ID", "grupo-consumidores")
CONSUMER_ID = os.getenv("CONSUMER_ID", "unknown") 

connected_clients = set()
consumer = None

print(f"[CONSUMER {CONSUMER_ID}] Iniciando...")
print(f"[CONSUMER {CONSUMER_ID}] Brokers: {KAFKA_BROKERS}")
print(f"[CONSUMER {CONSUMER_ID}] Tópico: {TOPIC}")
print(f"[CONSUMER {CONSUMER_ID}] Grupo: {GROUP_ID}")

conn = sqlite3.connect("sensores.db")
cursor = conn.cursor()

cursor.execute("""CREATE TABLE IF NOT EXISTS sensores(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id TEXT,
    producer_id TEXT,
    consumer_id TEXT, 
    partition INTEGER,
    temperatura INTEGER,
    vibracao INTEGER,
    energia REAL,
    timestamp INTEGER
)""")
conn.commit()

while consumer is None:
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BROKERS,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=GROUP_ID,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        print(f"[CONSUMER {CONSUMER_ID}] Conectado aos brokers: {KAFKA_BROKERS}")
    
        print(f"[CONSUMER {CONSUMER_ID}] Aguardando atribuição de partições...")
        
    except NoBrokersAvailable:
        print(f"[CONSUMER {CONSUMER_ID}] Nenhum broker disponível, tentando novamente em 5s...")
        time.sleep(5)

async def register_client(websocket):
    connected_clients.add(websocket)
    print(f"[CONSUMER {CONSUMER_ID}] Cliente WebSocket conectado. Total: {len(connected_clients)}")
    try:
        await websocket.wait_closed()
    finally:
        connected_clients.remove(websocket)
        print(f"[CONSUMER {CONSUMER_ID}] Cliente WebSocket desconectado. Total: {len(connected_clients)}")

async def kafka_loop():
    loop = asyncio.get_event_loop()
    
    last_assignment_check = 0
    
    while True:
        current_time = time.time()
        if current_time - last_assignment_check > 30: 
            if consumer.assignment():
                partitions = [f"P{p.partition}" for p in consumer.assignment()]
                print(f"[CONSUMER {CONSUMER_ID}] Partições atribuídas: {partitions}")
            last_assignment_check = current_time
        
        msgs = await loop.run_in_executor(None, consumer.poll, 1.0)
        
        for tp, batch in msgs.items():
            for msg in batch:
                data_dict = msg.value
                partition = msg.partition
                offset = msg.offset
                
                print(f"[CONSUMER {CONSUMER_ID}] Partição {partition}, "
                      f"Offset {offset}, Sensor: {data_dict.get('sensor_id', 'N/A')}, "
                      f"Producer: {data_dict.get('producer_id', 'N/A')}, "
                      f"Temp: {data_dict.get('temperatura', 'N/A')}°C")
                
                cursor.execute("""INSERT INTO sensores 
                               (sensor_id, producer_id, consumer_id, partition, 
                                temperatura, vibracao, energia, timestamp) 
                               VALUES (?,?,?,?,?,?,?,?)""",
                            (
                                data_dict.get("sensor_id"),
                                data_dict.get("producer_id"),
                                CONSUMER_ID, 
                                partition,                            
                                data_dict.get("temperatura"),
                                data_dict.get("vibracao"),
                                data_dict.get("energia"),
                                data_dict.get("timestamp"),
                            ))
                conn.commit()

               
                if connected_clients:
                    data_dict["consumer_id"] = CONSUMER_ID 
                    data_dict["partition"] = partition     
                    data = json.dumps(data_dict)
                    
                    await asyncio.gather(
                        *[ws.send(data) for ws in connected_clients],
                        return_exceptions=True
                    )

async def main():
    server = await websockets.serve(register_client, "0.0.0.0", 8080)
    print(f"[CONSUMER {CONSUMER_ID}] WebSocket rodando em ws://0.0.0.0:8080")
    await kafka_loop()

if __name__ == "__main__":
    asyncio.run(main())