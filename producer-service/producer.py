"""
	Mateus Mansour (2310555)
	Paulo Henrique Lopes Moncao (2310736)

Simulador de Produtor Kafka para Dados de Sensores Industriais.

Este script simula o envio periódico de dados de sensores (como temperatura, vibração e consumo de energia) 
para um cluster Kafka. As mensagens são produzidas com um formato JSON e enviadas para um tópico especificado.

O script realiza as seguintes funções:
- Estabelece conexão com os brokers Kafka configurados.
- Gera dados de sensores com valores aleatórios.
- Envia os dados para um tópico Kafka com uma chave baseada no sensor_id.
- Realiza tentativas automáticas de reconexão em caso de falha na conexão com o Kafka.

Variáveis de ambiente configuráveis:
- KAFKA_BROKERS: lista de brokers Kafka separados por vírgula (default: "kafka1:9092,kafka2:9093,kafka3:9094").
- TOPIC: nome do tópico Kafka onde as mensagens serão enviadas (default: "dados-sensores").
- SENSOR_PREFIX: prefixo que será usado na identificação dos sensores (default: "sensor_0").

Requisitos:
- Kafka em execução com o tópico especificado criado.
- Biblioteca kafka-python instalada.

Exemplo de execução contínua:
    python produtor_sensores.py
"""

import os
import json
import random
import time
import hashlib
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka1:9092,kafka2:9093,kafka3:9094").split(",")
TOPIC = os.getenv("TOPIC", "dados-sensores")
SENSOR_PREFIX = os.getenv("SENSOR_PREFIX", "sensor_0")  

producer = None

print(f"[PRODUCER {SENSOR_PREFIX}] Iniciando...")
print(f"[PRODUCER {SENSOR_PREFIX}] Brokers: {KAFKA_BROKERS}")
print(f"[PRODUCER {SENSOR_PREFIX}] Tópico: {TOPIC}")

while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks='all', 
            retries=3,
            batch_size=16384,
            linger_ms=10
        )
        print(f"[PRODUCER {SENSOR_PREFIX}] Conectado aos brokers: {KAFKA_BROKERS}")
    except NoBrokersAvailable:
        print(f"[PRODUCER {SENSOR_PREFIX}] Nenhum broker disponível, tentando novamente em 5s...")
        time.sleep(5)

message_count = 0

while True:
    sensor_id = f"{SENSOR_PREFIX}_{random.randint(1, 5)}" 
    
    data = {
        "sensor_id": sensor_id,  
        "producer_id": SENSOR_PREFIX,  
        "temperatura": random.randint(20, 100),
        "vibracao": random.randint(1, 10),
        "energia": round(random.uniform(0.5, 5.0), 2),
        "timestamp": time.time(),
        "message_id": message_count  
    }
    
    try:
        key = sensor_id.encode('utf-8')
        future = producer.send(TOPIC, key=key, value=data)
        
        
        metadata = future.get(timeout=10)
        
        message_count += 1
        
        print(f"[PRODUCER {SENSOR_PREFIX}] Enviado: Partição {metadata.partition}, "
              f"Offset {metadata.offset}, Sensor: {sensor_id}, "
              f"Temp: {data['temperatura']}°C")
              
    except Exception as e:
        print(f"[PRODUCER {SENSOR_PREFIX}] Erro ao enviar: {e}")
     
        try:
            producer.close()
        except:
            pass
        producer = None
        while producer is None:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BROKERS,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8")
                )
                print(f"[PRODUCER {SENSOR_PREFIX}] Reconectado aos brokers")
            except NoBrokersAvailable:
                print(f"[PRODUCER {SENSOR_PREFIX}] Tentando reconectar em 5s...")
                time.sleep(5)
    
    
    time.sleep(60)  