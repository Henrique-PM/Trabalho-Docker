# Sistema de Monitoramento de Sensores com Kafka em Cluster Docker

## Descrição

Este projeto implementa um **sistema de monitoramento de sensores** para uma fábrica inteligente utilizando **Apache Kafka em cluster Docker**.  
O sistema simula sensores (produtores) que enviam dados de temperatura, vibração e energia, processados por consumidores que detectam anomalias e registram informações em banco de dados e via WebSocket para um frontend.

---

## Arquitetura

O sistema segue a arquitetura **produtor → Kafka cluster → consumidor → frontend/log**, garantindo **balanceamento de carga, escalabilidade e tolerância a falhas**.

1. **Produtores (sensor-serviceX)**  
   - Enviam mensagens JSON periódicas para o tópico `dados-sensores`.  
   - Dados enviados: `sensor_id`, `producer_id`, `temperatura`, `vibracao`, `energia`, `timestamp`, `message_id`.  
   - Reconexão automática em caso de falha de broker.

2. **Cluster Kafka**  
   - 3 brokers (`kafka1`, `kafka2`, `kafka3`) com partições e replicação (replication-factor=2).  
   - `kafka-setup` cria e valida o tópico `dados-sensores`.  
   - Failover automático em caso de queda de broker.

3. **Consumidores (consumer-serviceX)**  
   - Pertencem ao mesmo **grupo de consumo** (`grupo-consumidores`).  
   - Processam partições atribuídas dinamicamente, detectam anomalias e gravam dados em **SQLite**.  
   - Enviam dados em tempo real para frontend via **WebSocket**.  
   - Rebalanço automático caso algum consumidor falhe.

4. **Frontend**  
   - Container Nginx servindo arquivos estáticos que exibem dados via WebSocket.

5. **Fluxo de Dados**
    - Produtores → Kafka Cluster → Consumidores → SQLite / Frontend


## Funcionalidades

- Cluster Kafka com múltiplos brokers e tópico particionado.  
- Produtores simulando sensores enviando dados periódicos.  
- Consumidores processando dados, detectando anomalias e salvando em SQLite.  
- Visualização em tempo real via WebSocket.  
- Failover e rebalanço automático de consumidores.  
- Makefile com comandos para build, up, stop, status e simulação de falhas.


## Estrutura do Projeto

```ascii
├── consumer-service/ # Código Python do consumidor
├── producer-service/ # Código Python do produtor
├── kafka/ # Dockerfile e configuração Kafka
├── frontend/ # Arquivos estáticos do frontend (Nginx)
├── docker-compose.yml # Orquestração de todos os containers
├── Makefile # Automatização de build, up, stop, fail
└── README.md # Documentação do projeto
```

---

## Makefile

```makefile
all: stop build up status

stop:
	@echo "Parando containers..."
	docker ps -a
	docker stop kafka1 kafka2 kafka3 kafka-setup producer-service0 producer-service1 producer-service2 consumer-service0 consumer-service1 consumer-service2 frontend || true
	docker rm kafka1 kafka2 kafka3 kafka-setup producer-service0 producer-service1 producer-service2 consumer-service0 consumer-service1 consumer-service2 frontend || true
	docker ps -a

build:
	docker-compose build

up:
	docker-compose up -d

status:
	docker ps -a

fail-kafka1:
	docker stop kafka1 || true

fail-consumer0:
	docker stop consumer-service0 || true

fail-producer0:
	docker stop producer-service0 || true

fail-kafka1-log:
	fail-kafka1
	@echo ">>> Verificando rebalanço nos outros brokers..."
	docker logs kafka2 --tail 50
	docker logs kafka3 --tail 50

fail-consumer0-log:
	fail-consumer0
	@echo ">>> Logs do grupo de consumidores (rebalanço esperado)..."
	docker logs consumer-service1 --tail 50
	docker logs consumer-service2 --tail 50

fail-producer0-log:
	fail-producer0
	@echo ">>> Verificando os outros produtores..."
	docker logs producer-service1 --tail 30
	docker logs producer-service2 --tail 30
