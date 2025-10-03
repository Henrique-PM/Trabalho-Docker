all: stop build up status

stop:
	@echo "Parando containers..."
	docker ps -a
	docker stop kafka1 || true
	docker stop kafka2 || true
	docker stop kafka3 || true
	docker stop kafka-setup || true
	docker stop producer-service0 || true
	docker stop producer-service1 || true
	docker stop producer-service2 || true
	docker stop consumer-service0 || true
	docker stop consumer-service1 || true
	docker stop consumer-service2 || true
	docker stop frontend || true
	docker rm kafka1 || true
	docker rm kafka2 || true
	docker rm kafka3 || true
	docker rm kafka-setup || true
	docker rm producer-service0 || true
	docker rm producer-service1 || true
	docker rm producer-service2 || true
	docker rm consumer-service0 || true
	docker rm consumer-service1 || true
	docker rm consumer-service2 || true
	docker rm frontend || true

	docker ps -a

build:
	docker-compose build

up:
	docker-compose up 

status:
	docker ps -a

fail-kafka1:
	docker stop kafka1 || true
fail-kafka2:
	docker stop kafka2 || true
fail-kafka3:
	docker stop kafka3 || true
fail-producer0:
	docker stop producer-service0 || true
fail-producer1:
	docker stop producer-service1 || true
fail-producer2:
	docker stop producer-service2 || true
fail-consumer0:
	docker stop consumer-service0 || true
fail-consumer1:
	docker stop consumer-service1 || true
fail-consumer2:
	docker stop consumer-service2 || true

fail-kafka1-log:
	fail-kafka1
	@echo ">>> Verificando rebalanço nos outros brokers..."
	docker logs kafka2 --tail 50
	docker logs kafka3 --tail 50

fail-consumer0-log:
	fail-consumer0
	@echo ">>> Logs do grupo de consumidores (rebalanço esperado)..."
	docker logs consumer_service1 --tail 50
	docker logs consumer_service2 --tail 50

fail-producer0-log:
	fail-producer0
	@echo ">>> Verificando os outros produtores..."
	docker logs producer_service1 --tail 30
	docker logs producer_service2 --tail 30
