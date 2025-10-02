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

