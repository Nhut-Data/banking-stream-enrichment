.PHONY: help setup up down generate test

help:
	@echo "Các lệnh có sẵn:"
	@echo "  make setup     — Tải Berka data vào data/raw/"
	@echo "  make up        — Khởi động toàn bộ stack (Docker)"
	@echo "  make down      — Tắt stack"
	@echo "  make generate  — Chạy data_generation (sinh trans_dev + trans_loadtest)"
	@echo "  make test      — Chạy unit tests"

setup:
	@echo "TODO: thêm lệnh tải Berka CSV về data/raw/"

up:
	docker compose up -d

down:
	docker compose down

generate:
	python -m data_generation.run

test:
	pytest tests/unit/ -v

register-connector:
	curl -X POST http://localhost:8083/connectors \
		-H "Content-Type: application/json" \
		-d @infra/debezium/connector-config.json
