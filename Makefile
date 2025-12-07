.PHONY: help up down logs clean test

.DEFAULT_GOAL := help

help: ## Display this help
	@echo "AirStay Analytics Platform - Available Commands:"
	@awk 'BEGIN {FS = ":.*##"; printf "\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  %-20s %s\n", $$1, $$2 } ' $(MAKEFILE_LIST)

up: ## Start all services
	@echo "🚀 Starting AirStay Analytics Platform..."
	docker-compose up -d
	@echo "✅ All services started!"
	@echo ""
	@echo "Access Points:"
	@echo "  • Airflow:    http://localhost:8080 (admin/admin)"
	@echo "  • MinIO:      http://localhost:9001 (minioadmin/minioadmin)"
	@echo "  • API:        http://localhost:8000/docs"
	@echo "  • Dashboard:  http://localhost:8501"
	@echo "  • Spark:      http://localhost:8089"

down: ## Stop all services
	docker-compose down

logs: ## View logs
	docker-compose logs -f

clean: ## Stop and remove all volumes
	docker-compose down -v

test: ## Run tests
	docker-compose exec dbt pytest /usr/app/tests -v

build: ## Build custom images
	docker-compose build

shell-dbt: ## Open dbt shell
	docker-compose exec dbt bash

shell-airflow: ## Open Airflow shell
	docker-compose exec airflow-webserver bash

run-dbt: ## Run dbt transformations
	docker-compose exec dbt dbt run --profiles-dir . --project-dir .

health: ## Check service health
	@echo "Checking services..."
	@docker-compose ps