include .env
export
DC = docker compose

.PHONY: start stop clean kafka-build create-table setup-all
build-image:
	@echo "Staring build custom-airflow"
	@docker build -f Dockerfile . -t custom-airflow

start-container:
	@echo "Starting Docker Compose services..."
	@COMPOSE_BAKE=true $(DC) --env-file .env up -d --force-recreate
stop:
	@echo "Stopping Docker Compose services..."
	@$(DC) down

clean:
	@echo "Stopping and removing all containers, networks, and volumes..."
	@$(DC) down -v
	@docker system prune -af

start:
	@echo "Setting up Weather-Sales Analysis Platform..."
	@echo "Creating necessary directories..."
	@mkdir -p data dags logs plugins config
	@echo "Copying DAG files to dags directory..."
	@cp -r code/dags/* ./dags/ 2>/dev/null || echo "No DAG files found in code/dags. Please ensure they exist."
	@echo "Building custom Airflow image..."
	@docker build -f Dockerfile . -t custom-airflow
	@echo "Starting all containers..."
	@COMPOSE_BAKE=true $(DC) --env-file .env up -d --force-recreate
	@echo "Waiting for database to be ready..."
	@sleep 10
	@echo "Creating database tables..."
	@echo "Setup complete!"
	@echo "Airflow UI: http://localhost:$(AIRFLOW_WEB_SERVER_PORTS)"
	@echo "Grafana UI: http://localhost:3000"
	@echo ""
	@echo "Default credentials:"
	@echo "- Airflow: $(AIRFLOW_SUPER_ADMIN_USERNAME) / $(AIRFLOW_SUPER_ADMIN_PASSWORD)"
	@echo "- Grafana: $(GRAFANA_ADMIN_USER) / $(GRAFANA_ADMIN_PASSWORD)"