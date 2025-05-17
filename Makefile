ENV_FILE ?= .env
COMPOSE   = docker-compose --env-file $(ENV_FILE)
UV        = uv
PY        = python

.PHONY: install up down logs reset-db shell \
        init_db load_movies enrich-crew analyze-descriptions run_queries \
        analyze-reports pipeline help

install:
	$(UV) sync

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f db

reset-db:
	$(COMPOSE) exec -T db psql -U $$POSTGRES_USER -d $$POSTGRES_DB -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
	$(COMPOSE) exec -T db psql -U $$POSTGRES_USER -d $$POSTGRES_DB -f /docker-entrypoint-initdb.d/01_init.sql

shell:
	$(COMPOSE) exec db psql -U $$POSTGRES_USER -d $$POSTGRES_DB

PYTHON_ANALYZER = python3
JSON_DATA_FILE_ANALYZER ?= data/movies_metadata.jsonl
SEED_NUM_ANALYZER ?= 1000
TOP_N_WORDS_ANALYZER ?= 20

init_db:
	@echo "Initializing database schema from new init_db.sql..."
	@docker compose up -d postgres
	@echo "New database initialization complete."

load_movies: $(JSON_DATA_FILE_ANALYZER)
	@echo "Loading movies from $(JSON_DATA_FILE_ANALYZER) using analyzer.py..."
	@$(PYTHON_ANALYZER) analyzer.py load-movies --json-file $(JSON_DATA_FILE_ANALYZER)
	@echo "Movie loading with analyzer.py complete."

enrich-crew:
	@echo "Enriching crew information via analyzer.py..."
	@$(PYTHON_ANALYZER) analyzer.py enrich-crew
	@echo "Crew enrichment complete."

analyze-descriptions:
	@echo "Performing linguistic analysis (top $(TOP_N_WORDS_ANALYZER) words) using analyzer.py..."
	@$(PYTHON_ANALYZER) analyzer.py analyze-descriptions --top-n $(TOP_N_WORDS_ANALYZER)
	@echo "Linguistic analysis with analyzer.py complete."

run_queries:
	@echo "Running sample SQL queries using analyzer.py..."
	@$(PYTHON_ANALYZER) analyzer.py execute-queries
	@echo "SQL queries execution with analyzer.py complete."

analyze-reports:
	@echo "Running original analysis for reports (word cloud, CSVs) using analyzer.py..."
	@$(PYTHON_ANALYZER) analyzer.py analyze-westerns
	@echo "Original analysis for reports with analyzer.py complete."

pipeline: init_db load_movies enrich-crew \
              analyze-descriptions run_queries analyze-reports
	@echo "✅  Main pipeline (analyzer.py based) complete"

$(JSON_DATA_FILE_ANALYZER):
	@mkdir -p $(dir $(JSON_DATA_FILE_ANALYZER))
	@echo "Please place your movie data file at $(JSON_DATA_FILE_ANALYZER) for the new pipeline."
	@echo "If you don't have one, you can try creating a dummy file or downloading one."

help:
	@echo "\nAvailable targets for Main Workflow (analyzer.py based):"
	@echo "  init_db                - Initialize DB with new schema (runs init_db.sql)"
	@echo "  load_movies            - Load movies from $(JSON_DATA_FILE_ANALYZER) via analyzer.py"
	@echo "  enrich-crew            - Enrich crew information via analyzer.py"
	@echo "  analyze-descriptions   - Perform linguistic analysis via analyzer.py"
	@echo "  analyze-reports        - Run original analysis (word cloud, CSVs) via analyzer.py"
	@echo "  run_queries            - Run new sample SQL queries via analyzer.py"
	@echo "  pipeline               - Run all main workflow steps: init_db, load_movies, enrich-crew, analyze-descriptions, run_queries, analyze-reports"
	@echo ""
	@echo "Configuration variables for Main workflow (can be overridden):"
	@echo "  JSON_DATA_FILE_ANALYZER=$(JSON_DATA_FILE_ANALYZER)"
	@echo "  SEED_NUM_ANALYZER=$(SEED_NUM_ANALYZER)"
	@echo "  TOP_N_WORDS_ANALYZER=$(TOP_N_WORDS_ANALYZER)"
	@echo ""
	@echo "Ensure PostgreSQL environment variables are set."
