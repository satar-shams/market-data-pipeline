.PHONY: up down extract test lint format

up:
	docker-compose up -d

down:
	docker-compose down

extract:
	python -m etl.extract.yfinance_extractor

test:
	pytest tests/ -v --cov=etl --cov-report=term-missing

lint:
	ruff check .

format:
	black .
