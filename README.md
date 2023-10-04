# Data Catalog implemented using Elasticsearch and Fast API

[![Run Tests](https://github.com/awhipp/fastapi-elasticsearch-data-catalog/actions/workflows/run_tests.yml/badge.svg)](https://github.com/awhipp/fastapi-elasticsearch-data-catalog/actions/workflows/run_tests.yml)

## Description

This is a simple data catalog implemented using Elasticsearch and Fast API. It is intended to be used as a starting point for building a more complex data catalog.

### Features

- Search for data assets
- Add new data assets
- Delete data assets
- Update data assets
- View data asset details
- (Future TBD) Automatically scan for new data assets based on data sources

## Installation

```bash
poetry install
```

## Usage

Start Elasticsearch

```bash
docker-compose up -d
```

Start the application

```bash
poetry shell
python app.py
```

Test the application

```bash
pytest
```
