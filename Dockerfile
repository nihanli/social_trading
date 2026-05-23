FROM python:3.11-slim

WORKDIR /app

# Install system deps for psycopg2 and spacy
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
COPY src/ src/

RUN pip install --no-cache-dir -e . \
    && python -m spacy download en_core_web_sm

COPY . .
