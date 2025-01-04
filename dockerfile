# Dockerfile
FROM ghcr.io/mlflow/mlflow

# Install psycopg2
RUN pip install psycopg2-binary