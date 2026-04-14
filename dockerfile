# Base image
FROM python:3.10-slim

# Working directory inside the container
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code into the container
COPY full_program/ /app/full_program/

# Ensure Python can find modules
ENV PYTHONPATH=/app/full_program/airflow

# Set the working directory where train.py is located
WORKDIR /app/full_program/airflow/dags/model_training_pipeline_dag