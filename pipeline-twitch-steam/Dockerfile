FROM apache/airflow:latest-python3.12

USER airflow

RUN pip install --no-cache-dir \
    databricks-sdk \
    requests
    
USER airflow