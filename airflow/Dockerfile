FROM apache/airflow:2.8.1

USER root
RUN apt-get update && apt-get install -y \
    default-mysql-client \
    && apt-get clean

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-mysql==5.7.1 \
    PyMySQL==1.1.0
