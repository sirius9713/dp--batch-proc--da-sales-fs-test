FROM docker-local-dataplatform.art.lmru.tech/airflow:1.10.12-2

# Become root to install requirements
USER root

ADD requirements.txt requirements.txt
ADD --chown=airflow:airflow src /src

RUN pip install -r requirements.txt \
    && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/* \
    /usr/share/man \
    /usr/share/doc \
    /usr/share/doc-base \
    ~/.cache/pip

RUN apt-get update && apt-get install -y wget

RUN mkdir -p ~/.clickhouse-client /usr/local/share/ca-certificates/Yandex && \
    wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" -O /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt && \
    wget "https://storage.yandexcloud.net/mdb/clickhouse-client.conf.example" -O ~/.clickhouse-client/config.xml

# Switch back to airflow user
USER airflow

# Set project name argument
# Example: PROJECT=mymod
ARG PROJECT=sales-fs

ADD dags /usr/local/airflow/dags/${PROJECT}

ENV PYTHONPATH=/src