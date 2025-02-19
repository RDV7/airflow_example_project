FROM apache/airflow:2.10.4

WORKDIR /opt/airflow/

COPY requirements.txt .

RUN python -m pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt

WORKDIR /opt/airflow/dist/

COPY /dist/ .

RUN python -m pip install example-0.1.0-py3-none-any.whl

WORKDIR /opt/airflow/



