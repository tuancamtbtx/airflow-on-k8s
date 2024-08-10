FROM apache/airflow:2.9.2-python3.9

USER root

# our codes
RUN mkdir -p /bigdata/airlake
COPY --chown=airflow:airflow . /bigdata
COPY requirements.txt /
COPY dags/ /opt/airflow/dags/
ENV PYTHONPATH=$PYTHONPATH:/bigdata/airlake

USER airflow
COPY --chown=airflow:airflow packages/* /opt/airflow/packages/
RUN pip install --no-cache-dir -r /requirements.txt

