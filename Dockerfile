FROM apache/airflow:2.9.2-python3.9 

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends git

# COPY requirements.txt constraints.txt scripts/deps /tmp/work/
# RUN /tmp/work/deps /tmp/work && rm -rf /tmp/work

# our codes
RUN mkdir -p /bigdata/airlake
COPY --chown=airflow:airflow . /bigdata
COPY requirements.txt /
COPY dags/ /opt/airflow/dags/
ENV PYTHONPATH=$PYTHONPATH:/bigdata/airlake

USER airflow
COPY --chown=airflow:airflow packages/* /opt/airflow/packages/
RUN pip install --no-cache-dir -r /requirements.txt
