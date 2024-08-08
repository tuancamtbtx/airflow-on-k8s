FROM vantuan12345/airfactory:airfactory:1.0.0_airflow2.9.2

USER airflow

# our codes
RUN mkdir -p /bigdata/airlake
COPY --chown=airflow:airflow . /bigdata
COPY requirements.txt /
COPY dags/ /opt/airflow/dags/
ENV PYTHONPATH=$PYTHONPATH:/bigdata/airlake

USER airflow
COPY --chown=airflow:airflow packages/* /opt/airflow/packages/
RUN pip install --no-cache-dir -r /requirements.txt

ENTRYPOINT ["/tini", "--"]
