FROM apache/airflow:2.8.4-python3.9 AS base 
USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /bigdata/airlake
COPY --chown=airflow:airflow --from=base . /bigdata
COPY packages/* /packages
COPY requirements.txt /

ENV PYTHONPATH=$PYTHONPATH:/bigdata

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
