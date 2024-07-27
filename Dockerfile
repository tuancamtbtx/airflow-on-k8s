FROM apache/airflow:2.9.2-python3.9 

USER root

RUN apt-get update \
    && apt-get autoremove -yqq --purge \
    && apt-get install -y --no-install-recommends git \
    && apt-get install -y --no-install-recommends htop git default-libmysqlclient-dev libsnappy-dev liblzma-dev patch curl default-mysql-client\
    && curl -fsSLo /usr/bin/kubectl "https://dl.k8s.io/release/v1.20.4/bin/linux/amd64/kubectl"\
    && chmod +x /usr/bin/kubectl\
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# set default timezone
RUN rm -f /etc/localtime && ln -s /usr/share/zoneinfo/Asia/Ho_Chi_Minh /etc/localtime

RUN adduser airflow

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

ENTRYPOINT ["/tini", "--"]
