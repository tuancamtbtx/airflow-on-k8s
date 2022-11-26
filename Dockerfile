FROM python:3.8-slim-buster
RUN pip install pytest-runner

ARG AIRFLOW_USER_HOME=/usr/local/airflow
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

RUN apt-get update\
    && apt-get install -y --no-install-recommends htop git default-libmysqlclient-dev libsnappy-dev liblzma-dev patch curl default-mysql-client\
    && apt-get autoremove -yqq --purge \
    && curl -fsSLo /usr/bin/kubectl "https://dl.k8s.io/release/v1.20.4/bin/linux/amd64/kubectl"\
    && chmod +x /usr/bin/kubectl\
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# set default timezone
RUN rm -f /etc/localtime && ln -s /usr/share/zoneinfo/Asia/Ho_Chi_Minh /etc/localtime

RUN adduser airflow

COPY ./requirements_airflow.txt  constraints.txt /tmp/

ARG DEV_APT_DEPS="\
    build-essential"
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    ${DEV_APT_DEPS} \
    ${ADDITIONAL_DEV_APT_DEPS} \
    && pip install --upgrade pip \
    && pip install --use-deprecated=legacy-resolver --no-cache-dir -r /tmp/requirements_airflow.txt --constraint "/tmp/constraints.txt" \
    && rm -f /tmp/requirements_airflow.txt /tmp/constraints.txt\
    && apt-get purge ${DEV_APT_DEPS} -y \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*



COPY ./entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg

RUN chown -R airflow: ${AIRFLOW_USER_HOME}


RUN chmod +x /entrypoint.sh

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]

# FROM python:3.8-slim-buster
# LABEL maintainer="ednarb29"

# # Never prompt the user for choices on installation/configuration of packages
# ENV DEBIAN_FRONTEND noninteractive
# ENV TERM linux

# # Airflow
# ARG AIRFLOW_VERSION=2.2.3
# ARG AIRFLOW_USER_HOME=/usr/local/airflow
# ARG AIRFLOW_DEPS=""
# ARG PYTHON_DEPS="wtforms==2.3.3"
# ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

# # Define en_US.
# ENV LANGUAGE en_US.UTF-8
# ENV LANG en_US.UTF-8
# ENV LC_ALL en_US.UTF-8
# ENV LC_CTYPE en_US.UTF-8
# ENV LC_MESSAGES en_US.UTF-8

# # Disable noisy "Handling signal" log messages:
# # ENV GUNICORN_CMD_ARGS --log-level WARNING

# COPY ./requirements_airflow.txt  constraints.txt /tmp/

# RUN set -ex \
#     && buildDeps=' \
#         freetds-dev \
#         libkrb5-dev \
#         libsasl2-dev \
#         libssl-dev \
#         libffi-dev \
#         libpq-dev \
#         git \
#     ' \
#     && apt-get update -yqq \
#     && apt-get upgrade -yqq \
#     && apt-get install -yqq --no-install-recommends \
#         $buildDeps \
#         freetds-bin \
#         build-essential \
#         default-libmysqlclient-dev \
#         apt-utils \
#         curl \
#         rsync \
#         netcat \
#         locales \
#     && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
#     && locale-gen \
#     && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
#     && useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow \
#     && pip install -U pip setuptools wheel \
#     && pip install pytz \
#     && pip install pyOpenSSL \
#     && pip install ndg-httpsclient \
#     && pip install pyasn1 \
#     && pip install apache-airflow[crypto,celery,postgres,hive,jdbc,mysql,ssh,redis${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION} \
#     && pip install --use-deprecated=legacy-resolver --no-cache-dir -r /tmp/requirements_airflow.txt --constraint "/tmp/constraints.txt" \
#     && if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi \
#     && apt-get purge --auto-remove -yqq $buildDeps \
#     && apt-get autoremove -yqq --purge \
#     && apt-get clean \
#     && rm -rf \
#         /var/lib/apt/lists/* \
#         /tmp/* \
#         /var/tmp/* \
#         /usr/share/man \
#         /usr/share/doc \
#         /usr/share/doc-base

# COPY script/entrypoint.sh /entrypoint.sh
# COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg

# RUN chown -R airflow: ${AIRFLOW_USER_HOME}
# RUN chmod +x /entrypoint.sh

# EXPOSE 8080 5555 8793

# USER airflow
# WORKDIR ${AIRFLOW_USER_HOME}
# ENTRYPOINT ["/entrypoint.sh"]
# CMD ["webserver"]