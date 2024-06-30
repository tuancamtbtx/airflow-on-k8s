# Airflow Data Engineer

[![Docker Pulls](https://badgen.net/docker/pulls/vantuan12345/airlake?icon=docker&label=pulls)](https://hub.docker.com/r/vantuan12345/airlake/)
[![Docker Stars](https://badgen.net/docker/stars/vantuan12345/spark-generator?icon=docker&label=stars)](https://hub.docker.com/r/vantuan12345/airlake/)
[![Docker Image Size](https://badgen.net/docker/size/vantuan12345/airlake?icon=docker&label=image%20size)](https://hub.docker.com/r/vantuan12345/airlake/)
![Github stars](https://badgen.net/github/stars/tuancamtbtx/airflow-example?icon=github&label=stars)
![Github forks](https://badgen.net/github/forks/tuancamtbtx/airflow-example?icon=github&label=forks)
![Github issues](https://img.shields.io/github/issues/tuancamtbtx/airflow-example)
![Github last-commit](https://img.shields.io/github/last-commit/tuancamtbtx/airflow-example)

## Operator Supported:


## Flow Design

![Flow design](./images/dags_flow.png)

## Setup 

```
	Install Docker vs Docker Compose
```
## How To Run

**Setup Env Var:**
```
	export AIRFLOW_UID=50000
```
**Build docker with docker-compose**
```
	docker-compose build
```
**Run docker**
```
	docker-compose up
```
**View Home Page**

| Variable            | Default value |  Role                |
|---------------------|---------------|----------------------|
| `ARIFLOW USER`      | `airflow`     | admin 				 |
| `ARIFLOW_PASS`      | `airflow`     | admin				 |


## Build Docker
  **Edit image name**

  **Edit image version**

  **Edit docker username**

```
	- bash build_docker.sh
```

## Development (For Me)

Install python3.8 with virtualenv first

```bash
# in virtualenv
# install core airflow
python3 -m pip install -r requirements_airflow.txt --constraint ./constraints.txt --use-deprecated=legacy-resolver
# spark
python3 -m pip install -r ./requirements_nodeps.txt --constrain ./constraints.txt --no-deps --use-deprecated=legacy-resolver

# extra libs used in aircake
python3 -m pip install -r requirements.txt --constraint ./constraints.txt --use-deprecated=legacy-resolver
```
**Setup Local Airflow**

*setup database*
- `export AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@127.0.0.1:5432/airflow`
```bash
airflow db init
```

**Scheduler**
```bash
airflow scheduler
```

**WebServer**
```bash
airflow webserver
```

## Contact:
- Email: nguyenvantuan140397@gmail.com
- Tele: Tuancamtbtx

## Note:

Please **DO NOT** edit code and data in this project
