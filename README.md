# Airflow For Data Engineer

[![Docker Pulls](https://badgen.net/docker/pulls/vantuan12345/airlake?icon=docker&label=pulls)](https://hub.docker.com/r/vantuan12345/airlake/)
[![Docker Stars](https://badgen.net/docker/stars/vantuan12345/spark-generator?icon=docker&label=stars)](https://hub.docker.com/r/vantuan12345/airlake/)
[![Docker Image Size](https://badgen.net/docker/size/vantuan12345/airlake?icon=docker&label=image%20size)](https://hub.docker.com/r/vantuan12345/airlake/)
![Github stars](https://badgen.net/github/stars/tuancamtbtx/airflow-example?icon=github&label=stars)
![Github forks](https://badgen.net/github/forks/tuancamtbtx/airflow-example?icon=github&label=forks)
![Github issues](https://img.shields.io/github/issues/tuancamtbtx/airflow-example)
![Github last-commit](https://img.shields.io/github/last-commit/tuancamtbtx/airflow-example)

## Airlake DataFactory
![factory](./assets/airlake-factory.gif)

```yaml
default_args:
  owner: nguyenvantuan140397@gmail.com
  start_date: 2024-06-28
schedule_interval: "0 2 * * *"
tasks:
  score_products:
    operator: BigQueryOperator
    sql: SELECT * FROM `p-dwh.dim.dim_product`
    destination_dataset_table: "p-dwh.dim.test_{{ macros.localtz.ds_nodash(ti) }}"

  update_realtime_cached:
    operator: BigqueryToCassandraOperator
    cassandra_dest_table: "dwh.dim_product"
    sql: |
      SELECT CAST(sku AS STRING) sku, product_fraud_score_new score
      FROM `p-dwh.dim.dim_product`
      WHERE sku IS NOT NULL
    dependencies: ["score_products"]

```
## Operator Supported:
| Operator        		     |
|----------------------------|
|	BigqueryToSheetOperator  |
|	BigQueryToGCSOperator	 | 
|SFTPGetMultipleFilesOperator|

## Airflow on k8s

![Airflow k8s design](./assets/arch-diag-kubernetes.png)

## Flow Design

![Flow design](./assets/airflow-flow.png)

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

# extra libs used in airlake
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

## Contributing
The project has a separate contribution file. Please adhere to the steps listed in the separate contributions [file](./CONTRIBUTING.md)


## License

This project is licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for more details.

## Contact:
- Email: nguyenvantuan140397@gmail.com
- Tele: Tuancamtbtx

## Note:

Please **DO NOT** edit code and data in this project

