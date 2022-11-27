# Cake Assessment Data Engineer
Test: “ Có 2 hệ thống SFTP gọi là A và B.

 

Thiết kế 1 pipeline chuyển dữ liệu các file mới đc sinh ra từ server SFTP A sang server SFTP B mà giữ nguyên cấu trúc thư mục.

Dữ liệu chỉ đi từ A -> B, dữ liệu xóa ở A thì ko đc xóa ở B.

Công nghê sử dụng : Airflow chạy trên nền Docker.

Ví dụ :

Ngày 2022/22/11 trên server A xuất hiện sftp_A://a/b/c/file_1.txt thì chuyển sang server B sftp_B://a/b/c/file_1.txt

Ngày 2022/23/11 trên server A xuất hiện sftp_A://a/b/c/file_2.txt thì chuyển sang server B sftp_B://a/b/c/file_2.txt

Ngày 2022/24/11 trên server A xuất hiện sftp_A://a/b/c/file_3.txt thì chuyển sang server B sftp_B://a/b/c/file_3.txt “


## Flow Design

![Flow design](./images/dags_flow.png)

## Setup 

```
	Install Docker vs Docker Compose
```
## How To Run

**Setup Env Var:**
```
	AIRFLOW_UID=50000
```
**Build docker with docker-compose**
```
	docker-compose build
```
**Run docker**
```
	docker-compose up
```
**Config Airlfow connection for SFTP Server A vs B**

| SFTP Server | connection id |  Host   | Port | User name  | Password  | Connection Type|
|-------------|---------------|---------|------|------------|-----------|----------------|
| `a`         | `sftp_a_conn` | `sftp_a`| 22   | `foo`      | `pass`    |     SFTP		 | 
| `b`         | `sftp_b_conn` | `sftp_b`| 22   | `foo`		| `pass`    |	  SFTP       |

**Fake data At SFTP Server:**

```
create file_1.txt and file_2.txt in folder ./tmp/sftp_a

```
![connection sftp](./images/airflow_connections.png)

**View Home Page**
![dags](./images/dags.png)

| Variable            | Default value |  Role                |
|---------------------|---------------|----------------------|
| `ARIFLOW USER`      | `airflow`     | admin 				 |
| `ARIFLOW_PASS`      | `airflow`     | admin				 |

**Run Dag**

**Result**

![sftp a](./images/sftp_a_folder.png)
![sftp b](./images/sftp_b_folder.png)

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

# extra libs used in airlock
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