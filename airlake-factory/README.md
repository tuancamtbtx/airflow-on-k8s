# Airlake Dag Factory
Dynamically build Airflow DAGs from YAML files

**Airlake Factory Tool**

![tool](../assets/dag_factory.png)

## Run Test Locally

**Must Have**
```
export PYTHONPATH=$PYTHONPATH:/airfactory
```
**Run Test**
```
make test
```
## Build Lib
```
make dist
```
## Usage This Lib
**Copy airfactory-1.0.0.tar.gz in dist folder to your project**

**Configure requirements.txt**
```
./packages/airfactory-1.0.0.tar.gz
```

## Benefits
- Construct DAGs without knowing Python
- Construct DAGs without learning Airflow primitives
- Avoid duplicative code
- Everyone loves YAML! ;)