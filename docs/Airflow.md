# Airflow development

A small development environment for airflow. Follow the instructions to start a local airflow environment. As soon as 
the local airflow starts, any changes inside `dags` or `plugins` directory will be automatically reflected (no need to
restart!).

> Note that this version starts a local environment using `CeleryExecutor`. It should be enough for developing and testing 
> custom operators. Unfortunately any DAG that is using `KubernetesPodOperator` won't run successfully. This shall be
> improved in the future.

## Local airflow environment

### Requirements

- docker & docker compose

### Starting airflow

At the root of the project run 

```bash
docker compose -f docker-compose-airflow.yml up
```

After a while you can access airflow at [http://localhost:8080](http://localhost:8080), with username/password `user`/`bitnami`.