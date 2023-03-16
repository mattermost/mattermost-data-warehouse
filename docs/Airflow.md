# Airflow development

A small development environment for airflow. Follow the instructions to start a local airflow environment. As soon as 
the local airflow starts, any changes inside `dags` or `plugins` directory will be automatically reflected (no need to
restart!).


## Local airflow environment

### Requirements

- docker & docker compose

### Starting airflow

At the root of the project run 

```bash
docker compose -f build/docker-compose.airflow.yml up
```

After a while you can access airflow at [http://localhost:8080](http://localhost:8080), with 
username/password `user`/`bitnami`.

### Configuring secrets

Create a secret with name `airflow`:

```shell
kubectx --kubeconfig=./build/kubeconfig.yaml create secret generic airflow
```

> Secret only needs to be created once.

To add values to the secret run:

```shell
kubectl --kubeconfig=./build/kubeconfig.yaml edit secret airflow   
```

Specify the values you'd like to add. More details on how to manage secrets is 
[available here](https://kubernetes.io/docs/tasks/configmap-secret/managing-secret-using-kubectl/).
