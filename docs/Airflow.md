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
username/password `user`/`bitnami123`.

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


### Example DAG

This is an example DAG that can be used for testing installation. Just copy the code in a python file under
[airflow/dags/mattermost_dags](../airflow/dags/mattermost_dags).

```python
import pendulum

from airflow.decorators import dag, task
@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def example_kubernetes_dag():
    """
    ### Example kubernetes operator DAG.
    This is a simple data pipeline example which demonstrates the use of
    the TaskFlow API together with Kubernetes operator. Uses the k3s cluster deployed using docker
    compose.

    Based on TaskFlow API examples available
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html).
    """

    @task.kubernetes(
        image="python:3.8-slim-buster",
        name="k8s_test",
        namespace="default",
        in_cluster=False,
        config_file="/opt/kube/airflow-kube.yaml",
    )
    def execute_in_k8s_pod():
        import time

        print("Hello from k8s pod")
        time.sleep(2)

    @task.kubernetes(image="python:3.8-slim-buster", namespace="default", in_cluster=False, config_file="/opt/kube/airflow-kube.yaml",)
    def print_pattern():
        n = 5
        for i in range(n):
            # inner loop to handle number of columns
            # values changing acc. to outer loop
            for j in range(i + 1):
                # printing stars
                print("* ", end="")

            # ending line after each row
            print("\r")

    execute_in_k8s_pod_instance = execute_in_k8s_pod()
    print_pattern_instance = print_pattern()

example_kubernetes_dag()
```

### Know limitations

Tasks need to define the following configuration while running the tasks locally:
- `in_cluster` to `False` and
- `config_file``/opt/kube/airflow-kube.yaml`.

This is not required if airflow is running inside a Kubernetes cluster.