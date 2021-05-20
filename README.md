# Dagster Docker-based Dev Environment

This project is based on the dagster team example project located at [Dagster docker example](https://github.com/dagster-io/dagster/tree/0.11.9/examples/deploy_docker)

## Getting Started

1. Clone this repository
2. Open up a command line tool and navigate to the root folder of this project.
3. Run docker-compose up
4. browse 127.0.0.1:3000

## Writing your first pipeline/DAG

### DAG folder structure

    dags
      my_dag
        my_dag_1.py
        my_dag_2.py
        my_dag_n.py
        my_repo.py

### Writing a simple pipeline/DAG

Follow the [dagster tutorial](https://docs.dagster.io/tutorial/intro-tutorial/single-solid-pipeline#a-single-solid-pipeline) on how to write pipelines/DAGS and solids/tasks in dagster.

### Repository

[Repository](https://docs.dagster.io/concepts/repositories-workspaces/repositories#repositories) is used to organise pipelines, schedules, sensors etc.

> If a single pipeline/DAG can fullfil your needs then you do not need a separate `repo.py` file, but if you need to write multiple pipelines/DAGs that serves the same need then you will need a separate `repo.py` file

### Sample Repository

    from dagster import repository
    from cereal import cereal_pipeline, cereal_schedule

    @repository
    def dags_repo():
        return [cereal_pipeline, cereal_schedule]

### Loading your repositories into Dagster

This is done in [*workspace.yaml*](https://docs.dagster.io/concepts/repositories-workspaces/workspaces#workspaces), the code below is going to load our sample repo to Dagster.

    load_from:
      - python_file:
          relative_path: dags/cereal/repo.py
          working_directory: /opt/dagster/dagster_home/dags/cereal

## Using Dagster-azure

To use [dagster-azure](https://docs.dagster.io/_apidocs/libraries/dagster-azure#azure-dagster-azure) create a *.env* file in the root of this project and add the Azure data lake storage key as env variable as follows:

    AZURE_DATA_LAKE_STORAGE_KEY=yourAzureDataLakeStorageKey

for testing purposes use azure-test/azure-test pipeline, make sure you replace the `storage_account` name with your storage account name on line 38.

## Using Dagster_docker run launcher

By default this setup uses *dagster.core.launcher* you can optionally set it up to use *dagster_docker* launcher which uses gRPC server.
The gRPC server loads and executes pipelines, in both dagit and dagster-daemon. By setting *DAGSTER_CURRENT_IMAGE* to its own image, we tell the [run launcher](https://docs.dagster.io/deployment/run-launcher) to use the following image when launching runs in a new container as well. Multiple containers like this can be deployed separately - each just needs to run on its own port, and have its own entry in the [workspace.yaml](https://docs.dagster.io/concepts/repositories-workspaces/workspaces) file that's loaded by dagit. gRPC server is helpful when you want to run your pipelines in a different container.

### gRPC in docker-compose

    dagster_gRPC:
        build:
          context: .
          dockerfile: ./Dockerfile_gRPC
        container_name: dagster_gRPC
        image: dagster_gRPC
        environment:
          DAGSTER_POSTGRES_USER: "dagster"
          DAGSTER_POSTGRES_PASSWORD: "dagster"
          DAGSTER_POSTGRES_DB: "dagster"
          DAGSTER_CURRENT_IMAGE: "worker"
        networks:
          - dagster-network
        volumes:
          - "./dags:<path/to/your/pipelines/folder>"

### Run launcher config for gRPC

    run_launcher:
      module: dagster_docker
      class: DockerRunLauncher
      config:
        env_vars:
          - DAGSTER_POSTGRES_USER
          - DAGSTER_POSTGRES_PASSWORD
          - DAGSTER_POSTGRES_DB
        network: dagster-network

### Dockerfile for dagster_gRPC

Dagster libraries to run both dagit and the dagster-daemon. Does not
need to have access to any pipeline code.

    FROM python:3.7-slim

    RUN pip install \
        dagster \
        dagster-postgres \
        dagster-docker \
        dagster-azure \
        setuptools

Set $DAGSTER_HOME and copy dagster instance and workspace YAML there

    ENV DAGSTER_HOME=/opt/dagster/dagster_home/
    ARG WORK_DIR=/opt/dagster/dags

    RUN mkdir -p ${DAGSTER_HOME} ${WORK_DIR}

    COPY dagster.yaml workspace.yaml $DAGSTER_HOME

Add repository code

    WORKDIR ${WORK_DIR}

    COPY ./dags ${WORK_DIR}/dags

    RUN pip install -e ./dags

Run dagster gRPC server on port 4000

    EXPOSE 4000

CMD allows this to be overridden from run launchers or executors that want to run other commands against your repository

    CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-f", "dags/cereal/repo.py"]

### Workspace.yaml for gRPC

    load_from:
Each entry here corresponds to a service in the docker-compose file that exposes pipelines.

    - grpc_server:
        host: dagster_gRPC
        port: 4000
        location_name: "dags"

### Create Python Packages for your repos

In gRPC mode you will need to organize your [dags](https://docs.dagster.io/concepts/solids-pipelines/pipelines) and [repositories](https://docs.dagster.io/concepts/repositories-workspaces/repositories) as python packages under dags folder, such as below.

    dags
      my_dag
        __init__.py
        my_dag.py
        my_repo.py
      setup.py

