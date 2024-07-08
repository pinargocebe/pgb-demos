# Developer handbook
1. [ Prerequisite ](#prerequisite)
1. [ How to work on your local ](#how-to-work-on-your-local)
1. [ Useful Commands ](#useful-commands)
1. [ How to dockerize application ](#how-to-dockerize-application)
1. [ Application design ](#application-design)
1. [ Application scripts ](#application-scripts)

## Prerequisite
- python 3.11
- java 17
- docker
- docker-compose

## How to work on your local

Run the following commands under the project folder:
```
cd yelp_data_analysis_demo
```
1. Install python3:

    For Linux
    ```
    sudo add-apt-repository ppa:deadsnakes/ppa
    sudo apt-get update
    sudo apt-get install python3.11 python3.11-dev python3.11-venv
    ```

    For OSX
    ```
    brew install python@3.11
    ```

    create alias:
    ```
    alias python=python3.11
    ```

2. Create virtual environment for local development:

    ```
    python -m venv $CUSTOM_DIRECTORY
    ```

3. Activate virtual environment and install dependencies.

    ```
    source $CUSTOM_DIRECTORY/bin/activate
    make install_dev
    ```

4. Create .env file under the current directory according to your settings. Ex:
    ```
    LOG_LEVEL=INFO
    MASTER_NODE_URL=local
    INPUT_PATH=input/yelp_dataset/ # contains input json files
    BRONZE_PATH=output/raw_data/yelp/
    SILVER_PATH=output/processed_data/yelp/
    GOLD_PATH=output/results/yelp/
    SCHEMA_PATH=schema/
    ```

5. Run 
    ```
    export PYTHONPATH=$PYTHONPATH:$(pwd)/app && spark-submit --packages io.delta:delta-spark_2.12:3.2.0 app/data_processor.py
    ```

6. When execution is finished, you should see the output of the application under the directory that you provided in 'GOLD_PATH' env variable.

## Useful Commands
* Run unit tests
    ```
    make run_tests
    ```

* Format code
    ```
    make format_code
    ```

* Run style checks for the code
    ```
    make check_code_style
    ```

## How to dockerize application

Run the following commands under the project folder:

```
cd yelp_data_analysis_demo/docker
```

1. Build docker image for the app and initialize containers by running following command:

    ```
    make run-d
    ```

    You should see the following containers in the output of **'docker ps'** command: **'docker-spark-worker-1'**, **'da-spark-history'** and **'da-spark-master'**. Also you can access spark  history ui from http://localhost:18080

2. Run application:
    ```
    make submit app=app/data_processor.py
    ```

3. When execution is finished, you should see the output of the application under the **'yelp_data_analysis_demo/output/'** directory.

Note: You could check [Makefile](docker/Makefile) for other docker build/run options. For example: If required, you can run multi-worker spark cluster via executing: 'make run-scaled'

## Application design
This section of the document summarizes the application architecture and design decisions for the data lake demo application by using the yelp test dataset.

Note: When designing the application, it was assumed that new data would arrive daily.

In order to process the data efficiently and organize it logically by improving the data quality and app performance Mediallion Architecture has been adopted in this demo. In the following you could find the implementation details for each layer:

### Bronze layer
In this layer data stored as it is and parquet format has been used to efficiently store the raw data. Incoming raw data stored into the year=YYYY/month=MM/day=DD by using the processing date in order to historically store the data.

### Silver layer
The silver layer comprises validated data, prepared for further analysis. Delta format has been used to prepare the latest snapshot of the data for further anlysis. Data for the each entity have been validated against the defined schema, dropped the duplicate data, and type conversion has been done if necessary. Also compaction and z-ordering have been done to improve query performance.

### Gold layer
Gold layer contains the aggreagated data. Data from the Silver layer is transformed into high-value data products with a structure that are ready to use by business users for reporting purposes. Delta format has been used to efficiently store tha data in this layer.

### Application scripts
- **data_processor.py**: This python script is the main entrpoint for the application and responsible to run all the workflow in order to prepare bronze, silver and gold layers.

- **transformers.py**: Transformers python script follows the abstract factory pattern and contains the entity specific transformation classes for each entity and a factory class to get transformer instance for the specific entity. All the data cleanup, transformation and schema validations have been done in this script.

- **schema.py**: Responsible to prepare spark schema for each entity by using the provided json schema files.

- **logger.py**: Logger class for the application

- **exception.py**: Custom exception class for the application.
