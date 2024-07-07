# Developer handbook
1. [ How to work on your local ](#how-to-work-on-your-local)
1. [ Useful Commands ](#useful-commands)

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

4. Run 

## Useful Commands
* Run unit tests
    ```
    make run_tests
    ```

* Format code
    ```
    make format_code
    ```

* Run style checks
    ```
    make check_code_style
    ```



tests
package app as docker or multiple
readme: short explanation of design desicion

assumed daily data
brew install --cask oracle-jdk@17
spark-submit --packages io.delta:delta-spark_2.12:3.2.0  app/data_processor.py