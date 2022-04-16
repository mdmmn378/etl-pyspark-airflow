export AIRFLOW_HOME=/home/mamun/projects/synced/credit-book-assignment/airflow

AIRFLOW_VERSION=2.2.5
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

airflow standalone