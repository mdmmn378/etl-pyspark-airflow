import datetime
import time
from random import random

import pendulum
import pytest

from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

DATA_INTERVAL_START = pendulum.datetime(2022, 4, 18, tz="UTC")
DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=1)

TEST_DAG_ID = "creditbook_etl_dag"
TEST_TASK_ID = "extract"


from airflow.models import DagBag
from dags.etl import creditbook_etl_dag


@pytest.fixture()
def dagbag():
    return DagBag()


def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id=TEST_DAG_ID)
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 3


@pytest.mark.dag_test
def test_creditbook_etl_dag():
    dag = creditbook_etl_dag()
    dagrun = dag.create_dagrun(
        run_id=str(time.time()),
        state=DagRunState.RUNNING,
        execution_date=None,
        start_date=DATA_INTERVAL_START,
        external_trigger=False,
        conf=None,
        run_type=DagRunType.MANUAL,
    )
    ti = dagrun.get_task_instance(task_id=TEST_TASK_ID)
    ti.task = dag.get_task(task_id=TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
