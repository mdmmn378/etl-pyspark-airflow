import datetime
import pendulum

import pytest

from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

DATA_INTERVAL_START = pendulum.datetime(2021, 9, 13, tz="UTC")
DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=1)

TEST_DAG_ID = "creditbook_etl_dag"
TEST_TASK_ID = "extract"


from dags.etl import creditbook_etl_dag


from airflow.models import DagBag


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
        run_id="test_run",
        state=DagRunState.RUNNING,
        execution_date=DATA_INTERVAL_START,
        start_date=DATA_INTERVAL_START,
        external_trigger=False,
        conf=None,
        run_type=DagRunType.MANUAL,
    )
    ti = dagrun.get_task_instance(task_id=TEST_TASK_ID)
    ti.task = dag.get_task(task_id=TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == "success"
    # # Assert something related to task
    # assert dag.dag_id == TEST_DAG_ID
    # assert dag.tasks[0].task_id == TEST_TASK_ID
    # assert dag.tasks[0].start_date == DATA_INTERVAL_START
    # assert dag.tasks[0].end_date == DATA_INTERVAL_END
    # assert dag.tasks[0].catchup is False
    # assert dag.tasks[0].tags == ["etl"]
