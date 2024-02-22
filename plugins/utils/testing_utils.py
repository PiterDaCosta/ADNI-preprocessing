"""
ADNI Preprocessing testing utility functions.

This module contains helper functions to be used in the context of testing
the ADNI preprocess project operators and dags.

Author: Pedro da Costa Porto (pedrodacosta@gmail.com)
Date: 2024-02-22
"""
import pendulum
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType


def dagrun_setup(dag, task_id):
    """
    Set up a DAGRun for a specified task within a DAG.

    This function creates a DAGRun with a specified state, execution date,
        start date,
    and run type. It then retrieves the task instance for the specified task
        within the DAGRun and returns the result of executing the task.

    Parameters:
        dag (DAG): The Apache Airflow DAG instance.
        task_id (str): The task ID for which to set up the DAGRun.

    Returns:
        Any: The result of executing the specified task.
    """
    execution_date = pendulum.datetime(
        2022,
        3,
        4,
        tz='America/Toronto'
    )
    dagrun = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=execution_date,
        start_date=execution_date,
        run_type=DagRunType.MANUAL
    )
    ti = dagrun.get_task_instance(task_id=task_id)
    ti.task = dag.get_task(task_id=task_id)
    return ti.task.execute(ti.get_template_context())
