from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from pendulum import datetime

TASK = "submit_mathias_capstone_job"
JOB = "Mathias-capstone-job"
QUEUE = "academy-capstone-winter-2024-job-queue"
DEF = "Mathias-capstone-batch"

START = datetime(year=2024, month=2, day=9, tz="Europe/Brussels")

with DAG(
    dag_id="run_container",
    description="Run container that executes pipeline",
    default_args={"owner": "Mathias"},
    schedule_interval="0 0 9 2 *",
    start_date=START,
) as dag:
    submit_batch_job = BatchOperator(
    task_id=TASK,
    job_name=JOB,
    job_queue=QUEUE,
    job_definition=DEF,
)