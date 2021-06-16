from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator
)

PROJECT_ID = '<your GCP project>'
DATAPROC_CLUSTER_NAME = '<your Dataproc cluster>'
DATAPROC_REGION = '<your Dataproc clusters region>'
PYSPARK_URI = "gs://path/to/dedupe-session-metrics-etl.py"
INPUT_BUCKET = "gs://path/to/raw_session_metrics"
OUTPUT_BUCKET = "gs://path/to/session_metrics"

# Most notable configuration here is that this DAG will be triggered,
# so it doesn't need a cron-based schedule. The arg list for the dataproc
# job references dag_run.conf which is available when triggering the DAG
# via the REST API with the provided cloud function
with DAG(
    'dedupe-session-metrics',
    description='Deduplicates Dataflow event time / receipt time partitioned outputs',
    schedule_interval=None,
    default_args={'start_date': datetime.today()}
) as dag:

    arg_list = [
        f"--input_bucket={INPUT_BUCKET}",
        "--event_time_second={{ dag_run.conf['event_time_second'] }}",
        f"--output_bucket={OUTPUT_BUCKET}"
    ]

    PYSPARK_JOB = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": PYSPARK_URI, 'args': arg_list},
    }

    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", job=PYSPARK_JOB, location=DATAPROC_REGION, project_id=PROJECT_ID
    )