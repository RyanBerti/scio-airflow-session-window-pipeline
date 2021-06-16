# scio-airflow-session-window-pipeline

Example architecture supporting late arriving data via Scio session windows and event-driven Airflow DAGs.

This repo contains a protobuf message definition, a Scio pipeline, an Airflow DAG, and a PySpark job. The sbt build
generates protobuf stubs for use by the Scio pipeline.

See [the associated Medium post](https://ryanbertiwork.medium.com/handling-late-arriving-data-with-apache-beam-and-apache-airflow-a2b310099a8e) 
for more info into the project's architecture and intent.

# Required Infrastructure

This architecture example utilizes the following Google Cloud infrastructure:
 * A PubSub topic+subscription which protobuf ExampleMessages will be published to 
 * A GS bucket with locations defined for
    * PySpark files
    * Raw Dataflow output
    * Deduplicated PySpark output
 * A PubSub topic where the Dataflow job will publish data availability events
 * A Composer instance which has the DAG from the python directory copied into its DAGs directory
 * A Cloud Function configured to read data availability events from the PubSub topic and trigger the DAG
 * A Dataproc cluster on which Airflow can drive PySpark jobs

# Deployment and Testing Instructions

### Update Infrastructure References
The following files in this repo need to be updated to point at your GCP project / infrastructure
 * src/test/scala/ryanberti/PubSubClientTest.scala
 * src/main/python/dedupe-session-metrics-dag.py
 
### Copy the PySpark job into your GCS bucket
Copy the 'dedupe-session-metrics-etl.py' file into the GS location you defined for PySpark files
 
### Deploy the Airflow DAG into your Composer environment
Copy the 'dedupe-session-metrics-dag.py' into the DAGs folder and validate it's available via the Airflow UI

### Create a Cloud Function
Utilize [these instructions](https://cloud.google.com/composer/docs/how-to/using/triggering-with-gcf) to enable 
Airflow's REST API, and then create a Cloud Function which triggers the 'dedupe-session-metrics' DAG
based on events in the PubSub topic containing data availability events. A modified version of the example trigger_dag
implementation can be found in this repo's python directory, which decodes the data from incoming PubSub messages and
passes that decoded object to Airflow.

### Deploy the SessionWindowPipeline
Setup your GCP credentials and start sbt. If you run with the DirectRunner, you may have some unexpected window
triggering applied due to the default trigger not being used. See src/main/scala/ryanberti/WindowOptionsUtils.scala
for triggering configuration for DirectRunner vs DataflowRunner.

```
runMain ryanberti.SessionWindowPipeline 
--input-subscription=<fully qualified PubSub subscription> 
--output-destination=gs://path/to/raw_session_metrics
--output-topic=<fully qualified PubSub topic>
--runner=DataflowRunner --region=<some region>
```

### Drive a PubSubClientTest Test
The "write messages within the same session with lateness" test adds enough time between events to showcase
the architecture's ability to handle late arriving data. Remove the 'ignore' flag and drive the test via sbt
or your IDE.


This project was originally based on the [scio.g8](https://github.com/spotify/scio.g8).
