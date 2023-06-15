#PARA USAR EN LA CLOUD FUNCTION
from google.cloud import bigquery
from google.cloud import storage

project_name = 'fantina-v1'
bigquery_dataset = 'Test'

def mover_dataset_x(event, context):
    file = event
    file_name = file['name']
    table_name = file_name.split('_')[0]
    print("Se detecto que se subio el archivo: " + file_name)
    source_uri = "gs://" + file['bucket'] + "/" + file_name

    client = bigquery.Client(project=project_name)
    table_id = project_name + "." + bigquery_dataset + "." + table_name

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
    )

    load_job = client.load_table_from_uri(
        source_uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    #destination_table = client.get_table(table_id)  # Make an API request.
    print("Se cargo la tabla: " + table_name + " en BigQuery")