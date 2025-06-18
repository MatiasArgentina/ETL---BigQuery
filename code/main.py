# PARA USAR EN LA CLOUD FUNCTION
# En requirements.txt
# functions-framework==3.*
# google-cloud-storage
# google-cloud-bigquery

import functions_framework
from google.cloud import bigquery

project_name = "poc-bq-vertex-450119"
bigquery_dataset = "test_ucema"


# CloudEvent trigger (para funciones 2nd gen con Eventarc + Cloud Storage)
@functions_framework.cloud_event
def mover_dataset_x(event):
    data = event.data
    file_name = data["name"]
    bucket_name = data["bucket"]

    print(f"Se detectó que se subió el archivo: {file_name}")
    source_uri = f"gs://{bucket_name}/{file_name}"
    table_name = file_name.split("_")[0]
    table_id = f"{project_name}.{bigquery_dataset}.{table_name}"

    client = bigquery.Client(project=project_name)
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
    )

    load_job = client.load_table_from_uri(source_uri, table_id, job_config=job_config)
    load_job.result()  # Espera a que termine

    print(f"✅ Se cargó la tabla '{table_name}' en BigQuery desde {source_uri}")
