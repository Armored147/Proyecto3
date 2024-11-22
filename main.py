import time
from google.cloud import dataproc_v1, storage

# Configura tus credenciales y parámetros de proyecto
PROJECT_ID = "bigdata-442414"
REGION = "us-central1"
CLUSTER_NAME = "etl-cluster"
BUCKET_NAME = "project3-tele"
GCS_ETL_SCRIPT = f"gs://{BUCKET_NAME}/etl_script.py"
ZONE = "us-central1-a"

def create_bucket_and_folder(bucket_name, folder_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Verificar si el bucket existe, y crearlo si no existe
    if not bucket.exists():
        print(f"El bucket {bucket_name} no existe. Creándolo...")
        try:
            bucket = client.create_bucket(bucket_name)
            print(f"Bucket {bucket_name} creado exitosamente.")
        except Exception as e:
            print(f"Error al crear el bucket {bucket_name}: {e}")
            raise
    else:
        print(f"El bucket {bucket_name} ya existe.")

    # Verificar si la carpeta existe (como prefijo en GCS)
    blob = bucket.blob(f"{folder_name}/")
    if not blob.exists():
        print(f"La carpeta {folder_name} no existe. Creándola...")
        try:
            blob.upload_from_string("")  # Crear un "objeto" vacío para representar la carpeta
            print(f"Carpeta {folder_name} creada exitosamente en {bucket_name}.")
        except Exception as e:
            print(f"Error al crear la carpeta {folder_name}: {e}")
            raise
    else:
        print(f"La carpeta {folder_name} ya existe en {bucket_name}.")

def create_cluster(dataproc, project_id, region, cluster_name):
    print("Creando clúster de Dataproc...")
    cluster_config = {
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "n1-standard-2",
        },
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "n1-standard-2",
        },
        "config_bucket": BUCKET_NAME,
        "temp_bucket": BUCKET_NAME,
        "gce_cluster_config": {
            "subnetwork_uri": "default",
            "internal_ip_only": False  # Cambiar de True a False
        },
    }

    cluster = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": cluster_config,
    }

    operation = dataproc.create_cluster(project_id=project_id, region=region, cluster=cluster)
    operation.result()
    print(f"Clúster {cluster_name} creado con éxito.")

def delete_cluster(dataproc, project_id, region, cluster_name):
    print("Eliminando clúster de Dataproc...")
    operation = dataproc.delete_cluster(project_id=project_id, region=region, cluster_name=cluster_name)
    operation.result()
    print(f"Clúster {cluster_name} eliminado con éxito.")

def submit_pyspark_job(dataproc, project_id, region, cluster_name, script_uri):
    print("Enviando trabajo PySpark al clúster...")
    job_details = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {"main_python_file_uri": script_uri},
    }

    result = dataproc.submit_job(project_id=project_id, region=region, job=job_details)
    job_id = result.reference.job_id
    print(f"Trabajo {job_id} enviado con éxito.")
    
    # Esperar a que el trabajo finalice
    timeout = 900  # 15 minutos
    start_time = time.time()
    while time.time() - start_time < timeout:
        job_status = dataproc.get_job(project_id=project_id, region=region, job_id=job_id).status.state
        if job_status in (dataproc_v1.types.JobStatus.State.DONE, dataproc_v1.types.JobStatus.State.ERROR):
            print(f"Estado del trabajo: {job_status.name}")
            break
        time.sleep(10)

def upload_etl_script_to_gcs(bucket_name, script_path, destination_blob_name):
    print(f"Subiendo script ETL a GCS: {destination_blob_name}")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(script_path)
    print(f"Script {script_path} subido exitosamente a {destination_blob_name}.")

if __name__ == "__main__":
    # Inicializa el cliente de Dataproc
    dataproc_client = dataproc_v1.ClusterControllerClient(client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"})
    job_client = dataproc_v1.JobControllerClient(client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"})

    try:
        # Ruta al script ETL local
        LOCAL_ETL_SCRIPT = "etl_script.py"

        # Verificar o crear el bucket y la carpeta "trusted"
        create_bucket_and_folder(BUCKET_NAME, "trusted")
        
        # Subir script ETL a GCS
        upload_etl_script_to_gcs(BUCKET_NAME, LOCAL_ETL_SCRIPT, "etl_script.py")

        # Crear el clúster
        create_cluster(dataproc_client, PROJECT_ID, REGION, CLUSTER_NAME)

        # Enviar el trabajo PySpark al clúster
        submit_pyspark_job(job_client, PROJECT_ID, REGION, CLUSTER_NAME, GCS_ETL_SCRIPT)
    
    except Exception as e:
        print(f"Error durante el proceso: {e}")
    
    finally:
        # Eliminar el clúster
        delete_cluster(dataproc_client, PROJECT_ID, REGION, CLUSTER_NAME)

