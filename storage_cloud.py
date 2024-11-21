import os
from google.cloud import storage
import json
import pandas as pd
import json
import boto3
from sodapy import Socrata
import pandas as pd
import pymysql


# Configura la autenticación con GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bigdata-442414-9e618bb22c7b.json"

def create_bucket_if_not_exists(bucket_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    if not bucket.exists():
        try:
            client.create_bucket(bucket_name)
            print(f"Bucket {bucket_name} creado exitosamente.")
        except Exception as e:
            print(f"Error al crear el bucket {bucket_name}: {e}")
    else:
        print(f"El bucket {bucket_name} ya existe.")

def upload_to_gcs(bucket_name, file_path, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    try:
        blob.upload_from_filename(file_path)
        print(f"Archivo subido exitosamente a gs://{bucket_name}/{blob_name}")
    except Exception as e:
        print(f"Error al subir el archivo: {e}")

def upload_json_to_gcs(bucket_name, json_data, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    try:
        blob.upload_from_string(json_data, content_type='application/json')
        print(f"Datos JSON subidos exitosamente a gs://{bucket_name}/{blob_name}")
    except Exception as e:
        print(f"Error al subir los datos JSON: {e}")

def main():
    bucket_name = 'project3-tele'

    # Crear el bucket si no existe
    create_bucket_if_not_exists(bucket_name)

    client = Socrata(
        "www.datos.gov.co",          
        "7iDXtNwmFas6LQi9280f7HUN2",
        username="vd16148@gmail.com",
        password="J8@fLp2#Wz!RmX9"
    )

    # Obtener datos del conjunto de datos 'gt2j-8ykr'
    results = client.get("gt2j-8ykr", limit=10000)

    # Convertir los resultados a JSON (si no es ya un JSON string)
    json_data = json.dumps(results)


    # subir el json
    upload_json_to_gcs(bucket_name, json_data, "data/datos.json")


    # Convertir csv y subirlo
    results_df = pd.DataFrame.from_records(results)

    # Guardar como CSV localmente
    csv_file = "csv_files/datos.csv"
    results_df.to_csv(csv_file, index=False)

    # Subir CSV a GCS
    upload_to_gcs(bucket_name, csv_file, "data/datos.csv")

    # Conectar a la base de datos Cloud SQL en GCP
    connection = pymysql.connect(
        host='34.134.247.64',  # Reemplaza con la IP de tu instancia de Cloud SQL
        user='tele',      # Reemplaza con tu nombre de usuario
        password='SjGqgJSRhfUI-dqM',  # Reemplaza con tu contraseña
        database='bigdata'   # Reemplaza con el nombre de tu base de datos
    )

    try:
        # Consulta SQL
        query = "SELECT * FROM covid_casos"  # Reemplaza con tu consulta SQL
        df = pd.read_sql(query, connection)

        # Convertir los datos a JSON
        json_data = df.to_json(orient='records')

        # Subir el JSON a GCS
        upload_json_to_gcs(bucket_name, json_data, "data/datos_mysql.json")
    except Exception as e:
        print(f"Error al extraer o subir los datos: {e}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()