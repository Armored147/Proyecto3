import json
import boto3
from sodapy import Socrata
import pandas as pd
import pymysql


def create_bucket_if_not_exists(bucket_name):
    s3 = boto3.client('s3')
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"El bucket {bucket_name} ya existe.")
    except:
        try:
            s3.create_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} creado exitosamente.")
        except Exception as e:
            print(f"Error al crear el bucket {bucket_name}: {e}")

def main():
    # Configurar cliente de S3
    s3 = boto3.client('s3')

    # Crear el bucket si no existe
    bucket_name = 'project-3-tele'
    create_bucket_if_not_exists(bucket_name)

    # Cliente autenticado con credenciales de Socrata
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

    results_df = pd.DataFrame.from_records(results)

    # Guardar como CSV localmente
    csv_file = "csv_files/datos.csv"
    results_df.to_csv(csv_file, index=False)

    # Subir el archivo CSV a S3
    s3_key = 'api_csv/datos.csv'

    try:
        s3.upload_file(csv_file, bucket_name, s3_key)
        print(f"Archivo CSV subido exitosamente a s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error al subir el archivo CSV: {e}")

    # Subir el JSON directamente a S3
    s3_key = 'api_json/datos.json'

    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json_data,
            ContentType='application/json'
        )
        print(f"Datos subidos exitosamente a s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error al subir los datos: {e}")


    # Conectar a la base de datos MySQL en la instancia de EC2
    connection = pymysql.connect(
        host='52.201.84.225',  
        user='root',          
        password='strong_password',     
        database='bigdata'      
    )

    try:
        # Consulta SQL
        query = "SELECT * FROM mytable"
        df = pd.read_sql(query, connection)

        # Guardar los datos en un archivo JSON
        json_file = "files_json/datos.json"
        df.to_json(json_file, orient='records', lines=True)

        # Subir el archivo JSON a S3
        s3_key = 'sql/datos.json'
        try:
            s3.upload_file(json_file, bucket_name, s3_key)
            print(f"Archivo JSON subido exitosamente a s3://{bucket_name}/{s3_key}")
        except Exception as e:
            print(f"Error al subir el archivo JSON: {e}")

    finally:
        connection.close()

if __name__ == "__main__":
    main()
