import os
import boto3
from data_api import main as run_data_api

s3 = boto3.client('s3')

def delete_all_files_in_bucket(bucket_name):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            for obj in response['Contents']:
                s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
            print(f"Todos los archivos en el bucket {bucket_name} han sido eliminados.")
        else:
            print(f"No hay archivos en el bucket {bucket_name}.")
    except Exception as e:
        print(f"Error al eliminar archivos en el bucket {bucket_name}: {e}")

def delete_bucket(bucket_name):
    try:
        # Primero eliminamos todos los archivos en el bucket
        delete_all_files_in_bucket(bucket_name)
        
        # Luego eliminamos el bucket
        s3.delete_bucket(Bucket=bucket_name)
        print(f"El bucket {bucket_name} ha sido eliminado.")
    except Exception as e:
        print(f"Error al eliminar el bucket {bucket_name}: {e}")

# Ejemplo de uso
if __name__ == "__main__":
    run_data_api()
    # delete_all_files_in_bucket('project-3-tele')
    # delete_bucket('project-3-tele')
    