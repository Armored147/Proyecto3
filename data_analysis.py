from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum
from google.cloud import bigquery

def create_bigquery_dataset(dataset_name):
    client = bigquery.Client()

    # Verificar si el dataset ya existe
    dataset_ref = client.dataset(dataset_name)
    try:
        client.get_dataset(dataset_ref)
        print(f"El dataset {dataset_name} ya existe.")
    except Exception:
        # Crear el dataset si no existe
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "us-central1"  # Asegúrate de usar la misma región que Dataproc
        client.create_dataset(dataset)
        print(f"Dataset {dataset_name} creado exitosamente.")

def upload_to_bigquery(bucket_name, refined_folder, dataset_name):
    client = bigquery.Client()
    tables = ["casos_por_departamento", "edad_promedio_por_departamento", "estado_por_departamento"]

    for table in tables:
        # Ajustar la ruta para incluir "analysis_results"
        source_uri = f"gs://{bucket_name}/{refined_folder}/analysis_results/{table}/*.parquet"
        table_id = f"{dataset_name}.{table}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True,
        )

        try:
            print(f"Cargando datos de {table} a BigQuery en {table_id}...")
            load_job = client.load_table_from_uri(source_uri, table_id, job_config=job_config)
            load_job.result()  # Esperar a que se complete
            print(f"Tabla {table_id} cargada exitosamente.")
        except Exception as e:
            print(f"Error cargando la tabla {table} a BigQuery: {e}")


def perform_data_analysis(bucket_name, trusted_folder, refined_folder, dataset_name):
    # Crear sesión de Spark
    spark = SparkSession.builder.appName("Data Analysis").getOrCreate()

    # Definir rutas
    input_path = f"gs://{bucket_name}/{trusted_folder}/datos_combinados.csv/*"
    output_path = f"gs://{bucket_name}/{refined_folder}/analysis_results"

    print(f"Leyendo datos desde {input_path}...")

    # Cargar datos desde Trusted
    df = spark.read.option("header", True).csv(input_path)

    # Eliminar encabezados repetidos y filas no válidas
    df = df.filter(
        (col("id_de_caso").isNotNull()) & (col("id_de_caso") != "id_de_caso")
    )

    # Convertir la columna "edad" a numérico (int) y manejar valores no válidos
    df = df.withColumn(
        "edad",
        when((col("edad") == "0") | (col("edad").isNull()), None).otherwise(
            col("edad").cast("int")
        ),
    )

    # Consultas descriptivas
    print("Realizando consultas descriptivas...")

    # Consulta 1: Total de casos por departamento
    casos_por_departamento = df.groupBy("nombre_departamento").count() \
        .withColumnRenamed("count", "total_casos")

    # Consulta 2: Edad promedio por departamento
    edad_promedio_por_departamento = df.groupBy("nombre_departamento") \
        .avg("edad") \
        .withColumnRenamed("avg(edad)", "edad_promedio")

    # Consulta 3: Total de fallecidos y recuperados por departamento
    estado_por_departamento = df.groupBy("nombre_departamento").agg(
        sum(when(col("estado") == "Fallecido", 1).otherwise(0)).alias("total_fallecidos"),
        sum(when(col("estado") == "Recuperado", 1).otherwise(0)).alias("total_recuperados")
    )

    # Guardar resultados en Refined
    print(f"Guardando resultados en {output_path}...")

    casos_por_departamento.write.mode("overwrite").parquet(f"{output_path}/casos_por_departamento")
    edad_promedio_por_departamento.write.mode("overwrite").parquet(f"{output_path}/edad_promedio_por_departamento")
    estado_por_departamento.write.mode("overwrite").parquet(f"{output_path}/estado_por_departamento")

    print("Consultas completadas y resultados guardados.")

    # Subir resultados a BigQuery
    upload_to_bigquery(bucket_name, refined_folder, dataset_name)

if __name__ == "__main__":
    BUCKET_NAME = "project3-tele"
    TRUSTED_FOLDER = "trusted"
    REFINED_FOLDER = "refined"
    DATASET_NAME = "covid_analysis"  # Nombre del dataset en BigQuery

    # Crear el dataset si no existe
    create_bigquery_dataset(DATASET_NAME)

    # Ejecutar el análisis y subir datos a BigQuery
    perform_data_analysis(BUCKET_NAME, TRUSTED_FOLDER, REFINED_FOLDER, DATASET_NAME)
