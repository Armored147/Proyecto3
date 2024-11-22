from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

# Crear sesión de Spark
spark = SparkSession.builder.appName("ETL Script").getOrCreate()

# Rutas de archivos en GCS
csv_path = "gs://project3-tele/data/datos.csv"
json_path = "gs://project3-tele/data/datos_mysql.json"
output_path = "gs://project3-tele/trusted/datos_combinados.csv"

# Cargar datos desde CSV
df_csv = spark.read.csv(csv_path, header=True)

# Cargar datos desde JSON
df_json = spark.read.json(json_path)

# Renombrar columnas del CSV para que coincidan con el JSON
df_csv = df_csv.withColumnRenamed("fecha_de_notificaci_n", "fecha_notificacion") \
               .withColumnRenamed("unidad_medida", "unidad_medida_edad") \
               .withColumnRenamed("ubicacion", "ubicacion_caso") \
               .withColumnRenamed("departamento_nom", "nombre_departamento") \
               .withColumnRenamed("ciudad_municipio_nom", "nombre_municipio") \
               .withColumnRenamed("per_etn_", "pertenencia_etnica") \
               .withColumnRenamed("nom_grupo_", "nombre_grupo_etnico") \
               .withColumnRenamed("pais_viajo_1_cod", "codigo_iso_pais") \
               .withColumnRenamed("pais_viajo_1_nom", "nombre_pais") \
               .withColumnRenamed("fecha_recuperado", "fecha_recuperacion")

# Seleccionar columnas comunes y agregar faltantes con valores nulos
common_columns = [
    "fecha_reporte_web", "id_de_caso", "fecha_notificacion", "codigo_divipola_departamento",
    "nombre_departamento", "codigo_divipola_municipio", "nombre_municipio", "edad",
    "unidad_medida_edad", "sexo", "tipo_contagio", "ubicacion_caso", "estado",
    "recuperado", "fecha_inicio_sintomas", "fecha_muerte", "fecha_diagnostico",
    "fecha_recuperacion", "tipo_recuperacion", "pertenencia_etnica", "nombre_grupo_etnico",
    "codigo_iso_pais", "nombre_pais"
]

df_csv = df_csv.select(
    *[col(c).alias(c) if c in df_csv.columns else lit(None).alias(c) for c in common_columns]
)

df_json = df_json.select(
    *[col(c).alias(c) if c in df_json.columns else lit(None).alias(c) for c in common_columns]
)

# Reemplazar valores no válidos en ambos DataFrames
df_csv = df_csv.replace("0000-00-00 00:00:00", None)
df_json = df_json.replace("0000-00-00 00:00:00", None)

# Combinar ambos DataFrames
df_combined = df_csv.unionByName(df_json)

# Guardar el resultado combinado en formato CSV
df_combined.write.csv(output_path, header=True, mode="overwrite")

print("ETL completado exitosamente.")
spark.stop()

