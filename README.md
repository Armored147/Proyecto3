# Tópicos Especiales en Telemática: C2466-ST0263-1716

## Estudiantes

- **Santiago Alberto Rozo Silva**, [sarozos@eafit.brightspace.com](mailto\:sarozos@eafit.edu.com)
- **Isis Catitza Amaya Arbeláez**, [icamayaa@eafit.edu.co](mailto\:icamayaa@eafit.edu.co)
- **Samuel Oviedo Paz**, [soviedop@eafit.edu.co](mailto\:soviedop@eafit.edu.co)
- **Samuel Acosta Aristizabal**, [sacostaa1@eafit.edu.co](mailto\:sacostaa1@eafit.edu.co)
- **Nicolás Tovar Almanza**, [ntovara@eafit.brightspace.com](mailto\:ntovara@eafit.brightspace.com)

## Profesor

- **Álvaro Enrique Ospina Sanjuan**, [aeospinas@eafit.edu.co](mailto\:aeospinas@eafit.edu.co)

## Video
[Video de proyecto](https://drive.google.com/file/d/1wVJTaH0zhiwrxhz6hKXpOE5O78j-ykzu/view?pli=1)

# Proyecto No 3

## 1. Breve descripción de la actividad

El Proyecto 3 consiste en diseñar y automatizar un proceso de captura, ingesta, procesamiento y entrega de datos relacionados con la gestión del COVID-19 en Colombia, utilizando Google Cloud Platform (GCP). El flujo de trabajo simula un escenario real de ingeniería de datos en un entorno de Big Data, donde se deben realizar tareas ETL (Extracción, Transformación y Carga) para preparar los datos de forma adecuada para el análisis.

### Aspectos Desarrollados

- **Captura e Ingesta de Datos**: Se realizó la captura de datos desde el Ministerio de Salud (API o archivos) y desde una base de datos relacional en Google Cloud SQL.
- **Procesamiento de Datos**: Utilización de Dataproc y Spark para el procesamiento ETL de los datos, almacenándolos en una zona intermedia y luego refinada en Cloud Storage.
- **Consulta y Análisis**: Implementación de consultas sobre los datos usando BigQuery, creando tablas con los resultados del análisis.
- **Entrega y Automatización**: Implementación de una API mediante Cloud Endpoints y Cloud Functions para acceder a los datos procesados.

### Aspectos No Desarrollados

- Consideramos que se realizaron todos los objetivos propuestos en el Proyecto

## 2. Información General de Diseño de Alto Nivel

El proyecto sigue un enfoque modular y de alto nivel para simular un escenario real de ingeniería de datos, dividido en distintas zonas para garantizar la integridad y calidad de los datos.

### Componentes Principales

- **Zonas de Almacenamiento**: Uso de Google Cloud Storage dividido en tres zonas:
  - Zona **Raw**: Almacenamiento de datos sin procesar desde las fuentes originales.
  - Zona **Trusted**: Datos procesados y preparados para un análisis inicial.
  - Zona **Refined**: Datos refinados listos para el consumo final.
- **Procesamiento ETL**: Uso de Dataproc y Spark para la transformación y limpieza de datos.
- **Consulta y Exposición de Datos**: BigQuery y Cloud Functions junto con Cloud Endpoints para el acceso controlado a los datos.

### Buenas Prácticas

- **Automatización**: Uso de scripts y configuraciones para minimizar la intervención manual.
- **Seguridad**: Configuración de IAM y permisos para controlar el acceso a los recursos.
- **Documentación Completa**: Cada fase del proyecto está documentada para garantizar reproducibilidad.

## 3. Ambiente de Desarrollo

- **Lenguaje de Programación**: Python y Shell Script.
- **Herramientas Utilizadas**:
  - **Google Cloud SDK**: Para interacción con los servicios de GCP.
  - **Apache Spark**: Para el procesamiento de datos en Dataproc.
  - **BigQuery**: Para la consulta y análisis de los datos.
  - **Cloud Functions y Endpoints**: Para la exposición de datos procesados mediante una API.

## 4. Ambiente de Ejecución (Producción)

- **GCP Services**:
  - **Dataproc**: Clúster usado para la ejecución de tareas de procesamiento.
  - **Cloud Storage**: Almacenamiento de los datos en las diferentes zonas.
  - **BigQuery**: Base de datos para almacenar resultados y realizar consultas.
  - **Cloud Functions y Cloud Endpoints**: Implementación de la API para acceso a los datos.

## 5. Implementación y Pruebas

### Script de Automatización

El proyecto incluyó la creación de un script para automatizar la ingesta, procesamiento y almacenamiento de los datos. A continuación se muestra un ejemplo del proceso de configuración de GCP:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/bigdata-442414-9e618bb22c7b.json"
gcloud auth activate-service-account --key-file="$GOOGLE_APPLICATION_CREDENTIALS"
gcloud config set project bigdata-442414
```

Este script permite activar las credenciales y desplegar los servicios necesarios para operar la API.

## 6. Conclusiones y Lecciones Aprendidas

Este proyecto nos ayudó a comprender el proceso de ingeniería de datos en un entorno de Big Data utilizando GCP. Se enfatizó la importancia de la automatización, la seguridad de los datos y la organización modular de cada una de las fases del flujo de trabajo.

