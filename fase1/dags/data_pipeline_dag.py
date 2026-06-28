from datetime import datetime
from pathlib import Path
import sys

import toml
from airflow.decorators import dag, task


# Ruta base dentro del contenedor de Airflow.
# Los volúmenes definidos en docker-compose.yml montan aquí los ficheros del proyecto.
PROJECT_ROOT = Path("/opt/airflow")

# Se añade la raíz del proyecto al path para poder importar los módulos de tasks.
sys.path.append(str(PROJECT_ROOT))


from tasks.extract import extract_data
from tasks.clean import clean_data
from tasks.validate import validate_data
from tasks.transform import transform_data
from tasks.eda import generate_eda_plots
from tasks.kafka_loader import load_to_kafka


CONFIG_PATH = PROJECT_ROOT / "config.toml"


def read_config() -> dict:
    """
    Lee el archivo de configuración del proyecto.

    Se usa un fichero TOML para mantener separadas las rutas, parámetros
    del pipeline y configuración de Kafka respecto al código del DAG.
    """

    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"No se ha encontrado el fichero {CONFIG_PATH}")

    return toml.load(CONFIG_PATH)


@dag(
    dag_id="netflix_etl_kafka_pipeline",
    description=(
        "Pipeline ETL con Airflow para limpieza, validación, "
        "transformación, análisis exploratorio y envío a Kafka."
    ),
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    tags=["sdpd", "airflow", "kafka", "etl"],
)
def netflix_pipeline():
    """
    Define el flujo completo del pipeline.

    La estructura sigue el patrón ETL solicitado:
    - extracción del CSV original
    - limpieza y preparación de datos
    - validación de calidad
    - transformación y generación de resultados descriptivos
    - envío del resultado final a Kafka
    """

    @task
    def extract_task() -> str:
        """
        Extrae el dataset original.

        Esta tarea comprueba que el CSV indicado en config.toml existe
        y que contiene registros antes de continuar con el resto del flujo.
        """

        config = read_config()

        return extract_data(
            raw_path=config["paths"]["raw_data"]
        )

    @task
    def clean_task(raw_path: str) -> str:
        """
        Limpia el dataset original.

        Se normalizan nombres de columnas, se eliminan duplicados,
        se tratan valores nulos y se guarda una primera versión
        procesada en formato Parquet.
        """

        config = read_config()

        return clean_data(
            raw_path=raw_path,
            output_path=config["paths"]["processed_data"]
        )

    @task
    def validate_task(processed_path: str) -> str:
        """
        Valida la calidad básica del dataset.

        Se comprueba la existencia de columnas obligatorias y se aplican
        reglas sencillas sobre campos importantes, como título y año.
        """

        return validate_data(
            processed_path=processed_path
        )

    @task
    def transform_task(validated_path: str) -> str:
        """
        Transforma el dataset validado.

        En esta tarea se generan variables derivadas, se almacena el
        dataset final en Parquet, se crea un resumen EDA en CSV y se
        generan gráficos básicos de análisis exploratorio.
        """

        config = read_config()

        final_path = transform_data(
            processed_path=validated_path,
            output_path=config["paths"]["processed_data"],
            report_path=config["paths"]["report_path"]
        )

        # Generación de gráficos EDA a partir del dataset ya transformado.
        # Se mantiene dentro de la misma tarea para no superar el límite de
        # 3 a 5 tareas indicado en el enunciado.
        generate_eda_plots(
            parquet_path=final_path,
            output_dir=f"{config['paths']['report_path']}/plots"
        )

        return final_path

    @task
    def kafka_task(final_path: str) -> str:
        """
        Envía el resultado final a Kafka.

        Los registros procesados se serializan como JSON y se publican
        en el topic configurado para dejar los datos disponibles para
        fases posteriores.
        """

        config = read_config()

        return load_to_kafka(
            final_path=final_path,
            bootstrap_servers=config["kafka"]["bootstrap_servers"],
            topic=config["kafka"]["topic"]
        )

    # Secuencia explícita de ejecución del DAG.
    # Así se visualiza claramente el flujo ETL en la interfaz de Airflow.
    raw_file = extract_task()
    cleaned_file = clean_task(raw_file)
    validated_file = validate_task(cleaned_file)
    transformed_file = transform_task(validated_file)
    kafka_task(transformed_file)


netflix_pipeline()