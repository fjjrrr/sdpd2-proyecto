from pathlib import Path

import pandas as pd


def extract_data(raw_path: str) -> str:
    """
    Carga inicial del dataset.

    En esta primera tarea se comprueba que el fichero original existe,
    se lee el CSV y se valida que contiene datos. No se modifica todavía
    el contenido del dataset, ya que esta tarea corresponde a la fase de
    extracción dentro del flujo ETL.
    """

    # Se convierte la ruta recibida a un objeto Path para trabajar de forma
    # más segura con rutas de ficheros.
    input_path = Path(raw_path)

    # Antes de intentar leer el CSV, se comprueba que el archivo existe.
    # Esto evita errores menos claros en pasos posteriores del pipeline.
    if not input_path.exists():
        raise FileNotFoundError(f"No se ha encontrado el fichero: {input_path}")

    # Lectura del dataset original. En esta fase se mantiene el fichero
    # tal como se ha descargado, sin aplicar transformaciones todavía.
    dataframe = pd.read_csv(input_path, engine="pyarrow")
    # Se comprueba que el dataset no esté vacío. Si no hay registros,
    # el resto del pipeline no tendría sentido.
    if dataframe.empty:
        raise ValueError("El dataset está vacío.")

    # Mensaje informativo para poder revisar en los logs de Airflow
    # cuántas filas y columnas se han cargado correctamente.
    print(
        f"Dataset cargado correctamente: "
        f"{dataframe.shape[0]} filas y {dataframe.shape[1]} columnas"
    )

    # Se devuelve la ruta del fichero para que la siguiente tarea del DAG
    # pueda continuar trabajando a partir de los datos originales.
    return str(input_path)