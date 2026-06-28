import pandas as pd


def validate_data(processed_path: str) -> str:
    """
    Comprueba distintas reglas básicas de validación sobre el dataset
    previamente limpiado.

    Esta tarea permite detectar posibles errores antes de continuar
    con las transformaciones finales y el envío a Kafka.
    """

    # Lectura del dataset procesado generado en la fase anterior.
    dataframe = pd.read_parquet(processed_path)

    # Lista de columnas consideradas obligatorias para el análisis.
    required_columns = [
        "show_id",
        "type",
        "title",
        "release_year",
        "listed_in"
    ]

    # Comprobación de columnas faltantes.
    missing_columns = [

        column
        for column in required_columns
        if column not in dataframe.columns
    ]

    # Si faltan columnas importantes, se detiene el pipeline.
    if missing_columns:

        raise ValueError(
            f"Faltan columnas obligatorias: {missing_columns}"
        )

    # Verificación de títulos nulos después de la limpieza.
    # Si aparecen aquí, indicaría un fallo en la fase anterior.
    if dataframe["title"].isna().any():

        raise ValueError(
            "Existen títulos nulos después de la limpieza."
        )

    # Comprobación básica del rango de años.
    # Se consideran válidos años entre 1900 y 2030.
    valid_years = dataframe["release_year"].between(1900, 2030)

    if not valid_years.all():

        raise ValueError(
            "Existen años fuera del rango permitido."
        )

    # Mensaje visible en los logs de Airflow.
    print("Validación completada correctamente.")

    # Se devuelve la misma ruta para continuar con la siguiente tarea.
    return processed_path