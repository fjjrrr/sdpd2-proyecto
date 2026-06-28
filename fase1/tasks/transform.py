from pathlib import Path

import pandas as pd


def transform_data(processed_path: str, output_path: str, report_path: str) -> str:
    """
    Aplica transformaciones finales sobre el dataset limpio y validado.

    Se generan variables derivadas mediante operaciones vectorizadas para
    mantener una ejecución eficiente.
    """

    dataframe = pd.read_parquet(processed_path)
    transformed_dataframe = dataframe.copy()

    transformed_dataframe["release_year"] = pd.to_numeric(
        transformed_dataframe["release_year"],
        errors="coerce"
    )

    # Antigüedad del contenido respecto al año de referencia del proyecto.
    transformed_dataframe["content_age"] = (
        2026 - transformed_dataframe["release_year"]
    )

    # Longitud del título.
    transformed_dataframe["title_length"] = (
        transformed_dataframe["title"]
        .astype("string")
        .str.len()
    )

    # Número de categorías calculado sin usar apply fila a fila.
    transformed_dataframe["num_categories"] = (
        transformed_dataframe["listed_in"]
        .astype("string")
        .str.count(",")
        .fillna(0)
        .astype(int)
        + 1
    )

    # Codificación binaria del tipo de contenido.
    transformed_dataframe["is_movie"] = (
        transformed_dataframe["type"]
        .astype("string")
        .str.lower()
        .eq("movie")
        .astype(int)
    )

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    transformed_dataframe.to_parquet(output_file, index=False)

    reports_dir = Path(report_path)
    reports_dir.mkdir(parents=True, exist_ok=True)

    summary_file = reports_dir / "eda_summary.csv"

    summary_columns = [
        "release_year",
        "content_age",
        "title_length",
        "num_categories",
        "is_movie"
    ]

    transformed_dataframe[summary_columns].describe().to_csv(summary_file)

    print(f"Dataset transformado guardado en: {output_file}")
    print(f"Resumen EDA guardado en: {summary_file}")

    return str(output_file)