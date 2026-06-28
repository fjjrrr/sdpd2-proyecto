from pathlib import Path

import pandas as pd


def clean_data(raw_path: str, output_path: str) -> str:
    """
    Realiza la limpieza principal del dataset.

    Se aplican operaciones vectorizadas siempre que es posible para evitar
    recorridos innecesarios y mejorar la eficiencia del pipeline.
    """

    # Lectura del dataset original.
    dataframe = pd.read_csv(raw_path, engine="pyarrow")

    # Normalización de nombres de columnas.
    dataframe.columns = (
        dataframe.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )

    # Eliminación de duplicados y registros sin título.
    dataframe = dataframe.drop_duplicates()

    if "title" in dataframe.columns:
        dataframe = dataframe.dropna(subset=["title"])

    # Columnas textuales sobre las que se tratan nulos y espacios.
    text_columns = ["director", "cast", "country", "rating", "duration"]
    existing_text_columns = [
        column for column in text_columns if column in dataframe.columns
    ]

    # Tratamiento conjunto de columnas textuales.
    if existing_text_columns:
        dataframe[existing_text_columns] = (
            dataframe[existing_text_columns]
            .fillna("unknown")
            .astype("string")
            .apply(lambda column: column.str.strip())
        )

    # Conversión de fecha de incorporación al catálogo.
    if "date_added" in dataframe.columns:
        dataframe["date_added"] = pd.to_datetime(
            dataframe["date_added"],
            errors="coerce"
        )

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Guardado en Parquet para mejorar la eficiencia de lectura/escritura.
    dataframe.to_parquet(output_file, index=False)

    print(f"Dataset limpio guardado en: {output_file}")

    return str(output_file)