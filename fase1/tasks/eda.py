from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def generate_eda_plots(parquet_path: str, output_dir: str) -> None:
    """
    Genera gráficos básicos de análisis exploratorio.

    Primero se calculan las agregaciones necesarias y después se crean
    las figuras, evitando recalcular información durante el graficado.
    """

    dataframe = pd.read_parquet(parquet_path)

    plots_dir = Path(output_dir)
    plots_dir.mkdir(parents=True, exist_ok=True)

    type_counts = dataframe["type"].value_counts()

    release_year = pd.to_numeric(
        dataframe["release_year"],
        errors="coerce"
    ).dropna()

    # Distribución de películas y series.
    figure, axis = plt.subplots(figsize=(6, 4))
    type_counts.plot(kind="bar", ax=axis)

    axis.set_title("Distribución de contenido")
    axis.set_xlabel("Tipo")
    axis.set_ylabel("Cantidad")

    figure.tight_layout()
    figure.savefig(plots_dir / "content_distribution.png")
    plt.close(figure)

    # Distribución de años de lanzamiento.
    figure, axis = plt.subplots(figsize=(8, 4))
    axis.hist(release_year, bins=30)

    axis.set_title("Distribución de años de lanzamiento")
    axis.set_xlabel("Año")
    axis.set_ylabel("Frecuencia")

    figure.tight_layout()
    figure.savefig(plots_dir / "release_year_distribution.png")
    plt.close(figure)