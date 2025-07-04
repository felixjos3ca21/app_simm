# src/utils/procesador_streamlit/previsualizador.py

import pandas as pd
from pathlib import Path
from src.procesador_streamlit.archivos import detectar_encoding

def cargar_previsualizacion(archivo: Path, n_filas: int = 10) -> pd.DataFrame:
    """
    Carga las primeras `n_filas` de un archivo .txt como DataFrame para previsualización.
    """
    encoding = detectar_encoding(archivo)
    try:
        df = pd.read_csv(
            archivo,
            sep="\t",
            encoding=encoding,
            dtype=str,
            nrows=n_filas,
            on_bad_lines="skip"
        )
        return df
    except Exception as e:
        raise RuntimeError(f"Error al leer archivo {archivo.name}: {e}")
