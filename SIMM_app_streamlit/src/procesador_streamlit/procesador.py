# src/utils/procesador_streamlit/procesador.py

import pandas as pd
from pathlib import Path
from sqlalchemy.engine import Engine
from typing import Literal, Tuple
from src.procesador_streamlit.clasificadores import detectar_tipo_archivo
from src.procesador_streamlit.funciones_bases import procesar_archivo_ap, procesar_archivo_comparendos
from sqlalchemy import text

def procesar_archivo(archivo: Path, tipo: Literal["AP", "COMP"]) -> pd.DataFrame:
    """
    Llama a la función de procesamiento adecuada según el tipo de archivo.
    """
    if tipo == "AP":
        return procesar_archivo_ap(archivo)
    elif tipo == "COMP":
        return procesar_archivo_comparendos(archivo)
    else:
        raise ValueError(f"Tipo de archivo no reconocido: {tipo}")

def clasificar_registros(df: pd.DataFrame, engine: Engine, tabla_destino: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Clasifica los registros del DataFrame en tres grupos:
    - nuevos: no están en la base
    - actualizables: existen pero con valor = 0, y nuevo valor > 0
    - duplicados: ya existen y no deben actualizarse
    """
    ids = df["id_registro"].tolist()

    consulta = text(f"""
        SELECT id_registro, valor
        FROM {tabla_destino}
        WHERE id_registro = ANY(:ids)
    """)

    with engine.connect() as conn:
        resultado = conn.execute(consulta, {"ids": ids}).fetchall()
        existentes = {row[0]: row[1] for row in resultado}

    nuevos, actualizables, duplicados = [], [], []

    for _, fila in df.iterrows():
        id_ = fila["id_registro"]
        valor_nuevo = fila["valor"]

        if id_ not in existentes:
            nuevos.append(fila)
        elif existentes[id_] == 0 and valor_nuevo > 0:
            actualizables.append(fila)
        else:
            duplicados.append(fila)

    df_nuevos = pd.DataFrame(nuevos)
    df_actualizables = pd.DataFrame(actualizables)
    df_duplicados = pd.DataFrame(duplicados)

    return df_nuevos, df_actualizables, df_duplicados
