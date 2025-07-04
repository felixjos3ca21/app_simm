import pandas as pd
from pathlib import Path
from datetime import datetime

RUTA_CONSOLIDADO = Path("data/consolidado_respaldo_pagos.csv")
RUTA_CONSOLIDADO.parent.mkdir(parents=True, exist_ok=True)  # crea la carpeta si no existe

def guardar_en_consolidado(df: pd.DataFrame, tipo: str, archivo: str) -> None:
    """
    Agrega los registros de un archivo procesado al CSV consolidado.
    No elimina los anteriores. Simplemente concatena.
    """
    if df.empty:
        return

    df = df.copy()
    df["archivo_origen"] = archivo
    df["tipo_pago"] = tipo
    df["fecha_procesamiento"] = datetime.now()

    try:
        if RUTA_CONSOLIDADO.exists():
            df_existente = pd.read_csv(RUTA_CONSOLIDADO, dtype=str)
            df_total = pd.concat([df_existente, df], ignore_index=True)
        else:
            df_total = df

        df_total.to_csv(RUTA_CONSOLIDADO, index=False)
    except Exception as e:
        print(f"❌ Error guardando en consolidado: {e}")
