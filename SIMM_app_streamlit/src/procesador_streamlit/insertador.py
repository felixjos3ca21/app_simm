# src/utils/procesador_streamlit/insertador.py

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from datetime import datetime

def insertar_registros(df: pd.DataFrame, tabla: str, engine: Engine) -> int:
    """
    Inserta registros nuevos a la tabla correspondiente.
    Retorna la cantidad insertada.
    """
    if df.empty:
        return 0

    try:
        df.to_sql(tabla, con=engine, if_exists='append', index=False, method='multi')
        return len(df)
    except Exception as e:
        print(f"❌ Error al insertar en {tabla}: {e}")
        return 0

def actualizar_registros(df: pd.DataFrame, tabla: str, engine: Engine) -> int:
    """
    Actualiza registros existentes con valor = 0 por nuevos > 0.
    Retorna la cantidad actualizada.
    """
    if df.empty:
        return 0

    updated = 0
    try:
        with engine.begin() as conn:
            for _, row in df.iterrows():
                query = text(f"""
                    UPDATE {tabla}
                    SET valor = :valor
                    WHERE id_registro = :id AND valor = 0
                """)
                conn.execute(query, {
                    "valor": row["valor"],
                    "id": row["id_registro"]
                })
                updated += 1
    except Exception as e:
        print(f"❌ Error actualizando registros en {tabla}: {e}")
    return updated

def registrar_archivo(nombre: str, tipo: str, cantidad: int, estado: str, hash_archivo: str, engine: Engine):
    """
    Registra en la tabla archivos_procesados_pagos.
    """
    query = text("""
        INSERT INTO archivos_procesados_pagos
        (ruta_archivo, tipo_archivo, registros_procesados, estado, hash_archivo, fecha_procesamiento)
        VALUES (:nombre, :tipo, :cantidad, :estado, :hash, :fecha)
        ON CONFLICT (ruta_archivo) DO NOTHING
    """)
    try:
        with engine.begin() as conn:
            conn.execute(query, {
                "nombre": nombre,
                "tipo": tipo,
                "cantidad": cantidad,
                "estado": estado,
                "hash": hash_archivo,
                "fecha": datetime.now()
            })
    except Exception as e:
        print(f"❌ Error registrando archivo {nombre}: {e}")
