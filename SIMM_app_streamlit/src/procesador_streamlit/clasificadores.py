# src/utils/procesador_streamlit/clasificadores.py

import re
import hashlib
import pandas as pd
from pathlib import Path
from sqlalchemy.engine import Engine
from src.database.postgres import DatabaseManager
from src.procesador_streamlit.archivos import detectar_encoding
from sqlalchemy import text

# Expresiones regulares para detectar tipo
PATRON_AP = re.compile(r'\bap[\s_\-]*pa?g?[a-z]{2,}', re.IGNORECASE)
PATRON_COMP = re.compile(r'(compa|compr)[a-z]*end?[oa]s?', re.IGNORECASE)

def detectar_tipo_archivo(archivo: Path) -> str:
    """
    Detecta si el archivo corresponde a tipo AP, Comparendos o desconocido.
    """
    nombre = archivo.name
    if PATRON_AP.search(nombre):
        return "AP"
    elif PATRON_COMP.search(nombre):
        return "COMP"
    else:
        return "DESCONOCIDO"

def calcular_hash_archivo(archivo: Path) -> str:
    """
    Calcula el hash SHA256 del contenido del archivo.
    """
    sha256 = hashlib.sha256()
    with open(archivo, "rb") as f:
        while chunk := f.read(8192):
            sha256.update(chunk)
    return sha256.hexdigest()

def consultar_estado_archivo(engine, nombre_archivo: str, hash_archivo: str) -> str:
    """
    Consulta si el archivo ya fue procesado y determina su estado.
    Retorna: 'nuevo', 'ya_procesado', 'modificado'
    """
    query = text("""
        SELECT hash_archivo 
        FROM archivos_procesados_pagos
        WHERE ruta_archivo ILIKE :nombre
        LIMIT 1
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {"nombre": f"%{nombre_archivo}"}).fetchone()

        if result:
            hash_db = result[0]
            if hash_db == hash_archivo:
                return "ya_procesado"
            else:
                return "modificado"

        return "nuevo"
