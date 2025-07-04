
import os
from pathlib import Path
from typing import List
import streamlit as st

def seleccionar_carpeta_base() -> Path | None:
    """
    Permite al usuario escribir o seleccionar una carpeta base desde Streamlit.
    """
    carpeta = st.text_input("📁 Ingrese la ruta de la carpeta base:", value="", placeholder="Ej. C:\\Users\\mi_usuario\\Documentos\\Pagos")
    
    if carpeta and Path(carpeta).exists():
        st.success(f"📂 Carpeta seleccionada: {carpeta}")
        return Path(carpeta)
    elif carpeta:
        st.warning("⚠️ La ruta ingresada no existe o no es válida.")
    
    return None

def obtener_txt_recursivamente(carpeta_base: Path) -> List[Path]:
    """
    Recorre la carpeta base y todas sus subcarpetas en busca de archivos .txt.
    """
    archivos_txt = []
    for root, _, files in os.walk(carpeta_base):
        for file in files:
            if file.lower().endswith(".txt"):
                archivos_txt.append(Path(root) / file)
    return archivos_txt
