import chardet
from pathlib import Path

def detectar_encoding(archivo: Path, muestra_bytes: int = 100000) -> str:
    """
    Detecta el encoding de un archivo leyendo los primeros `muestra_bytes`.
    Por defecto, se leen los primeros 100KB del archivo.
    """
    with open(archivo, "rb") as f:
        rawdata = f.read(muestra_bytes)
    resultado = chardet.detect(rawdata)
    return resultado.get("encoding", "utf-8") or "utf-8"
