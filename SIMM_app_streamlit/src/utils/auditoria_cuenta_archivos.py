import pandas as pd
from pathlib import Path
import re
from collections import defaultdict

def auditoria_pagos(directorio):
    """
    Analiza un directorio y genera conteos precisos de archivos AP y COMPARENDOS.
    
    Args:
        directorio (str): Ruta a analizar (ej: 'C:/pagos/2023')
        
    Returns:
        dict: {
            'total_archivos': int,
            'ap': {'count': int, 'ejemplos': [str]},
            'comp': {'count': int, 'ejemplos': [str]},
            'otros': {'count': int, 'ejemplos': [str]},
            'por_carpeta': {
                'ruta/carpeta1': {'ap': int, 'comp': int, 'otros': int},
                'ruta/carpeta2': {'ap': int, 'comp': int, 'otros': int}
            }
        }
    """
    # Configuración
    patron_ap = re.compile(r'\bap[\s_\-]*pagos?\b', re.IGNORECASE)
    patron_comp = re.compile(r'\b(comp[aá]r?endos?)\b', re.IGNORECASE)
    
    resultados = {
        'total_archivos': 0,
        'ap': {'count': 0, 'ejemplos': []},
        'comp': {'count': 0, 'ejemplos': []},
        'otros': {'count': 0, 'ejemplos': []},
        'por_carpeta': defaultdict(lambda: {'ap': 0, 'comp': 0, 'otros': 0})
    }

    # Recorrer archivos
    for archivo in Path(directorio).rglob('*.txt'):
        resultados['total_archivos'] += 1
        nombre = archivo.name.lower()
        carpeta = str(archivo.parent)
        
        # Clasificación
        if patron_ap.search(nombre):
            resultados['ap']['count'] += 1
            resultados['ap']['ejemplos'].append(str(archivo))
            resultados['por_carpeta'][carpeta]['ap'] += 1
        elif patron_comp.search(nombre):
            resultados['comp']['count'] += 1
            resultados['comp']['ejemplos'].append(str(archivo))
            resultados['por_carpeta'][carpeta]['comp'] += 1
        else:
            resultados['otros']['count'] += 1
            resultados['otros']['ejemplos'].append(str(archivo))
            resultados['por_carpeta'][carpeta]['otros'] += 1

    return resultados

def imprimir_auditoria(resultados):
    """Muestra los resultados de forma organizada"""
    print("\n" + "="*50)
    print("AUDITORÍA DE ARCHIVOS DE PAGOS")
    print("="*50)
    
    print(f"\n📂 Directorio analizado: {resultados.get('directorio', '')}")
    print(f"🔍 Total archivos .txt: {resultados['total_archivos']}")
    
    print("\n📊 Conteo por tipo:")
    print(f"- AP: {resultados['ap']['count']} archivos")
    print(f"- COMPARENDOS: {resultados['comp']['count']} archivos")
    print(f"- Otros: {resultados['otros']['count']} archivos")
    
    print("\n📂 Distribución por carpetas:")
    for carpeta, counts in resultados['por_carpeta'].items():
        print(f"\n  {carpeta}:")
        print(f"  AP: {counts['ap']:>3} | COMP: {counts['comp']:>3} | Otros: {counts['otros']:>3}")

# Ejemplo de uso
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        directorio = sys.argv[1]
        print(f"🔍 Iniciando auditoría en: {directorio}")
        resultados = auditoria_pagos(directorio)
        resultados['directorio'] = directorio  # Guardamos la ruta original
        imprimir_auditoria(resultados)
        
        # Opcional: Exportar a Excel
        exportar = input("\n¿Exportar a Excel? (s/n): ").lower() == 's'
        if exportar:
            df = pd.DataFrame.from_dict(resultados['por_carpeta'], orient='index')
            df.to_excel("auditoria_pagos.xlsx")
            print("✅ Exportado a 'auditoria_pagos.xlsx'")
    else:
        print("ERROR: Debes especificar una ruta\nEjemplo: python auditoria.py C:/pagos/2023")