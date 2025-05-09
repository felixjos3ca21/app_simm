from limpieza_pagos import procesar_pagos  # Asumo que guardaste la función en este módulo
import traceback
import os
import pandas as pd

def procesar_carpeta_pagos(ruta_carpeta: str):
    """
    Función para procesar todos los archivos TXT de una carpeta usando procesar_pagos
    """
    archivos = [f for f in os.listdir(ruta_carpeta) if f.lower().endswith('.txt')]
    
    todos_resultados = []
    todos_errores = []
    archivos_error = []
    
    for archivo in archivos:
        try:
            ruta_completa = os.path.join(ruta_carpeta, archivo)
            df_procesado, df_errores, warnings = procesar_pagos(ruta_completa, archivo)
            
            if not df_procesado.empty:
                todos_resultados.append(df_procesado)
            
            if not df_errores.empty:
                todos_errores.append(df_errores)
                
            if warnings:
                print(f"Advertencias en {archivo}: {warnings}")
                
        except Exception as e:
            archivos_error.append(archivo)
            print(f"Error procesando {archivo}: {str(e)}")
            traceback.print_exc()
    
    # Concatenar todos los resultados
    df_final = pd.concat(todos_resultados, ignore_index=True) if todos_resultados else pd.DataFrame()
    df_errores_final = pd.concat(todos_errores, ignore_index=True) if todos_errores else pd.DataFrame()
    
    return df_final, df_errores_final, archivos_error

# Prueba de la función
try:
    resultados, errores, archivos_error = procesar_carpeta_pagos(
        r"C:\Users\1513873\ANDES BPO S.A.S\Johan Felipe JG. Gómez Arango - SIMM\BASES\2025\ENERO\02_01_2025 - copia"
    )
    
    print("\n═ RESULTADOS ═══════════════════════════════════════")
    print(f"Total registros válidos: {len(resultados)}")
    print(f"Total registros con errores: {len(errores)}")
    print(f"Archivos con errores: {', '.join(archivos_error) if archivos_error else 'Ninguno'}")
    
    if not resultados.empty:
        print("\nMuestra de datos procesados:")
        print(resultados.head())
        
    if not errores.empty:
        print("\nMuestra de errores:")
        print(errores.head())
        
except Exception as e:
    print(f"ERROR GLOBAL: {str(e)}")
    traceback.print_exc()