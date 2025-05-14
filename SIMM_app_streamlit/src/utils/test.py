# from limpieza_pagos import procesar_pagos 
# import traceback
# import os
# import pandas as pd

# def procesar_carpeta_pagos(ruta_carpeta: str):
#     """
#     Función para procesar todos los archivos TXT de una carpeta usando procesar_pagos
#     """
#     archivos = [f for f in os.listdir(ruta_carpeta) if f.lower().endswith('.txt')]
    
#     todos_resultados = []
#     todos_errores = []
#     archivos_error = []
    
#     for archivo in archivos:
#         try:
#             ruta_completa = os.path.join(ruta_carpeta, archivo)
#             df_procesado, df_errores, warnings = procesar_pagos(ruta_completa, archivo)
            
#             if not df_procesado.empty:
#                 todos_resultados.append(df_procesado)
            
#             if not df_errores.empty:
#                 todos_errores.append(df_errores)
                
#             if warnings:
#                 print(f"Advertencias en {archivo}: {warnings}")
                
#         except Exception as e:
#             archivos_error.append(archivo)
#             print(f"Error procesando {archivo}: {str(e)}")
#             traceback.print_exc()
    
#     # Concatenar todos los resultados
#     df_final = pd.concat(todos_resultados, ignore_index=True) if todos_resultados else pd.DataFrame()
#     df_errores_final = pd.concat(todos_errores, ignore_index=True) if todos_errores else pd.DataFrame()
    
#     return df_final, df_errores_final, archivos_error

# # Prueba de la función
# try:
#     resultados, errores, archivos_error = procesar_carpeta_pagos(
#         r"C:\Users\1513873\ANDES BPO S.A.S\Johan Felipe JG. Gómez Arango - SIMM\BASES\2025\ENERO\02_01_2025 - copia"
#     )
    
#     print("\n═ RESULTADOS ═══════════════════════════════════════")
#     print(f"Total registros válidos: {len(resultados)}")
#     print(f"Total registros con errores: {len(errores)}")
#     print(f"Archivos con errores: {', '.join(archivos_error) if archivos_error else 'Ninguno'}")
    
#     if not resultados.empty:
#         print("\nMuestra de datos procesados:")
#         print(resultados.head())
        
#     if not errores.empty:
#         print("\nMuestra de errores:")
#         print(errores.head())
        
# except Exception as e:
#     print(f"ERROR GLOBAL: {str(e)}")
#     traceback.print_exc()

#--__________________________________________________
# test_limpieza.py
# # test_limpieza.py
# import pandas as pd
# from limpieza_archivo import preparar_datos

# def mostrar_progreso(progreso, mensaje):
#     print(f"[{int(progreso*100)}%] {mensaje}")

# # Ejecutar prueba
# if __name__ == "__main__":
#     print("=== PRUEBA INICIADA ===")
#     ruta = r"C:\Users\1513873\ANDES BPO S.A.S\Johan Felipe JG. Gómez Arango - SIMM\BASES\Gestiones\1-BASE CONSOLIDADA DE ENERO_prueba.xlsx"  # Cambiar por tu ruta real
#     nombre = "1-BASE CONSOLIDADA DE ENERO_prueba.xlsx"
    
#     df_valido, df_errores, log = preparar_datos(
#         ruta_archivo=ruta,
#         nombre_archivo=nombre,
#         update_progress=mostrar_progreso
#     )
    
#     print("\n=== RESULTADOS ===")
#     print(f"Registros válidos: {len(df_valido)}")
#     print(f"Registros con errores: {len(df_errores)}")
#     print("\nLog completo:")
#     print(log)

import os
import shutil
import pandas as pd
from datetime import datetime
from limpieza_pagos import procesar_pagos

def crear_archivo_prueba(ruta, contenido, separador='\t'):
    """Crea archivos de prueba temporales"""
    df = pd.DataFrame(contenido)
    df.to_csv(ruta, sep=separador, index=False, encoding='ISO-8859-1')

def test_escenario_completo():
    """Prueba completa con diferentes tipos de archivos"""
    print("\n=== TEST ESCENARIO COMPLETO ===")
    
    # Crear carpeta temporal de prueba
    test_dir = "test_data_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(test_dir, exist_ok=True)
    
    try:
        # 1. Archivo válido completo
        crear_archivo_prueba(
            os.path.join(test_dir, "pagos_validos.txt"),
            {
                'id_usuario': ['123456', '789012', '345678'],
                'nombres': ['Juan', 'María', 'Pedro'],
                'apellidos': ['Pérez', 'Gómez', 'López'],
                'fecha_liquida': ['01/01/2023', '15/01/2023 10:30:00', '2023-01-20'],
                'compute_0004': [150000, 200000, 180000],
                'intereses': [5000, 0, 2000],
                'nro_comparendo': ['CMP2023001', 'CMP2023002', 'CMP2023003'],
                'nro_resolucion': ['RES001', 'RES002', 'RES003']
            }
        )
        
        # 2. Archivo con errores
        crear_archivo_prueba(
            os.path.join(test_dir, "pagos_con_errores.txt"),
            {
                'id_usuario': ['', '789012', 'INVALIDO'],
                'nombres': ['Juan', '', 'Pedro'],
                'apellidos': ['Pérez', 'Gómez', ''],
                'fecha_pago': ['INVALIDO', '15/01/2023', ''],
                'valor': [0, -100, 'ABC'],
                'nro_acuerdo': ['ACU001', 'ACU002', 'ACU003']
            }
        )
        
        # 3. Archivo con formato diferente (CSV con ;)
        crear_archivo_prueba(
            os.path.join(test_dir, "pagos_formato_diferente.csv"),
            {
                'id_usuario': ['987654', '654321'],
                'nombre_completo': ['Ana Torres', 'Carlos Sánchez'],
                'fecha_liquida_contrav': ['2023-02-01 08:15:00', '01/02/2023'],
                'valor_total': [250000, 300000],
                'numero_comparendo': ['CMP2023004', 'CMP2023005']
            },
            separador=';'
        )
        
        # Procesar la carpeta de prueba
        resultados = procesar_pagos(test_dir)
        
        # Mostrar resultados
        print("\nRESULTADOS DEL PROCESAMIENTO:")
        print(f"- Archivos procesados: {len(resultados['df_limpio']['archivo_origen'].unique())}")
        print(f"- Registros válidos: {len(resultados['df_limpio'])}")
        print(f"- Registros con errores: {len(resultados['df_errores'])}")
        
        if not resultados['advertencias']:
            print("- No hubo advertencias")
        else:
            print("\nADVERTENCIAS:")
            for warn in resultados['advertencias']:
                print(f"  • {warn}")
        
        # Mostrar estructura de datos válidos
        print("\nMUESTRA DE DATOS VÁLIDOS:")
        print(resultados['df_limpio'].head(3))
        
        # Mostrar errores encontrados
        if not resultados['df_errores'].empty:
            print("\nERRORES DETECTADOS:")
            print(resultados['df_errores'].head())
        
        # Verificar estructura final
        columnas_esperadas = [
            'id_registro', 'nro_acuerdo', 'nro_comparendo', 'documento',
            'nombre_usuario', 'valor', 'fecha_pago', 'archivo_origen',
            'identificador_infraccion', 'fecha_carga'
        ]
        
        assert all(col in resultados['df_limpio'].columns for col in columnas_esperadas), \
            "❌ Error: Faltan columnas requeridas"
        
        print("\n✅ Prueba exitosa: Todos los campos requeridos están presentes")
        
    finally:
        # Limpiar carpeta temporal
        shutil.rmtree(test_dir)

def test_archivo_vacio():
    """Prueba con archivo vacío"""
    print("\n=== TEST ARCHIVO VACÍO ===")
    
    test_dir = "test_vacio_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(test_dir, exist_ok=True)
    
    try:
        # Crear archivo vacío
        open(os.path.join(test_dir, "vacio.txt"), 'w').close()
        
        resultados = procesar_pagos(test_dir)
        
        print("Resultados:")
        print(f"- Válidos: {len(resultados['df_limpio'])}")
        print(f"- Errores: {len(resultados['df_errores'])}")
        
        assert resultados['df_limpio'].empty, "Se detectaron registros válidos en archivo vacío"
        assert "Error procesando archivo" in resultados['advertencias'][0], \
            "No se generó advertencia para archivo vacío"
        
        print("✅ Prueba exitosa: Manejo correcto de archivo vacío")
        
    finally:
        shutil.rmtree(test_dir)

def test_estructura_minima():
    """Prueba con estructura mínima válida"""
    print("\n=== TEST ESTRUCTURA MÍNIMA ===")
    
    test_dir = "test_minimo_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(test_dir, exist_ok=True)
    
    try:
        crear_archivo_prueba(
            os.path.join(test_dir, "minimo_valido.txt"),
            {
                'id_usuario': ['111111'],
                'nombres': ['Mínimo'],
                'apellidos': ['Válido'],
                'valor': [100000],
                'fecha_liquida': ['01/01/2023']
            }
        )
        
        resultados = procesar_pagos(test_dir)
        
        print("Resultados:")
        print(f"- Válidos: {len(resultados['df_limpio'])}")
        print(f"- Errores: {len(resultados['df_errores'])}")
        
        assert len(resultados['df_limpio']) == 1, "Debería haber 1 registro válido"
        assert resultados['df_errores'].empty, "No debería haber errores"
        
        print("✅ Prueba exitosa: Estructura mínima procesada correctamente")
        
    finally:
        shutil.rmtree(test_dir)

if __name__ == "__main__":
    # Ejecutar todas las pruebas
    test_escenario_completo()
    test_archivo_vacio()
    test_estructura_minima()
    
    print("\n🔥 Todas las pruebas completadas 🔥")