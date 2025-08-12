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

# import os
# import shutil
# import pandas as pd
# from datetime import datetime
# from limpieza_pagos import procesar_pagos

# def crear_archivo_prueba(ruta, contenido, separador='\t'):
#     """Crea archivos de prueba temporales"""
#     df = pd.DataFrame(contenido)
#     df.to_csv(ruta, sep=separador, index=False, encoding='ISO-8859-1')

# def test_escenario_completo():
#     """Prueba completa con diferentes tipos de archivos"""
#     print("\n=== TEST ESCENARIO COMPLETO ===")
    
#     # Crear carpeta temporal de prueba
#     test_dir = "test_data_" + datetime.now().strftime("%Y%m%d_%H%M%S")
#     os.makedirs(test_dir, exist_ok=True)
    
#     try:
#         # 1. Archivo válido completo
#         crear_archivo_prueba(
#             os.path.join(test_dir, "pagos_validos.txt"),
#             {
#                 'id_usuario': ['123456', '789012', '345678'],
#                 'nombres': ['Juan', 'María', 'Pedro'],
#                 'apellidos': ['Pérez', 'Gómez', 'López'],
#                 'fecha_liquida': ['01/01/2023', '15/01/2023 10:30:00', '2023-01-20'],
#                 'compute_0004': [150000, 200000, 180000],
#                 'intereses': [5000, 0, 2000],
#                 'nro_comparendo': ['CMP2023001', 'CMP2023002', 'CMP2023003'],
#                 'nro_resolucion': ['RES001', 'RES002', 'RES003']
#             }
#         )
        
#         # 2. Archivo con errores
#         crear_archivo_prueba(
#             os.path.join(test_dir, "pagos_con_errores.txt"),
#             {
#                 'id_usuario': ['', '789012', 'INVALIDO'],
#                 'nombres': ['Juan', '', 'Pedro'],
#                 'apellidos': ['Pérez', 'Gómez', ''],
#                 'fecha_pago': ['INVALIDO', '15/01/2023', ''],
#                 'valor': [0, -100, 'ABC'],
#                 'nro_acuerdo': ['ACU001', 'ACU002', 'ACU003']
#             }
#         )
        
#         # 3. Archivo con formato diferente (CSV con ;)
#         crear_archivo_prueba(
#             os.path.join(test_dir, "pagos_formato_diferente.csv"),
#             {
#                 'id_usuario': ['987654', '654321'],
#                 'nombre_completo': ['Ana Torres', 'Carlos Sánchez'],
#                 'fecha_liquida_contrav': ['2023-02-01 08:15:00', '01/02/2023'],
#                 'valor_total': [250000, 300000],
#                 'numero_comparendo': ['CMP2023004', 'CMP2023005']
#             },
#             separador=';'
#         )
        
#         # Procesar la carpeta de prueba
#         resultados = procesar_pagos(test_dir)
        
#         # Mostrar resultados
#         print("\nRESULTADOS DEL PROCESAMIENTO:")
#         print(f"- Archivos procesados: {len(resultados['df_limpio']['archivo_origen'].unique())}")
#         print(f"- Registros válidos: {len(resultados['df_limpio'])}")
#         print(f"- Registros con errores: {len(resultados['df_errores'])}")
        
#         if not resultados['advertencias']:
#             print("- No hubo advertencias")
#         else:
#             print("\nADVERTENCIAS:")
#             for warn in resultados['advertencias']:
#                 print(f"  • {warn}")
        
#         # Mostrar estructura de datos válidos
#         print("\nMUESTRA DE DATOS VÁLIDOS:")
#         print(resultados['df_limpio'].head(3))
        
#         # Mostrar errores encontrados
#         if not resultados['df_errores'].empty:
#             print("\nERRORES DETECTADOS:")
#             print(resultados['df_errores'].head())
        
#         # Verificar estructura final
#         columnas_esperadas = [
#             'id_registro', 'nro_acuerdo', 'nro_comparendo', 'documento',
#             'nombre_usuario', 'valor', 'fecha_pago', 'archivo_origen',
#             'identificador_infraccion', 'fecha_carga'
#         ]
        
#         assert all(col in resultados['df_limpio'].columns for col in columnas_esperadas), \
#             "❌ Error: Faltan columnas requeridas"
        
#         print("\n✅ Prueba exitosa: Todos los campos requeridos están presentes")
        
#     finally:
#         # Limpiar carpeta temporal
#         shutil.rmtree(test_dir)

# def test_archivo_vacio():
#     """Prueba con archivo vacío"""
#     print("\n=== TEST ARCHIVO VACÍO ===")
    
#     test_dir = "test_vacio_" + datetime.now().strftime("%Y%m%d_%H%M%S")
#     os.makedirs(test_dir, exist_ok=True)
    
#     try:
#         # Crear archivo vacío
#         open(os.path.join(test_dir, "vacio.txt"), 'w').close()
        
#         resultados = procesar_pagos(test_dir)
        
#         print("Resultados:")
#         print(f"- Válidos: {len(resultados['df_limpio'])}")
#         print(f"- Errores: {len(resultados['df_errores'])}")
        
#         assert resultados['df_limpio'].empty, "Se detectaron registros válidos en archivo vacío"
#         assert "Error procesando archivo" in resultados['advertencias'][0], \
#             "No se generó advertencia para archivo vacío"
        
#         print("✅ Prueba exitosa: Manejo correcto de archivo vacío")
        
#     finally:
#         shutil.rmtree(test_dir)

# def test_estructura_minima():
#     """Prueba con estructura mínima válida"""
#     print("\n=== TEST ESTRUCTURA MÍNIMA ===")
    
#     test_dir = "test_minimo_" + datetime.now().strftime("%Y%m%d_%H%M%S")
#     os.makedirs(test_dir, exist_ok=True)
    
#     try:
#         crear_archivo_prueba(
#             os.path.join(test_dir, "minimo_valido.txt"),
#             {
#                 'id_usuario': ['111111'],
#                 'nombres': ['Mínimo'],
#                 'apellidos': ['Válido'],
#                 'valor': [100000],
#                 'fecha_liquida': ['01/01/2023']
#             }
#         )
        
#         resultados = procesar_pagos(test_dir)
        
#         print("Resultados:")
#         print(f"- Válidos: {len(resultados['df_limpio'])}")
#         print(f"- Errores: {len(resultados['df_errores'])}")
        
#         assert len(resultados['df_limpio']) == 1, "Debería haber 1 registro válido"
#         assert resultados['df_errores'].empty, "No debería haber errores"
        
#         print("✅ Prueba exitosa: Estructura mínima procesada correctamente")
        
#     finally:
#         shutil.rmtree(test_dir)

# if __name__ == "__main__":
#     # Ejecutar todas las pruebas
#     test_escenario_completo()
#     test_archivo_vacio()
#     test_estructura_minima()
    
#     print("\n🔥 Todas las pruebas completadas 🔥")

# import pandas as pd
# from tkinter import Tk, filedialog
# from datetime import datetime
# import hashlib
# import numpy as np
# from typing import Tuple
# import re
# from limpieza_bases import preparar_datos_bases

# def prueba_interactiva():
#     # Configurar la ventana para seleccionar archivo
#     root = Tk()
#     root.withdraw()  # Ocultar la ventana principal
    
#     print("=== PRUEBA INTERACTIVA DE preparar_datos_bases ===")
#     print("Por favor, selecciona un archivo Excel para procesar")
    
#     # Permitir seleccionar archivo
#     ruta_archivo = filedialog.askopenfilename(
#         title="Seleccionar archivo Excel",
#         filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")]
#     )
    
#     if not ruta_archivo:
#         print("No se seleccionó ningún archivo. Prueba cancelada.")
#         return
    
#     nombre_archivo = ruta_archivo.split('/')[-1]
    
#     print(f"\nProcesando archivo: {nombre_archivo}")
#     print("Por favor espera...\n")
    
#     # Procesar el archivo
#     try:
#         df_procesado, df_errores, mensaje = preparar_datos_bases(ruta_archivo, nombre_archivo)
        
#         # Mostrar resultados
#         print("=== RESULTADOS ===")
#         print(mensaje)
        
#         print("\n=== DATOS PROCESADOS (primeras filas) ===")
#         print(df_procesado.head())
        
#         print("\n=== DATOS CON ERRORES (primeras filas) ===")
#         print(df_errores.head())
        
#         # Opción para guardar resultados
#         guardar = input("\n¿Deseas guardar los resultados? (s/n): ").lower()
#         if guardar == 's':
#             timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#             df_procesado.to_excel(f"resultados_procesados_{timestamp}.xlsx", index=False)
#             df_errores.to_excel(f"resultados_errores_{timestamp}.xlsx", index=False)
#             print("Resultados guardados en archivos Excel.")
            
#     except Exception as e:
#         print(f"\nERROR DURANTE EL PROCESAMIENTO: {str(e)}")

# if __name__ == "__main__":
#     prueba_interactiva()

# import os
# import pandas as pd
# from Procesador_pagos import (
#     detectar_encoding,
#     parse_fecha,
#     limpiar_nombres,
#     procesar_archivo_ap,
#     procesar_archivo_comparendos,
#     procesar_lote_archivos
# )
# import tempfile
# import pytest

# # Configuración de logging para pruebas
# import logging
# logging.basicConfig(level=logging.DEBUG)
# logger = logging.getLogger(__name__)

# # Directorio de prueba
# TEST_DIR = os.path.join(os.path.dirname(__file__), 'test_files')
# os.makedirs(TEST_DIR, exist_ok=True)

# def crear_archivo_prueba(nombre: str, contenido: str, encoding: str = 'utf-8') -> str:
#     """Crea un archivo de prueba temporal"""
#     ruta = os.path.join(TEST_DIR, nombre)
#     with open(ruta, 'w', encoding=encoding) as f:
#         f.write(contenido)
#     return ruta

# def test_detectar_encoding():
#     """Prueba la detección de encoding"""
#     # Crear archivo con encoding específico
#     contenido = "nro_acuerdo\tid_usuario\tnombres\tapellidos\tvalor\tfecha_liquida\tconsecutivo_cuota\n"
#     contenido += "123\t456\tJuan\tPérez\t100000\t2023-01-15\t1"
    
#     # UTF-8
#     ruta_utf8 = crear_archivo_prueba('test_utf8.txt', contenido, 'utf-8')
#     assert detectar_encoding(ruta_utf8).lower() == 'utf-8'
    
#     # Latin-1
#     ruta_latin1 = crear_archivo_prueba('test_latin1.txt', contenido, 'latin-1')
#     assert detectar_encoding(ruta_latin1).lower() in ['latin-1', 'iso-8859-1']

# def test_parse_fecha():
#     """Prueba el parseo de diferentes formatos de fecha"""
#     from datetime import datetime
    
#     # Formatos esperados
#     assert parse_fecha("2023-01-15") == datetime(2023, 1, 15)
#     assert parse_fecha("15/01/2023") == datetime(2023, 1, 15)
#     assert parse_fecha("15-01-2023") == datetime(2023, 1, 15)
#     assert parse_fecha("20230115") == datetime(2023, 1, 15)
    
#     # Valores inválidos
#     assert parse_fecha("") is None
#     assert parse_fecha("fecha inválida") is None
#     assert parse_fecha("15/13/2023") is None  # Mes inválido

# def test_limpiar_nombres():
#     """Prueba la limpieza de caracteres especiales en nombres"""
#     df = pd.DataFrame({
#         'nombres': ['María', 'J0sé', 'Ana$', 'L@ura', None],
#         'apellidos': ['Gómez', 'Pérez*', 'Sánchez', 'Díaz#', 'López']
#     })
    
#     df_limpio = limpiar_nombres(df)
    
#     assert df_limpio['nombres'].tolist() == ['María', 'Jsé', 'Ana', 'Laura', None]
#     assert df_limpio['apellidos'].tolist() == ['Gómez', 'Pérez', 'Sánchez', 'Díaz', 'López']

# def test_procesar_archivo_ap():
#     """Prueba el procesamiento de archivos AP"""
#     contenido = """nro_acuerdo\tid_usuario\tnombres\tapellidos\tvalor\tfecha_liquida\tconsecutivo_cuota
# 123\t456\tJuan\tPérez\t100000\t2023-01-15\t1
# 124\t789\tMaría\tGómez\t150000\t15/01/2023\t2"""
    
#     ruta_ap = crear_archivo_prueba('test_ap.txt', contenido)
#     df = procesar_archivo_ap(ruta_ap)
    
#     # Verificaciones básicas
#     assert len(df) == 2
#     assert 'nombre_usuario' in df.columns
#     assert df['valor'].sum() == 250000
#     assert df['tipo_pago'].unique()[0] == 'AP'
#     assert not df['id_registro'].duplicated().any()  # IDs únicos

# def test_procesar_archivo_comparendos():
#     """Prueba el procesamiento de archivos COMP"""
#     contenido = """nro_comparendo\tnro_recibo\tfecha_liquida_contrav\tcompute_0004\tid_usuario\tnombres\tapellidos\tnro_resolucion\tintereses
# C001\tR001\t2023-01-15\t200000\t123\tCarlos\tSánchez\tRES001\t50000
# C002\tR002\t15/01/2023\t300000\t456\tLuisa\tDíaz\tRES002\t75000"""
    
#     ruta_comp = crear_archivo_prueba('test_comp.txt', contenido)
#     df = procesar_archivo_comparendos(ruta_comp)
    
#     # Verificaciones básicas
#     assert len(df) == 2
#     assert 'nombre_usuario' in df.columns
#     assert df['valor'].sum() == 625000  # 200k+50k + 300k+75k
#     assert df['tipo_pago'].unique()[0] == 'COMPARENDO'
#     assert not df['id_registro'].duplicated().any()

# def test_procesar_lote_archivos():
#     """Prueba el procesamiento de múltiples archivos"""
#     # Crear archivos de prueba
#     contenido_ap = """nro_acuerdo\tid_usuario\tnombres\tapellidos\tvalor\tfecha_liquida\tconsecutivo_cuota
# 123\t456\tJuan\tPérez\t100000\t2023-01-15\t1"""
    
#     contenido_comp = """nro_comparendo\tnro_recibo\tfecha_liquida_contrav\tcompute_0004\tid_usuario\tnombres\tapellidos\tnro_resolucion\tintereses
# C001\tR001\t2023-01-15\t200000\t123\tCarlos\tSánchez\tRES001\t50000"""
    
#     ruta_ap = crear_archivo_prueba('lote_ap.txt', contenido_ap)
#     ruta_comp = crear_archivo_prueba('lote_comp.txt', contenido_comp)
#     ruta_invalida = crear_archivo_prueba('lote_invalido.txt', "contenido,invalido")
    
#     # Procesar lote
#     df_ap, df_comp = procesar_lote_archivos([ruta_ap, ruta_comp, ruta_invalida])
    
#     # Verificar resultados
#     assert len(df_ap) == 1
#     assert len(df_comp) == 1
#     assert df_ap['tipo_pago'].unique()[0] == 'AP'
#     assert df_comp['tipo_pago'].unique()[0] == 'COMPARENDO'

# def test_archivos_invalidos():
#     """Prueba el manejo de archivos inválidos"""
#     # Archivo con encoding extraño
#     ruta_encoding_rar = crear_archivo_prueba('test_encoding_rar.txt', "test\xa0data", 'latin-1')
    
#     # Archivo con estructura incorrecta
#     contenido_mal = "col1,col2,col3\n1,2,3"
#     ruta_estruct_mal = crear_archivo_prueba('test_estruct_mal.txt', contenido_mal)
    
#     # Procesar (debería fallar pero continuar)
#     df_ap, df_comp = procesar_lote_archivos([ruta_encoding_rar, ruta_estruct_mal])
    
#     assert df_ap.empty and df_comp.empty

# def generar_reporte_test(ruta_archivos: str) -> dict:
#     """
#     Función para testear el procesador con archivos reales en una ruta específica
#     Devuelve un reporte con estadísticas de los archivos procesados
    
#     Args:
#         ruta_archivos (str): Ruta donde se encuentran los archivos a procesar
        
#     Returns:
#         dict: Diccionario con estadísticas del procesamiento
#     """
#     if not os.path.exists(ruta_archivos):
#         return {"error": f"La ruta {ruta_archivos} no existe"}
    
#     # Identificar archivos en la ruta
#     archivos = [os.path.join(ruta_archivos, f) for f in os.listdir(ruta_archivos) 
#                if f.endswith('.txt') and (f.upper().startswith('AP') or f.upper().startswith('COMP'))]
    
#     if not archivos:
#         return {"error": "No se encontraron archivos AP o COMP en la ruta especificada"}
    
#     # Procesar archivos
#     resultados = {
#         "total_archivos": len(archivos),
#         "archivos_procesados": 0,
#         "archivos_fallidos": 0,
#         "registros_ap": 0,
#         "registros_comp": 0,
#         "archivos_fallidos_lista": []
#     }
    
#     try:
#         df_ap, df_comp = procesar_lote_archivos(archivos)
        
#         resultados["archivos_procesados"] = len(archivos)
#         resultados["registros_ap"] = len(df_ap) if not df_ap.empty else 0
#         resultados["registros_comp"] = len(df_comp) if not df_comp.empty else 0
        
#         # Ejemplo de cómo podrías guardar los resultados
#         if not df_ap.empty:
#             df_ap.to_csv(os.path.join(ruta_archivos, 'resultados_ap.csv'), index=False)
#         if not df_comp.empty:
#             df_comp.to_csv(os.path.join(ruta_archivos, 'resultados_comp.csv'), index=False)
            
#     except Exception as e:
#         resultados["error"] = str(e)
    
#     return resultados

# if __name__ == '__main__':
#     # Ejecutar pruebas unitarias
#     print("Ejecutando pruebas unitarias...")
#     pytest.main([__file__, '-v'])
    
#     # Ejemplo de uso con archivos reales
#     ruta_ejemplo = input("\nIngrese la ruta de archivos a procesar (deje vacío para omitir): ").strip()
    
#     if ruta_ejemplo:
#         print(f"\nProcesando archivos en {ruta_ejemplo}...")
#         reporte = generar_reporte_test(ruta_ejemplo)
        
#         print("\nReporte de procesamiento:")
#         for k, v in reporte.items():
#             print(f"{k}: {v}")
        
#         if 'error' not in reporte:
#             print("\nArchivos de resultados generados en la misma ruta:")
#             print("- resultados_ap.csv")
#             print("- resultados_comp.csv")
#     else:
#         print("\nPruebas completadas. No se procesaron archivos reales.")


