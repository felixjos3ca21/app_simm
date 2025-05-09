# import os
# import pandas as pd
# import numpy as np
# from datetime import datetime
# import hashlib
# from time import time

# pd.options.display.float_format = '{:.0f}'.format

# # Ruta base donde están los archivos
# CARPETA = r"C:\Users\1513873\ANDES BPO S.A.S\Johan Felipe JG. Gómez Arango - SIMM\BASES\2025\ENERO\02_01_2025 - copia"

# # ================================================
# # Función para mostrar el paso actual
# # ================================================
# def update_step(step_name: str):
#     print(f"\n🛠️ Paso: {step_name}")

# # ================================================
# # Paso 1-2: Configuración inicial y lectura archivo
# # ================================================
# def limpieza_pagos(ruta_archivo: str, archivo: str) -> pd.DataFrame:
#     update_step(f"Lectura y transformación del archivo: {archivo}")
    
#     try:
#         df = pd.read_csv(ruta_archivo, sep='\t', encoding='ISO-8859-1', header=0, dtype=str)
#     except Exception as e:
#         raise ValueError(f"No se pudo leer el archivo {archivo}: {str(e)}")
    
#     df = df.copy()
    
#     if archivo.startswith('Ap pagados'):
#         columnas = ['nro_acuerdo', 'id_usuario', 'nombres', 'apellidos', 'valor', 'fecha_liquida','consecutivo_cuota']
#         df = df[[col for col in columnas if col in df.columns]]
#         df = df.rename(columns={'fecha_liquida': 'fecha_pago','id_usuario': 'documento'})
#         df['nombre_usuario'] = df['nombres'].str.cat(df['apellidos'], sep=' ', na_rep='').str.strip()
#         df = df.drop(columns=['nombres', 'apellidos'], errors='ignore')
#         df['nro_acuerdo'] = df['nro_acuerdo'].astype(str)
#         df['documento'] = df['documento'].astype(str)

#     elif archivo.startswith('Comparendos pagados'):
#         columnas = ['nro_comparendo', 'nro_recibo', 'fecha_liquida_contrav',
#                     'compute_0004', 'id_usuario', 'nombres', 'apellidos',
#                     'nro_resolucion', 'intereses']
#         df = df[[col for col in columnas if col in df.columns]]
#         df = df.rename(columns={'fecha_liquida_contrav': 'fecha_pago', 'id_usuario': 'documento'})
#         df['documento'] = df['documento'].astype(str)

#         df['nro_comparendo'] = np.where(
#             df['nro_comparendo'].isnull() | (df['nro_comparendo'] == ''),
#             df['nro_resolucion'], df['nro_comparendo']
#         )

#         df['nombre_usuario'] = df['nombres'].str.cat(df['apellidos'], sep=' ', na_rep='').str.strip()
#         df = df.drop(columns=['nombres', 'apellidos'], errors='ignore')

#         df['compute_0004'] = pd.to_numeric(df['compute_0004'], errors='coerce')
#         df['intereses'] = pd.to_numeric(df['intereses'], errors='coerce')
#         df['valor'] = (df['compute_0004'].fillna(0) + df['intereses'].fillna(0)).astype(int).astype(str)

#         for col in ['nro_comparendo', 'nro_recibo', 'nro_resolucion']:
#             df[col] = df[col].astype(str)

#     df['archivo_origen'] = archivo
#     return df

# # ================================================
# # Paso 3: Procesar todos los archivos
# # ================================================
# def procesar_archivos(carpeta: str) -> list:
#     update_step("Procesamiento de todos los archivos individuales")
#     archivos = [f for f in os.listdir(carpeta) if f.lower().endswith('.txt')]
#     dataframes = []

#     for archivo in archivos:
#         ruta = os.path.join(carpeta, archivo)
#         try:
#             df = limpieza_pagos(ruta, archivo)
#             dataframes.append(df)
#             print(f"✅ {archivo}: {len(df)} registros")
#         except Exception as e:
#             print(f"❌ Error en {archivo}: {str(e)}")

#     return dataframes

# # ================================================
# # Paso 4-5: Concatenación y Validaciones
# # ================================================
# def concatenar_y_enriquecer(dataframes: list) -> pd.DataFrame:
#     update_step("Concatenación de archivos y generación de campos unificados")
    
#     if not dataframes:
#         raise ValueError("No hay archivos válidos para concatenar.")

#     df_final = pd.concat(dataframes, ignore_index=True)

#     # Generar columna 
#     df_final['identificador_infraccion'] = df_final['nro_comparendo'].fillna(df_final.get('nro_acuerdo', ''))

#     # Validar y limpiar fechas
#     df_final['fecha_pago'] = pd.to_datetime(df_final['fecha_pago'], errors='coerce')
#     fechas_invalidas = df_final['fecha_pago'].isna().sum()
#     if fechas_invalidas > 0:
#         print(f"⚠️ {fechas_invalidas} registros con fecha_pago inválida")

#     # Paso 7: Generación de ID único
#     update_step("Creando ID único por registro")

#     grupo = [
#         'documento', 
#         'nombre_usuario', 
#         'valor', 
#         'fecha_pago',
#         'archivo_origen', 
#         'nro_acuerdo', 
#         'nro_comparendo',
#         'consecutivo_cuota'
#     ]

#     # Crear contador por grupo para evitar duplicados exactos
#     df_final['contador_grupo'] = df_final.groupby(grupo).cumcount() + 1

#     # Generar ID único con SHA-256
#     df_final['id_registro'] = df_final.apply(
#         lambda x: hashlib.sha256((
#             f"{x.get('documento', '')}_"
#             f"{x.get('nombre_usuario', '')}_"
#             f"{x.get('valor', '')}_"
#             f"{x['fecha_pago'].timestamp() if pd.notnull(x.get('fecha_pago')) else ''}_"
#             f"{x.get('archivo_origen', '')}_"
#             f"{x.get('nro_acuerdo', '')}_"
#             f"{x.get('nro_comparendo', '')}_"
#             f"{x.get('consecutivo_cuota', '')}_"
#             f"{x['contador_grupo']}"
#         ).encode()).hexdigest(),
#         axis=1
#     )

#     df_final.drop('contador_grupo', axis=1, inplace=True)

#     # Validación de duplicados
#     update_step("Validación final de duplicados")
#     registros_duplicados = df_final.duplicated(subset=['id_registro']).sum()
#     print(f"🧾 Duplicados encontrados: {registros_duplicados}")

#     return df_final

# # ================================================
# # Ejecución principal
# # ================================================
# def ejecutar_limpieza_general():
#     try:
#         update_step("Inicio del proceso de limpieza")
#         dataframes = procesar_archivos(CARPETA)
#         df_limpio = concatenar_y_enriquecer(dataframes)

#         print("\n📄 DATA FINAL LIMPIA")
#         print(f"Registros totales: {len(df_limpio):,}")
#         print(df_limpio.head())
#         duplicados = df_limpio[df_limpio.duplicated(subset=['id_registro'], keep=False)]
#         print(f"\n🔍 Registros duplicados (total: {len(duplicados)}):")
#         print(duplicados.sort_values(by='id_registro').head(10))
#         return df_limpio
  
#     except Exception as e:
#         print(f"❌ Error general: {str(e)}")
#         return pd.DataFrame()

# # Para ejecutar
# df_final_limpio = ejecutar_limpieza_general()

#------------------------------
# Fin del script ------------------------------


# import os
# import pandas as pd
# import numpy as np
# from datetime import datetime
# import hashlib
# from typing import Tuple, List, Optional, Callable
# from charset_normalizer import detect
# import re
# import traceback
# import psycopg2
# import sqlalchemy as sa
# from sqlalchemy import inspect

# # ================================================
# # CONFIGURACIONES
# # ================================================
# CAMPOS_OBLIGATORIOS = ['documento', 'valor', 'fecha_pago']
# REGEX_DOCUMENTO = r'^\d{6,20}$'
# TOTAL_PASOS = 7  # Pasos principales del proceso

# # ================================================
# # FUNCIONES DE PROGRESO
# # ================================================
# def update_step(
#     current_step: int,
#     total_steps: int,
#     message: str,
#     update_progress: Optional[Callable[[float, str], None]] = None
# ) -> int:
#     """Actualiza el progreso del proceso"""
#     if update_progress:
#         progress = current_step / total_steps
#         update_progress(progress, f"Paso {current_step}/{total_steps}: {message}")
#     print(f"\n🔄 [Paso {current_step}/{total_steps}] {message}")
#     return current_step + 1

# # ================================================
# # FUNCIONES PRINCIPALES 
# # ================================================
# def procesar_archivo_pago(
#     ruta_archivo: str,
#     nombre_archivo: str,
#     update_progress: Optional[Callable[[float, str], None]] = None
# ) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
#     """
#     Procesa un archivo individual de pagos con seguimiento de progreso
#     """
#     paso_actual = 1
#     try:
#         # Paso 1: Lectura del archivo con más información de diagnóstico
#         paso_actual = update_step(paso_actual, TOTAL_PASOS, "Leyendo archivo", update_progress)
        
#         # 1. Detectar encoding y separador
#         with open(ruta_archivo, 'rb') as f:
#             raw_data = f.read(10000)  # Leer solo los primeros 10KB para análisis
#             encoding = detect(raw_data)['encoding']
            
#             # Intentar detectar el separador
#             first_lines = raw_data.decode(encoding, errors='ignore').split('\n')[:5]
#             possible_seps = [',', ';', '\t', '|']
#             sep_counts = {sep: sum(line.count(sep) for line in first_lines for sep in possible_seps)}
#             detected_sep = max(sep_counts.items(), key=lambda x: x[1])[0] if any(sep_counts.values()) else ','
            
#             st.write(f"🔍 Detectado: Encoding={encoding}, Separador='{detected_sep}'")

#         # 2. Leer archivo con diferentes enfoques si falla
#         try:
#             df = pd.read_csv(
#                 ruta_archivo, 
#                 sep=detected_sep, 
#                 encoding=encoding,  
#                 dtype=str, 
#                 na_values=['', 'NA', 'NULL', 'N/A', 'null'],
#                 on_bad_lines='warn'
#             )
#         except Exception as e:
#             st.warning(f"Intento 1 falló ({str(e)}). Probando lectura directa...")
#             df = pd.read_table(ruta_archivo, encoding=encoding, dtype=str)
            
#         # 3. Verificar datos leídos
#         st.write(f"📄 Muestra de datos crudos ({nombre_archivo}):")
#         st.dataframe(df.head(3))
#         st.write(f"Columnas detectadas: {df.columns.tolist()}")
#         st.write(f"Número de filas: {len(df)}")

#         if df.empty:
#             raise ValueError("El DataFrame está vacío después de la lectura")
        
#          # En procesar_archivo_pago, después de leer el CSV:
#         print(f"\n📂 Muestra de datos crudos ({nombre_archivo}):")
#         print(df.head(3).to_markdown())
        
        
#         # Paso 2: Procesamiento inicial
#         paso_actual = update_step(paso_actual, TOTAL_PASOS, "Clasificando tipo de archivo", update_progress)
#         print(f"\n🔧 Antes de procesar {nombre_archivo}: {df.columns.tolist()}")
#         if nombre_archivo.startswith('Ap pagados'):
#             df_procesado = _procesar_acuerdos(df)
#         elif nombre_archivo.startswith('Comparendos pagados'):
#             df_procesado = _procesar_comparendos(df)
#         else:
#             raise ValueError("Tipo de archivo no reconocido")
#         print(f"\n✅ Después de procesar {nombre_archivo}:")
#         print(df_procesado.head(3).to_markdown())

#         # Paso 3: Validación de registros
#         paso_actual = update_step(paso_actual, TOTAL_PASOS, "Validando registros", update_progress)
#         registros_validos = []
#         registros_error = []
        
#         for idx, fila in df_procesado.iterrows():
#             try:
#                 _validar_registro(fila)
#                 registros_validos.append(fila.to_dict())
#             except Exception as e:
#                 registros_error.append({
#                     'archivo': nombre_archivo,
#                     'error': str(e),
#                     'registro': fila.to_dict()
#                 })

#         # Paso 4: Generación de metadatos
#         paso_actual = update_step(paso_actual, TOTAL_PASOS, "Agregando metadatos", update_progress)
#         df_valido = pd.DataFrame(registros_validos)
#         if not df_valido.empty:
#             df_valido = _agregar_metadatos(df_valido, nombre_archivo)

#         # Paso 5: Generación de ID único
#         paso_actual = update_step(paso_actual, TOTAL_PASOS, "Generando IDs únicos", update_progress)
#         if not df_valido.empty:
#             df_valido = _generar_id_unico(df_valido)

#         # Paso 6: Finalización
#         update_step(paso_actual, TOTAL_PASOS, "Proceso completado", update_progress)
#         if not df_valido.empty:
#             df_valido = df_valido[COLUMNAS_FINALES]
#         return df_valido, pd.DataFrame(registros_error), "OK"

#     except Exception as e:
#         error_msg = f"{str(e)}\nTraceback:\n{traceback.format_exc()}"
#         print(f"\n❌ ERROR CRÍTICO EN {nombre_archivo}:")
#         traceback.print_exc()
#         error = pd.DataFrame([{
#             'archivo': nombre_archivo,
#             'error': error_msg,
#             'registro': None
#         }])
#         return pd.DataFrame(), error, str(e)
        
   

# def procesar_carpeta_pagos(
#     carpeta: str,
#     update_progress: Optional[Callable[[float, str], None]] = None
# ) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
#     """
#     Procesa todos los archivos en una carpeta con seguimiento de progreso
#     """
    
#     paso_actual = 1
#     archivos = [f for f in os.listdir(carpeta) if f.lower().endswith('.txt')]
#     total_archivos = len(archivos)
#     data_valida = []
#     data_errores = []
#     archivos_con_error = []

#     # Paso 1: Preparación
#     paso_actual = update_step(paso_actual, TOTAL_PASOS, "Iniciando procesamiento de carpeta", update_progress)

#     for i, archivo in enumerate(archivos):
#         # Actualizar progreso general
#         if update_progress:
#             progress = (i + 1) / total_archivos
#             update_progress(progress, f"Procesando archivo {i+1}/{total_archivos}: {archivo}")

#         ruta_completa = os.path.join(carpeta, archivo)
#         df_valido, df_error, mensaje = procesar_archivo_pago(ruta_completa, archivo, update_progress)
        
#         if not df_valido.empty:
#             data_valida.append(df_valido)
#         if not df_error.empty:
#             data_errores.append(df_error)
#         if mensaje != "OK":
#             archivos_con_error.append(archivo)

#     # Paso final: Consolidación
#     paso_actual = update_step(TOTAL_PASOS, TOTAL_PASOS, "Finalizando proceso", update_progress)
#     df_consolidado = pd.concat(data_valida, ignore_index=True) if data_valida else pd.DataFrame()
#     df_errores = pd.concat(data_errores, ignore_index=True) if data_errores else pd.DataFrame()
    
#     return df_consolidado, df_errores, archivos_con_error

# # ================================================
# # FUNCIONES DE APOYO
# # ================================================

# COLUMNAS_FINALES = [
#     "id_registro",
#     "nro_acuerdo",
#     "nro_comparendo",
#     "documento",
#     "nombre_usuario",
#     "valor",
#     "fecha_pago",
#     "archivo_origen",
#     "identificador_infraccion",
#     "fecha_carga"
# ]


# def _procesar_acuerdos(df: pd.DataFrame) -> pd.DataFrame:
#     """Lógica específica para archivos de acuerdos de pago"""
#     # Mapeo y limpieza
#     df = df.rename(columns={
#         'fecha_liquida': 'fecha_pago',
#         'id_usuario': 'documento'
#     }).copy()
    
#     # Generar campos necesarios
#     df['nombre_usuario'] = (df['nombres'] + ' ' + df['apellidos']).str.strip()
#     df['nro_comparendo'] = None  # Valor por defecto para acuerdos
#     df['identificador_infraccion'] = df['nro_acuerdo']
    
#     # Convertir tipos de datos
#     df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
#     df['fecha_pago'] = pd.to_datetime(df['fecha_pago'], errors='coerce')
    
#     return df[COLUMNAS_FINALES + ['consecutivo_cuota']]

# def _procesar_comparendos(df: pd.DataFrame) -> pd.DataFrame:
#     """Lógica específica para archivos de comparendos"""
#     # Cálculo de valores
#     df['valor'] = pd.to_numeric(df['compute_0004'], errors='coerce') + pd.to_numeric(df['intereses'], errors='coerce')
#     df['identificador_infraccion'] = df['nro_comparendo'].fillna(df['nro_resolucion'])
    
#     # Renombrar y limpiar
#     df = df.rename(columns={
#         'fecha_liquida_contrav': 'fecha_pago',
#         'id_usuario': 'documento'
#     }).copy()
    
#     # Generar campos
#     df['nombre_usuario'] = (df['nombres'] + ' ' + df['apellidos']).str.strip()
#     df['nro_acuerdo'] = None  # Valor por defecto para comparendos
#     df['fecha_pago'] = pd.to_datetime(df['fecha_pago'], errors='coerce')
    
#     return df[COLUMNAS_FINALES]

# def _validar_registro(fila: pd.Series):
#     """Valida un registro individual"""
    
#     # Campos obligatorios
#     if any(pd.isna(fila[col]) for col in CAMPOS_OBLIGATORIOS):
#         raise ValueError("Campos obligatorios faltantes")
    
#     if pd.isna(fila['nro_comparendo']) and fila['tipo_registro'] == 'COMPARENDO':
#         raise ValueError("Comparendo requiere nro_comparendo")
    
#     # Formato documento (CORREGIDO)
#     documento = str(fila['documento']).strip()
#     if not re.fullmatch(REGEX_DOCUMENTO, documento):
#         raise ValueError(f"Documento inválido: {documento}")
    
#     # Valor positivo
#     if float(fila['valor']) <= 0:
#         raise ValueError("El valor debe ser positivo")

# def _agregar_metadatos(df: pd.DataFrame, nombre_archivo: str) -> pd.DataFrame:
#     """Agrega metadatos de procesamiento"""
#     df['archivo_origen'] = nombre_archivo[:60]
#     df['fecha_carga'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#     return df

# def _generar_id_unico(df: pd.DataFrame) -> pd.DataFrame:
#     """Genera ID único SHA-256 para cada registro"""
#     if 'consecutivo_cuota' not in df.columns:
#         df['consecutivo_cuota'] = None  

#     grupo = [
#         'documento', 
#         'nombre_usuario', 
#         'valor', 
#         'fecha_pago',
#         'archivo_origen', 
#         'nro_acuerdo', 
#         'nro_comparendo',
#         'consecutivo_cuota'
#     ]

#     df['fecha_pago'] = pd.to_datetime(df['fecha_pago']).dt.strftime('%Y-%m-%d %H:%M:%S')
#     df['valor'] = df['valor'].round(2).astype(str)

#     df['contador_grupo'] = df.groupby(grupo).cumcount() + 1

#     # Crear string único
#     df['unique_string'] = df.apply(
#         lambda x: '|'.join([
#             str(x['documento']),
#             str(x['nombre_usuario']),
#             str(x['valor']),
#             str(x['fecha_pago']),
#             str(x.get('nro_acuerdo', '')),
#             str(x.get('nro_comparendo', '')),
#             str(x.get('consecutivo_cuota', ''))
#         ]), axis=1
#     )
    
#     # Generar hash SHA-256
#     df['id_registro'] = df['unique_string'].apply(
#         lambda x: hashlib.sha256(x.encode()).hexdigest()
#     )
    
#     return df.drop(columns=['unique_string', 'consecutivo_cuota'])

import pandas as pd
from datetime import datetime
import hashlib
import numpy as np
from typing import Tuple

def procesar_pagos(ruta_archivo: str, nombre_archivo: str, update_progress=None) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    """
    Procesa archivos de pagos (TXT) y los prepara para carga en la tabla pagos.
    
    Args:
        ruta_archivo: Ruta completa del archivo a procesar
        nombre_archivo: Nombre del archivo (para metadatos)
        update_progress: Función callback para actualizar progreso (opcional)
        
    Returns:
        Tuple con:
        - DataFrame de registros válidos
        - DataFrame de registros con errores
        - Mensajes de advertencia/errores
    """
    try:
        # =============================================================================
        # 1. Configuración inicial
        # =============================================================================
        total_steps = 12
        current_step = 0
        warnings = []
        
        def update_step(message: str):
            nonlocal current_step
            current_step += 1
            progress = current_step / total_steps
            progress = min(progress, 1.0)
            if update_progress:
                update_progress(progress, f"Paso {current_step}/{total_steps}: {message}")

        update_step("Iniciando procesamiento de pagos")
        
        # =============================================================================
        # 2. Lectura del archivo
        # =============================================================================
        update_step("Leyendo archivo TXT")
        try:
            # Leer manteniendo todo como texto para evitar problemas con números grandes
            df = pd.read_csv(ruta_archivo, sep='\t', encoding='ISO-8859-1', header=0, dtype=str)
        except Exception as e:
            raise ValueError(f"Error leyendo archivo: {str(e)}")

        # =============================================================================
        # 3. Procesamiento según tipo de archivo
        # =============================================================================
        update_step("Identificando tipo de archivo")
        
        if nombre_archivo.startswith('Ap pagados'):
            # =============================================
            # Transformaciones para archivo AP PAGADOS
            # =============================================
            update_step("Procesando archivo de Acuerdos de Pago")
            
            columnas_requeridas = [
                'nro_acuerdo', 'id_usuario', 'nombres', 
                'apellidos', 'valor', 'fecha_liquida', 'consecutivo_cuota'
            ]
            
            # Verificar columnas existentes
            columnas_faltantes = set(columnas_requeridas) - set(df.columns)
            if columnas_faltantes:
                warnings.append(f"Advertencia: Faltan columnas en archivo AP: {', '.join(columnas_faltantes)}")
            
            columnas_disponibles = [col for col in columnas_requeridas if col in df.columns]
            df = df[columnas_disponibles]
            
            # Renombrar columna
            if 'fecha_liquida' in df.columns:
                df = df.rename(columns={'fecha_liquida': 'fecha_pago'})
            
            # Crear nombre_usuario
            if 'nombres' in df.columns and 'apellidos' in df.columns:
                df['nombre_usuario'] = df['nombres'].str.cat(df['apellidos'], sep=' ', na_rep='').str.strip()
                df = df.drop(columns=['nombres', 'apellidos'])
            else:
                df['nombre_usuario'] = ''
                warnings.append("Advertencia: No se encontraron columnas de nombres/apellidos")
            
            # Forzar tipo texto para valores numéricos grandes
            if 'nro_acuerdo' in df.columns:
                df['nro_acuerdo'] = df['nro_acuerdo'].astype(str)
            if 'id_usuario' in df.columns:
                df['documento'] = df['id_usuario'].astype(str)  # Mapeamos a documento
            else:
                df['documento'] = ''
                warnings.append("Advertencia: No se encontró columna id_usuario (documento)")
            
            # Agregar campos faltantes para consistencia
            df['nro_comparendo'] = ''
            df['identificador_infraccion'] = df['nro_acuerdo']
            
        elif nombre_archivo.startswith('Comparendos pagados'):
            # =============================================
            # Transformaciones para archivo COMPARENDOS
            # =============================================
            update_step("Procesando archivo de Comparendos")
            
            columnas_requeridas = [
                'nro_comparendo', 'nro_recibo', 'fecha_liquida_contrav',
                'compute_0004', 'id_usuario', 'nombres', 'apellidos',
                'nro_resolucion', 'intereses'
            ]
            
            # Verificar columnas existentes
            columnas_faltantes = set(columnas_requeridas) - set(df.columns)
            if columnas_faltantes:
                warnings.append(f"Advertencia: Faltan columnas en archivo Comparendos: {', '.join(columnas_faltantes)}")
            
            columnas_disponibles = [col for col in columnas_requeridas if col in df.columns]
            df = df[columnas_disponibles]
            
            # Renombrar columna
            if 'fecha_liquida_contrav' in df.columns:
                df = df.rename(columns={'fecha_liquida_contrav': 'fecha_pago'})
            
            # Llenar nro_comparendo con nro_resolucion si está vacío
            if 'nro_comparendo' in df.columns and 'nro_resolucion' in df.columns:
                df['nro_comparendo'] = np.where(
                    df['nro_comparendo'].isnull() | (df['nro_comparendo'] == ''),
                    df['nro_resolucion'],
                    df['nro_comparendo']
                )
            elif 'nro_resolucion' in df.columns:
                df['nro_comparendo'] = df['nro_resolucion']
            else:
                df['nro_comparendo'] = ''
                warnings.append("Advertencia: No se encontró columna nro_comparendo o nro_resolucion")
            
            # Crear nombre_usuario
            if 'nombres' in df.columns and 'apellidos' in df.columns:
                df['nombre_usuario'] = df['nombres'].str.cat(df['apellidos'], sep=' ', na_rep='').str.strip()
                df = df.drop(columns=['nombres', 'apellidos'])
            else:
                df['nombre_usuario'] = ''
                warnings.append("Advertencia: No se encontraron columnas de nombres/apellidos")
            
            # Crear columna valor (compute_0004 + intereses)
            try:
                if 'compute_0004' in df.columns and 'intereses' in df.columns:
                    # Convertir a numérico
                    df['compute_0004'] = pd.to_numeric(df['compute_0004'], errors='coerce').fillna(0)
                    df['intereses'] = pd.to_numeric(df['intereses'], errors='coerce').fillna(0)
                    
                    # Calcular suma y convertir a string sin notación científica
                    df['valor'] = (df['compute_0004'] + df['intereses']).apply(lambda x: f"{int(x)}" if not pd.isna(x) else "")
                    
                    # Eliminar columnas temporales
                    df = df.drop(columns=['compute_0004', 'intereses'])
                else:
                    df['valor'] = ''
                    warnings.append("Advertencia: No se encontraron columnas para calcular valor (compute_0004, intereses)")
            except Exception as e:
                df['valor'] = ''
                warnings.append(f"Error en cálculos de valor: {str(e)}")
            
            # Forzar tipo texto para valores numéricos grandes
            cols_numericas = ['nro_comparendo', 'nro_recibo', 'nro_resolucion']
            for col in cols_numericas:
                if col in df.columns:
                    df[col] = df[col].astype(str)
            
            # Mapear id_usuario a documento
            if 'id_usuario' in df.columns:
                df['documento'] = df['id_usuario'].astype(str)
            else:
                df['documento'] = ''
                warnings.append("Advertencia: No se encontró columna id_usuario (documento)")
            
            # Agregar campos faltantes para consistencia
            df['nro_acuerdo'] = ''
            df['consecutivo_cuota'] = ''
            df['identificador_infraccion'] = df['nro_comparendo']
        else:
            raise ValueError("Tipo de archivo no reconocido (debe empezar con 'Ap pagados' o 'Comparendos pagados')")

        # =============================================================================
        # 4. Columnas comunes y metadatos
        # =============================================================================
        update_step("Agregando metadatos comunes")
        
        df['archivo_origen'] = nombre_archivo
        df['fecha_carga'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # =============================================================================
        # 5. Generación de ID único
        # =============================================================================
        update_step("Generando ID único para cada registro")
        
        # Campos para generar el hash único
        campos_id = [
            'nro_acuerdo', 'nro_comparendo', 'documento', 
            'nombre_usuario', 'valor', 'fecha_pago','consecutivo_cuota'
        ]
        
        # Asegurar que existan todas las columnas necesarias
        for col in campos_id:
            if col not in df.columns:
                df[col] = ''
        
        df['id_registro'] = df.apply(
            lambda x: hashlib.sha256((
                f"{x['nro_acuerdo']}_"
                f"{x['nro_comparendo']}_"
                f"{x['documento']}_"
                f"{x['nombre_usuario']}_"
                f"{x['valor']}_"
                f"{x['consecutivo_cuota']}_"
                f"{x['fecha_pago']}"
            ).encode()).hexdigest(),
            axis=1
        )

        # =============================================================================
        # 6. Validación de campos obligatorios
        # =============================================================================
        update_step("Validando campos obligatorios")
        
        campos_obligatorios = ['documento', 'nombre_usuario', 'valor', 'fecha_pago']
        
        # Verificar que existan las columnas obligatorias
        for col in campos_obligatorios:
            if col not in df.columns:
                raise ValueError(f"Falta columna obligatoria: {col}")
        
        # Convertir fecha_pago a datetime
        try:
            df['fecha_pago'] = pd.to_datetime(df['fecha_pago'], errors='coerce')
        except Exception as e:
            raise ValueError(f"Error al convertir fechas: {str(e)}")
        
        # Convertir valor a numérico
        try:
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
        except Exception as e:
            raise ValueError(f"Error al convertir valores numéricos: {str(e)}")

        # =============================================================================
        # 7. Separación de registros válidos/erróneos
        # =============================================================================
        update_step("Clasificando registros válidos/erróneos")
        
        # Identificar registros con errores (campos obligatorios vacíos o inválidos)
        mask_errores = (
            df['documento'].isna() | 
            (df['documento'] == '') | 
            df['nombre_usuario'].isna() | 
            (df['nombre_usuario'] == '') | 
            df['valor'].isna() | 
            (df['valor'] <= 0) | 
            df['fecha_pago'].isna()
        )
        
        df_errores = df[mask_errores].copy()
        df_procesado = df[~mask_errores].copy()

        # =============================================================================
        # 8. Selección y orden de columnas finales
        # =============================================================================
        update_step("Preparando estructura final")
        
        columnas_finales = [
            'id_registro', 'nro_acuerdo', 'nro_comparendo', 'documento',
            'nombre_usuario', 'valor', 'fecha_pago', 'archivo_origen',
            'identificador_infraccion', 'fecha_carga'
        ]
        
        # Seleccionar solo columnas existentes
        columnas_existentes = [col for col in columnas_finales if col in df_procesado.columns]
        df_procesado = df_procesado[columnas_existentes]
        
        if not df_errores.empty:
            columnas_existentes_err = [col for col in columnas_finales if col in df_errores.columns]
            df_errores = df_errores[columnas_existentes_err]

        # =============================================================================
        # 9. Validación final
        # =============================================================================
        update_step("Realizando validación final")
        
        # Verificar IDs únicos
        if df_procesado['id_registro'].duplicated().any():
            duplicados = df_procesado[df_procesado.duplicated('id_registro', keep=False)]
            raise ValueError(f"IDs duplicados encontrados:\n{duplicados.head()}")
            
        # Verificar que haya registros válidos
        if df_procesado.empty:
            warnings.append("Advertencia: No se encontraron registros válidos después del procesamiento")

        return df_procesado, df_errores, "\n".join(warnings)

    except Exception as e:
        error_step = f"Error en paso {current_step}" if current_step > 0 else "Error inicial"
        raise ValueError(f"{error_step}: {str(e)}")
