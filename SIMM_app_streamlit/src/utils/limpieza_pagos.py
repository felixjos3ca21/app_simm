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


import os
import pandas as pd
import numpy as np
from datetime import datetime
import hashlib
from typing import Tuple, List, Optional, Callable

# ================================================
# CONFIGURACIONES
# ================================================
CAMPOS_OBLIGATORIOS = ['documento', 'valor', 'fecha_pago']
REGEX_DOCUMENTO = r'^\d{6,20}$'
VERSION_PROCESO = "2.1"
TOTAL_PASOS = 7  # Pasos principales del proceso

# ================================================
# FUNCIONES DE PROGRESO
# ================================================
def update_step(
    current_step: int,
    total_steps: int,
    message: str,
    update_progress: Optional[Callable[[float, str], None]] = None
) -> int:
    """Actualiza el progreso del proceso"""
    if update_progress:
        progress = current_step / total_steps
        update_progress(progress, f"Paso {current_step}/{total_steps}: {message}")
    print(f"\n🔄 [Paso {current_step}/{total_steps}] {message}")
    return current_step + 1

# ================================================
# FUNCIONES PRINCIPALES (Modificadas con seguimiento de progreso)
# ================================================
def procesar_archivo_pago(
    ruta_archivo: str,
    nombre_archivo: str,
    update_progress: Optional[Callable[[float, str], None]] = None
) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    """
    Procesa un archivo individual de pagos con seguimiento de progreso
    """
    paso_actual = 1
    try:
        # Paso 1: Lectura del archivo
        paso_actual = update_step(paso_actual, TOTAL_PASOS, "Leyendo archivo", update_progress)
        df = pd.read_csv(ruta_archivo, sep='\t', encoding='ISO-8859-1', dtype=str, na_values=['', 'NA', 'NULL'])
        df = df.copy()
        
        # Paso 2: Procesamiento inicial
        paso_actual = update_step(paso_actual, TOTAL_PASOS, "Clasificando tipo de archivo", update_progress)
        if nombre_archivo.startswith('Ap pagados'):
            df_procesado = _procesar_acuerdos(df)
        elif nombre_archivo.startswith('Comparendos pagados'):
            df_procesado = _procesar_comparendos(df)
        else:
            raise ValueError("Tipo de archivo no reconocido")

        # Paso 3: Validación de registros
        paso_actual = update_step(paso_actual, TOTAL_PASOS, "Validando registros", update_progress)
        registros_validos = []
        registros_error = []
        
        for idx, fila in df_procesado.iterrows():
            try:
                _validar_registro(fila)
                registros_validos.append(fila.to_dict())
            except Exception as e:
                registros_error.append({
                    'archivo': nombre_archivo,
                    'error': str(e),
                    'registro': fila.to_dict()
                })

        # Paso 4: Generación de metadatos
        paso_actual = update_step(paso_actual, TOTAL_PASOS, "Agregando metadatos", update_progress)
        df_valido = pd.DataFrame(registros_validos)
        if not df_valido.empty:
            df_valido = _agregar_metadatos(df_valido, nombre_archivo)

        # Paso 5: Generación de ID único
        paso_actual = update_step(paso_actual, TOTAL_PASOS, "Generando IDs únicos", update_progress)
        if not df_valido.empty:
            df_valido = _generar_id_unico(df_valido)

        # Paso 6: Finalización
        update_step(paso_actual, TOTAL_PASOS, "Proceso completado", update_progress)
        return df_valido, pd.DataFrame(registros_error), "OK"

    except Exception as e:
        error = pd.DataFrame([{
            'archivo': nombre_archivo,
            'error': f"Error general: {str(e)}",
            'registro': None
        }])
        return pd.DataFrame(), error, str(e)

def procesar_carpeta_pagos(
    carpeta: str,
    update_progress: Optional[Callable[[float, str], None]] = None
) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
    """
    Procesa todos los archivos en una carpeta con seguimiento de progreso
    """
    
    paso_actual = 1
    archivos = [f for f in os.listdir(carpeta) if f.lower().endswith('.txt')]
    total_archivos = len(archivos)
    data_valida = []
    data_errores = []
    archivos_con_error = []

    # Paso 1: Preparación
    paso_actual = update_step(paso_actual, TOTAL_PASOS, "Iniciando procesamiento de carpeta", update_progress)

    for i, archivo in enumerate(archivos):
        # Actualizar progreso general
        if update_progress:
            progress = (i + 1) / total_archivos
            update_progress(progress, f"Procesando archivo {i+1}/{total_archivos}: {archivo}")

        ruta_completa = os.path.join(carpeta, archivo)
        df_valido, df_error, mensaje = procesar_archivo_pago(ruta_completa, archivo, update_progress)
        
        if not df_valido.empty:
            data_valida.append(df_valido)
        if not df_error.empty:
            data_errores.append(df_error)
        if mensaje != "OK":
            archivos_con_error.append(archivo)

    # Paso final: Consolidación
    paso_actual = update_step(TOTAL_PASOS, TOTAL_PASOS, "Finalizando proceso", update_progress)
    df_consolidado = pd.concat(data_valida, ignore_index=True) if data_valida else pd.DataFrame()
    df_errores = pd.concat(data_errores, ignore_index=True) if data_errores else pd.DataFrame()
    
    return df_consolidado, df_errores, archivos_con_error

# ================================================
# FUNCIONES DE APOYO
# ================================================
def _procesar_acuerdos(df: pd.DataFrame) -> pd.DataFrame:
    """Lógica específica para archivos de acuerdos de pago"""
    columnas = [
        'nro_acuerdo', 'id_usuario', 'nombres', 'apellidos', 
        'valor', 'fecha_liquida', 'consecutivo_cuota'
    ]
    df = df[[c for c in columnas if c in df.columns]]
    
    # Renombrar y limpiar
    df = df.rename(columns={
        'fecha_liquida': 'fecha_pago',
        'id_usuario': 'documento'
    })
    
    # Generar campos
    df['nombre_usuario'] = df['nombres'].str.cat(df['apellidos'], sep=' ', na_rep='').str.strip()
    df['tipo_registro'] = 'ACUERDO'
    df['nro_comparendo'] = np.nan
    
    return df.drop(columns=['nombres', 'apellidos'], errors='ignore')

def _procesar_comparendos(df: pd.DataFrame) -> pd.DataFrame:
    """Lógica específica para archivos de comparendos"""
    columnas = [
        'nro_comparendo', 'nro_recibo', 'fecha_liquida_contrav',
        'compute_0004', 'id_usuario', 'nombres', 'apellidos',
        'nro_resolucion', 'intereses'
    ]
    df = df[[c for c in columnas if c in df.columns]]
    
    # Renombrar y limpiar
    df = df.rename(columns={
        'fecha_liquida_contrav': 'fecha_pago',
        'id_usuario': 'documento'
    })
    
    # Lógica de campos
    df['nro_comparendo'] = np.where(
        df['nro_comparendo'].isnull() | (df['nro_comparendo'] == ''),
        df['nro_resolucion'], df['nro_comparendo']
    )
    
    # Cálculo de valores
    df['compute_0004'] = pd.to_numeric(df['compute_0004'], errors='coerce')
    df['intereses'] = pd.to_numeric(df['intereses'], errors='coerce')
    df['valor'] = (df['compute_0004'].fillna(0) + df['intereses'].fillna(0))
    df['tipo_registro'] = 'COMPARENDO'
    df['nro_acuerdo'] = np.nan
    
    return df.drop(columns=['nombres', 'apellidos'], errors='ignore')

def _validar_registro(fila: pd.Series):
    """Valida un registro individual"""
    # Campos obligatorios
    if any(pd.isna(fila[col]) for col in CAMPOS_OBLIGATORIOS):
        raise ValueError("Campos obligatorios faltantes")
    
    # Formato documento
    if not str(fila['documento']).strip().match(REGEX_DOCUMENTO):
        raise ValueError(f"Documento inválido: {fila['documento']}")
    
    # Valor positivo
    if float(fila['valor']) <= 0:
        raise ValueError("El valor debe ser positivo")

def _agregar_metadatos(df: pd.DataFrame, nombre_archivo: str) -> pd.DataFrame:
    """Agrega metadatos de procesamiento"""
    df['archivo_origen'] = nombre_archivo[:60]
    df['fecha_carga'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['version_proceso'] = VERSION_PROCESO
    return df

def _generar_id_unico(df: pd.DataFrame) -> pd.DataFrame:
    """Genera ID único SHA-256 para cada registro"""
    grupo = [
        'documento', 
        'nombre_usuario', 
        'valor', 
        'fecha_pago',
        'archivo_origen', 
        'nro_acuerdo', 
        'nro_comparendo',
        'consecutivo_cuota'
    ]

    # Crear contador por grupo para evitar duplicados exactos
    df['contador_grupo'] = df.groupby(grupo).cumcount() + 1

    # Generar ID único con SHA-256
    df['id_registro'] = df.apply(
        lambda x: hashlib.sha256((
            f"{x.get('documento', '')}_"
            f"{x.get('nombre_usuario', '')}_"
            f"{x.get('valor', '')}_"
            f"{x['fecha_pago'].timestamp() if pd.notnull(x.get('fecha_pago')) else ''}_"
            f"{x.get('archivo_origen', '')}_"
            f"{x.get('nro_acuerdo', '')}_"
            f"{x.get('nro_comparendo', '')}_"
            f"{x.get('consecutivo_cuota', '')}_"
            f"{x['contador_grupo']}"
        ).encode()).hexdigest(),
        axis=1
    )

    df.drop('contador_grupo', axis=1, inplace=True)
    
    return df