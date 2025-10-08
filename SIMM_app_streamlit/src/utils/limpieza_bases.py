import pandas as pd
from datetime import datetime
import hashlib
import numpy as np
from typing import Tuple
import re
import logging

logger = logging.getLogger(__name__)

def preparar_datos_bases(ruta_archivo: str, nombre_archivo: str, update_progress=None) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    """
    Procesa archivos de bases/infracciones para carga en la tabla 'bases'
    
    Args:
        ruta_archivo (str): Ruta del archivo Excel a procesar
        nombre_archivo (str): Nombre original del archivo
        update_progress (function, optional): Callback para reportar progreso
        
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, str]: 
            - Datos procesados
            - Registros con errores
            - Mensaje de resumen
    """
    try:
        # =============================================================================
        # 1. Configuración inicial
        # =============================================================================
        total_steps = 9  # Aumentado a 9 pasos
        current_step = 0
        warnings = []
        errores_hojas = []
        
        def update_step(message: str):
            nonlocal current_step
            current_step += 1
            if update_progress:
                progress = current_step / total_steps
                update_progress(progress, f"Paso {current_step}/{total_steps}: {message}")

        update_step("Iniciando procesamiento de bases")
        
        # Columnas requeridas (versión mejorada)
        columnas_requeridas = {
            'BASE': 'object',
            'FECHA DE ENTREGA': 'datetime64[ns]',
            'TIPO DE DCTO': 'object',
            'IDENTIFICACIÓN': 'object',
            'NOMBRE': 'object',  
            'NRO. COMPARENDO': 'object',
            'FECHA DE COMPARENDO': 'datetime64[ns]',  # Nueva columna que mencionas
            'CODIGO DE INFRACCIÓN': 'object',  # Nueva columna que mencionas
            'PLACA': 'object',  # Nueva columna que mencionas
            'VALOR INFRACCIÓN': 'float64', 
            'VALOR INTERESES': 'float64',  # Asegúrate que coincide exactamente con el nombre en el archivo
            'CEL1': 'object'
        }

        # =============================================================================
        # 2. Lectura inteligente del archivo
        # =============================================================================
        update_step("Leyendo archivo Excel")
        try:
            xls = pd.ExcelFile(ruta_archivo)
            if "REGISTROS" not in xls.sheet_names:
                raise ValueError("La hoja 'REGISTROS' no existe en el archivo Excel.")
            
            df_all_columns = pd.read_excel(xls, sheet_name="REGISTROS", nrows=0)
            
            available_columns = df_all_columns.columns.tolist()
            
            # Mapeo de columnas 
            column_mapping = {
                'BASE': 'BASE',
                'FECHA DE ENTREGA': 'FECHA DE ENTREGA',
                'TIPO DE DCTO': 'TIPO DE DCTO',
                'IDENTIFICACIÓN': 'IDENTIFICACIÓN',
                'NOMBRE': 'NOMBRE',  
                'NRO. COMPARENDO': 'NRO. COMPARENDO',
                'FECHA DE COMPARENDO': 'FECHA DE COMPARENDO',
                'CODIGO DE INFRACCIÓN': 'CODIGO DE INFRACCIÓN',
                'PLACA': 'PLACA',
                'VALOR INFRACCIÓN': 'VALOR INFRACCIÓN',
                'VALOR INTERESES': 'VALOR INTERESES',  
                'CEL1': 'CEL1'
            }
            
            # Verificar qué columnas están realmente disponibles
            columns_to_read = []
            for db_col, file_col in column_mapping.items():
                if file_col in available_columns:
                    columns_to_read.append(file_col)
                else:
                    logger.warning(f"Columna '{file_col}' no encontrada en el archivo, se usará NULL para '{db_col}'")
                    warnings.append(f"Columna no encontrada en archivo: {file_col} (se usará NULL para {db_col})")
            
            # Leer solo las columnas disponibles
            df = pd.read_excel(
                ruta_archivo,
                sheet_name="REGISTROS",
                usecols=columns_to_read,
                dtype=columnas_requeridas,
                parse_dates=['FECHA DE ENTREGA', 'FECHA DE COMPARENDO'],
                na_values=['', ' ', 'NULL', 'nan', 'NaN', 'NaT']
            )
            
            # Renombrar columnas al nombre estándar que espera el resto del código
            df = df.rename(columns={v: k for k, v in column_mapping.items() if v in df.columns})
            
            # Asegurar que todas las columnas esperadas existan (crear las faltantes como NULL)
            for col in columnas_requeridas:
                if col not in df.columns:
                    df[col] = pd.NA
        except Exception as e:
            logger.error(f"Error leyendo hoja 'REGISTROS': {str(e)}")
            raise ValueError(f"Error leyendo hoja 'REGISTROS': {str(e)}")

        # =============================================================================
        # 3. Limpieza avanzada de datos
        # =============================================================================
        update_step("Normalizando datos")
        
        # Limpieza de strings con manejo de NaN
        def clean_string(x):
            if pd.isna(x):
                return x
            return str(x).strip()
            
        string_cols = ['BASE', 'TIPO DE DCTO', 'IDENTIFICACIÓN', 'NOMBRE', 'NRO. COMPARENDO']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].apply(clean_string)

        # Limpieza avanzada de teléfonos (conserva formato internacional)
        if 'CEL1' in df.columns:
            df['CEL1'] = df['CEL1'].astype(str).str.replace(r'[^\d\+]', '', regex=True)
            df['CEL1'] = df['CEL1'].replace({'nan': pd.NA, 'None': pd.NA})

        # =============================================================================
        # 4. Procesamiento de valores numéricos (versión mejorada)
        # =============================================================================
        update_step("Procesando valores monetarios")
        
        try:
            if 'VALOR INFRACCIÓN' not in df.columns:
                df['VALOR INFRACCIÓN'] = 0.0
            else:
                df['VALOR INFRACCIÓN'] = pd.to_numeric(df['VALOR INFRACCIÓN'], errors='coerce')

            if 'VALOR INTERESES' not in df.columns:
                df['VALOR INTERESES'] = 0.0
            else:
                df['VALOR INTERESES'] = pd.to_numeric(df['VALOR INTERESES'], errors='coerce')

            # Calcular suma fila por fila
            df['valor'] = df[['VALOR INFRACCIÓN', 'VALOR INTERESES']].sum(axis=1, skipna=True)
            df['valor'] = df['valor'].round(2)

            # Eliminar columnas originales
            df.drop(columns=['VALOR INFRACCIÓN', 'VALOR INTERESES'], errors='ignore', inplace=True)
            
        except Exception as e:
            logger.error(f"Error en cálculos de valor: {str(e)}")
            warnings.append(f"Error en cálculos de valor: {str(e)}")
            df['valor'] = np.nan

        # =============================================================================
        # 5. Procesamiento robusto de fechas
        # =============================================================================
        update_step("Procesando fechas")
        
        def parse_fecha(columna):
            return pd.to_datetime(df[columna], errors='coerce', dayfirst=True).dt.date

        # Aplicar a columnas de fecha como tipo `date`
        if 'FECHA DE ENTREGA' in df.columns:
            df['FECHA DE ENTREGA'] = parse_fecha('FECHA DE ENTREGA')
            
        if 'FECHA DE COMPARENDO' in df.columns:
            df['FECHA DE COMPARENDO'] = parse_fecha('FECHA DE COMPARENDO')

        # =============================================================================
        # 6. Generación de identificadores únicos mejorados
        # =============================================================================
        update_step("Generando identificadores")
        
        # Campos base para hash (ajustados según tu feedback)
        campos_hash = ['BASE', 'IDENTIFICACIÓN', 'NRO. COMPARENDO']
        
        # Asegurar que los campos existan y manejar NaN
        df_hash = df[campos_hash].fillna('NULL').astype(str)
        
        # Generar hash único más estable
        df['hash_unico'] = df_hash.apply(
            lambda x: hashlib.sha256(
                '|'.join(x.values).encode()
            ).hexdigest(),
            axis=1
        )
        
        # ID registro con más información de contexto
        df['id_registro'] = df.apply(
            lambda x: hashlib.sha256((
                f"{x['hash_unico']}_"
                f"{str(x['FECHA DE ENTREGA']) if pd.notnull(x.get('FECHA DE ENTREGA')) else 'NULL'}_"
                f"{nombre_archivo.lower()}"
            ).encode()).hexdigest(),
            axis=1
        )

        # =============================================================================
        # 7. Mapeo a estructura final con validación
        # =============================================================================
        update_step("Mapeando a estructura final")
        
        mapeo_columnas = {
            'id_registro': 'id_registro',
            'BASE': 'base',
            'FECHA DE ENTREGA': 'fecha_entrega',
            'TIPO DE DCTO': 'tipo_documento',
            'IDENTIFICACIÓN': 'documento',
            'NOMBRE': 'nombre',
            'NRO. COMPARENDO': 'numero_comparendo',
            'FECHA DE COMPARENDO': 'fecha_comparendo',
            'CODIGO DE INFRACCIÓN': 'codigo_infraccion',
            'PLACA': 'placa',
            'valor': 'valor_infraccion',
            'CEL1': 'telefono',
        }
        
        # Crear DataFrame final solo con columnas existentes
        df_final = pd.DataFrame()
        for col_db, col_archivo in mapeo_columnas.items():
            if col_db in df.columns:
                df_final[col_db] = df[col_db]
        
        # Campos adicionales con validación
        df_final['archivo_origen'] = nombre_archivo[:100]
        df_final['fecha_carga'] = datetime.now()
        df_final['base_cleaned'] = ''

        # =============================================================================
        # 8. Validación avanzada de datos
        # =============================================================================
        update_step("Validando datos")
        
        # Reglas de validación configurables
        reglas_validacion = {
            'base': lambda x: not pd.isna(x) and str(x).strip() != '',
            'fecha_entrega': lambda x: pd.notna(x),
            'documento': lambda x: pd.notna(x) and re.match(r'^[a-zA-Z0-9]{5,}$', str(x)),
            'numero_comparendo': lambda x: pd.isna(x) or re.match(r'^[A-Za-z0-9\-]+$', str(x))
        }
        
        # Aplicar validaciones
        df_final['errores'] = ''
        for campo, validacion in reglas_validacion.items():
            if campo in df_final.columns:
                mask = ~df_final[campo].apply(validacion)
                df_final.loc[mask, 'errores'] += f"{campo.upper()}_INVALIDO;"
        
        # Separar registros válidos de errores
        mask_errores = df_final['errores'] != ''
        df_errores = df_final[mask_errores].copy()
        df_procesado = df_final[~mask_errores].copy()
        
        # Limpiar columna temporal de errores
        df_errores['error'] = df_errores['errores'].str.strip(';')
        df_errores.drop(columns=['errores'], inplace=True)
        df_procesado.drop(columns=['errores'], inplace=True)
        df_procesado['estado_telefono'] = ""

        # =============================================================================
        # 9. Resultados finales y reportes
        # =============================================================================
        update_step("Generando reportes")
        
        # Estadísticas de calidad
        stats = {
            'total_registros': len(df),
            'registros_validos': len(df_procesado),
            'registros_con_errores': len(df_errores),
            'errores_por_tipo': df_errores['error'].value_counts().to_dict(),
            'warnings': warnings
        }
        
        mensaje = (
            f"Procesado completo:\n"
            f"- Registros totales: {stats['total_registros']}\n"
            f"- Registros válidos: {stats['registros_validos']}\n"
            f"- Registros con errores: {stats['registros_con_errores']}\n"
            f"- Tipos de errores: {', '.join(stats['errores_por_tipo'].keys()) if stats['errores_por_tipo'] else 'Ninguno'}\n"
            f"- Advertencias: {', '.join(warnings) if warnings else 'Ninguna'}"
        )
        
        return df_procesado, df_errores, mensaje

    except Exception as e:
        logger.error(f"Error en paso {current_step}: {str(e)}")
        error_msg = f"Error en paso {current_step}: {str(e)}"
        if 'df' in locals():
            return pd.DataFrame(), pd.DataFrame({'error': [error_msg]}), error_msg
        return pd.DataFrame(), pd.DataFrame(), error_msg