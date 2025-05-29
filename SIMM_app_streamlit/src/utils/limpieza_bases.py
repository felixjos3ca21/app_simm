import pandas as pd
from datetime import datetime
import hashlib
import numpy as np
from typing import Tuple
import re

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
        total_steps = 8
        current_step = 0
        
        def update_step(message: str):
            nonlocal current_step
            current_step += 1
            if update_progress:
                progress = current_step / total_steps
                update_progress(progress, f"Paso {current_step}/{total_steps}: {message}")

        update_step("Iniciando procesamiento de bases")
        
        # Columnas requeridas (ajustar según tus archivos)
        columnas_requeridas = {
            'BASE',
            'FECHA DE ENTREGA',
            'TIPO DE DCTO',
            'IDENTIFICACIÓN',
            'NOMBRE',
            'NRO. COMPARENDO',
            'FECHA DE COMPARENDO',
            'VALOR INFRACCIÓN',
            'CEL1'
        }

        # =============================================================================
        # 2. Lectura y validación del archivo
        # =============================================================================
        update_step("Leyendo archivo Excel")
        try:
            df = pd.read_excel(
                ruta_archivo,
                dtype={'IDENTIFICACIÓN': str, 'NRO. COMPARENDO': str, 'CEL1': str}
            ).replace({'': pd.NA, ' ': pd.NA, 'NULL': pd.NA, 'nan': pd.NA, 'NaN': pd.NA})
        except Exception as e:
            raise ValueError(f"Error leyendo archivo: {str(e)}")

        # Verificar columnas requeridas
        faltantes = columnas_requeridas - set(df.columns)
        if faltantes:
            raise ValueError(f"Columnas faltantes: {', '.join(faltantes)}")

        # =============================================================================
        # 3. Limpieza básica y normalización
        # =============================================================================
        update_step("Normalizando datos")
        
        # Limpieza de strings
        string_cols = ['BASE', 'TIPO DE DCTO', 'IDENTIFICACIÓN', 'NOMBRE', 'NRO. COMPARENDO']
        df[string_cols] = df[string_cols].apply(lambda x: x.astype(str).str.strip())
        
        # Limpieza de teléfonos
        df['CEL1'] = df['CEL1'].astype(str).str.replace(r'[^\d+]', '', regex=True)
        
        # Convertir valores numéricos
        df['VALOR INFRACCIÓN'] = pd.to_numeric(df['VALOR INFRACCIÓN'], errors='coerce')
        if 'VALOR INTERESES' in df.columns:
            df['VALOR INTERESES'] = pd.to_numeric(df['VALOR INTERESES'], errors='coerce')

        # =============================================================================
        # 4. Procesamiento de fechas
        # =============================================================================
        update_step("Procesando fechas")
        
        # Función para convertir fechas mixtas
        def convertir_fecha(columna):
            # Intenta formato fecha primero
            fecha = pd.to_datetime(df[columna], errors='coerce')
            
            # Para los que fallan, extrae patrones de fecha
            mascara_nulos = fecha.isna()
            if mascara_nulos.any():
                patrones_fecha = [
                    r'(\d{2,4}[-/]\d{2}[-/]\d{2,4})',  # YYYY-MM-DD o DD-MM-YYYY
                    r'(\d{2,4}\d{2}\d{2,4})',          # YYYYMMDD o DDMMYYYY
                ]
                
                for patron in patrones_fecha:
                    extracciones = df.loc[mascara_nulos, columna].astype(str).str.extract(patron)[0]
                    fecha.loc[mascara_nulos] = pd.to_datetime(extracciones, errors='coerce')
                    mascara_nulos = fecha.isna()
            
            return fecha

        df['FECHA DE ENTREGA'] = convertir_fecha('FECHA DE ENTREGA')
        df['FECHA DE COMPARENDO'] = convertir_fecha('FECHA DE COMPARENDO')

        # =============================================================================
        # 5. Generación de identificadores únicos
        # =============================================================================
        update_step("Generando identificadores")
        
        # Hash único basado en campos clave
        campos_hash = ['IDENTIFICACIÓN', 'NRO. COMPARENDO', 'FECHA DE COMPARENDO']
        df['hash_unico'] = df[campos_hash].apply(
            lambda x: hashlib.sha256(
                '|'.join([str(x[c]) if not pd.isna(x[c]) else 'NULL' for c in campos_hash]).encode()
            ).hexdigest(),
            axis=1
        )
        
        # ID registro único
        df['id_registro'] = df.apply(
            lambda x: hashlib.sha256((
                f"{x['hash_unico']}_"
                f"{x['FECHA DE ENTREGA'].timestamp() if pd.notnull(x['FECHA DE ENTREGA']) else 'NULL'}_"
                f"{nombre_archivo}"
            ).encode()).hexdigest(),
            axis=1
        )

        # =============================================================================
        # 6. Mapeo a estructura de tabla 'bases'
        # =============================================================================
        update_step("Mapeando a estructura final")
        
        mapeo_columnas = {
            'id_registro': 'id_registro',
            'hash_unico': 'hash_unico',
            'BASE': 'base',
            'FECHA DE ENTREGA': 'fecha_entrega',
            'TIPO DE DCTO': 'tipo_documento',
            'IDENTIFICACIÓN': 'documento',
            'NOMBRE': 'nombre',
            'NRO. COMPARENDO': 'numero_comparendo',
            'FECHA DE COMPARENDO': 'fecha_comparendo',
            'CODIGO DE INFRACCIÓN': 'codigo_infraccion',
            'PLACA': 'placa',
            'VALOR INFRACCIÓN': 'valor_infraccion',
            'CEL1': 'telefono',
            'VALOR INTERESES': 'valor_intereses'
        }
        
        df_final = pd.DataFrame()
        for col_db, col_archivo in mapeo_columnas.items():
            if col_archivo in df.columns:
                df_final[col_db] = df[col_archivo]
        
        # Campos adicionales
        df_final['archivo_origen'] = nombre_archivo[:100]
        df_final['fecha_carga'] = datetime.now()

        # =============================================================================
        # 7. Validación y separación de errores
        # =============================================================================
        update_step("Validando datos")
        
        # Campos obligatorios
        campos_obligatorios = ['documento', 'fecha_entrega']
        df_final[campos_obligatorios] = df_final[campos_obligatorios].replace({
            'nan': pd.NA, 'None': pd.NA, '': pd.NA
        })
        
        # Identificar errores
        mask_errores = df_final[campos_obligatorios].isna().any(axis=1)
        df_errores = df_final[mask_errores].copy()
        df_procesado = df_final[~mask_errores].copy()
        
        # Asignar códigos de error
        if not df_errores.empty:
            df_errores['error'] = df_errores.apply(
                lambda x: 'DOCUMENTO_VACIO' if pd.isna(x['documento']) else 'FECHA_ENTREGA_VACIA',
                axis=1
            )
        
        # =============================================================================
        # 8. Resultados finales
        # =============================================================================
        update_step("Finalizando procesamiento")
        
        mensaje = (
            f"Procesado completo. "
            f"Registros válidos: {len(df_procesado)}, "
            f"Errores: {len(df_errores)}"
        )
        
        return df_procesado, df_errores, mensaje

    except Exception as e:
        error_msg = f"Error en paso {current_step}: {str(e)}"
        if 'df' in locals():
            return pd.DataFrame(), pd.DataFrame({'error': [error_msg]}), error_msg
        return pd.DataFrame(), pd.DataFrame(), error_msg