# -*- coding: utf-8 -*-
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
