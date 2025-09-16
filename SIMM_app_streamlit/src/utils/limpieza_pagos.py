# -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime
from typing import Tuple
import os
from datetime import date

def procesar_pagos(ruta_archivo: str, nombre_archivo: str, update_progress=None) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
       
    """
    Procesa archivos de pagos y los prepara para carga en la tabla pagos.
    """
    def parse_fecha(fecha_str):
        """Intenta parsear la fecha en múltiples formatos comunes."""
        if pd.isna(fecha_str) or fecha_str == '':
            return None
        
        formatos = [
            '%d/%m/%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y', 
            '%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y %H:%M:%S', '%Y/%m/%d %H:%M:%S'
        ]
        
        for fmt in formatos:
            try:
                return pd.to_datetime(fecha_str, format=fmt, errors='raise')
            except:
                continue
        
        return None
    
    try:
        # =============================================================================
        # 1. Configuración inicial
        # =============================================================================
        total_steps = 8
        current_step = 0
        warnings = []
        
        def update_step(message: str):
            nonlocal current_step
            current_step += 1
            progress = current_step / total_steps
            progress = min(progress, 1.0)

        update_step("Iniciando procesamiento de pagos")
        
        # =============================================================================
        # 2. Lectura del archivo XLSX
        # =============================================================================
        update_step("Leyendo archivo XLSX")
        try:
            df = pd.read_excel(ruta_archivo)
            if df.empty:
                raise ValueError("El archivo se leyó pero está vacío")
        except Exception as e:
            raise ValueError(f"Error leyendo archivo: {str(e)}")


        # =============================================================================
        # 3. Columnas comunes y metadatos
        # =============================================================================
        update_step("Agregando metadatos comunes")
        
        df['archivo_origen'] = nombre_archivo
        df['fecha_carga'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # =============================================================================
        # 4. Generación de id_registro autoincremental y metadatos
        # =============================================================================
        update_step("Generando id_registro autoincremental y metadatos")
        
        # Resetear índice y crear id_registro autoincremental (empezando en 1)
        df = df.reset_index(drop=True).reset_index().rename(columns={"index": "id_registro"})
        df["id_registro"] = df["id_registro"] + 1
        # Metadatos de carga
        nombre_archivo = os.path.basename(ruta_archivo)
        fecha_carga = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df["archivo_origen"] = nombre_archivo
        df["fecha_carga"] = fecha_carga

        # =============================================================================
        # 5. Validación de campos obligatorios y tipos
        # =============================================================================
        update_step("Validando campos obligatorios y tipos")


        # =============================================================================
        # 6. Separación de registros válidos/erróneos
        # =============================================================================
        update_step("Clasificando registros válidos/erróneos")
        # Identificar registros con errores (campos obligatorios vacíos o inválidos)
        mask_errores = (
            df['codcliente'].isna() | (df['codcliente'] == '') |
            df['nitcliente'].isna() | (df['nitcliente'] == '') |
            df['numobligacion'].isna() | (df['numobligacion'] == '') |
            df['valorpago'].isna() | (df['valorpago'] <= 0) |
            df['fechapago'].isna()
        )
        df_errores = df[mask_errores].copy()
        df_procesado = df[~mask_errores].copy()

        # =============================================================================
        # 7. Selección y orden de columnas finales
        # =============================================================================
        update_step("Preparando estructura final")
        mapeo_columnas_bd = {
            'BASE': 'base',
            'FECHA GEST': 'fecha_gest',
            'APLICA PAGO GESTIÓN': 'aplica_pago_gestion',
            'FECHA SMS': 'fecha_sms',
            'CAMPAÑA': 'campania',
            'APLICA PAGO SMS': 'aplica_pago_sms',
            'APLICACIÓN FINAL': 'aplicacion_final',
            'FECHA SENCILLA': 'fecha_sencilla',
            'AÑO': 'anio',
            'DIA': 'dia',
            'MES': 'mes',
            'SEMANA': 'semana',
            'NÚMERO MES': 'numero_mes',
            'NOMBRE DÍA SEMANA': 'nombre_dia_semana',
            'CRUCE SMS': 'cruce_sms',
            'ESTRATEGIA': 'estrategia',
            'INFRACCION': 'infraccion',
            'Período 21 al 20': 'periodo_21_20'
        }
        df_procesado = df_procesado.rename(columns=mapeo_columnas_bd)
        columnas_finales = [
            'id_registro', 'codcliente', 'nitcliente', 'numobligacion', 'fechapago', 'valorpago',
            'base', 'fecha_gest', 'aplica_pago_gestion', 'fecha_sms', 'campania',
            'aplica_pago_sms', 'aplicacion_final', 'fecha_sencilla', 'anio', 'dia',
            'mes', 'semana', 'numero_mes', 'nombre_dia_semana', 'cruce_sms',
            'estrategia', 'infraccion', 'periodo_21_20', 'archivo_origen', 'fecha_carga'
        ]
        columnas_existentes = [col for col in columnas_finales if col in df_procesado.columns]
        df_procesado = df_procesado[columnas_existentes]
        if not df_errores.empty:
            columnas_existentes_err = [col for col in columnas_finales if col in df_errores.columns]
            df_errores = df_errores[columnas_existentes_err]

        # =============================================================================
        # 8. Validación final
        # =============================================================================
        update_step("Realizando validación final")

         # Convertir columnas de fecha a tipo date (sin hora)
        for col in ['fechapago', 'fecha_gest', 'fecha_sms', 'fecha_sencilla']:
            if col in df_procesado.columns:
                df_procesado[col] = pd.to_datetime(df_procesado[col], errors='coerce').dt.date
            if not df_errores.empty and col in df_errores.columns:
                df_errores[col] = pd.to_datetime(df_errores[col], errors='coerce').dt.date

        # fecha_carga como datetime completo
        if 'fecha_carga' in df_procesado.columns:
            df_procesado['fecha_carga'] = pd.to_datetime(df_procesado['fecha_carga'], errors='coerce')
        if not df_errores.empty and 'fecha_carga' in df_errores.columns:
            df_errores['fecha_carga'] = pd.to_datetime(df_errores['fecha_carga'], errors='coerce')
        
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
