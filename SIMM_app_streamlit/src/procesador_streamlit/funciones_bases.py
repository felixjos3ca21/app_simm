# src/utils/procesamiento/funciones_base.py

import pandas as pd
import numpy as np
from datetime import datetime
import hashlib
from pathlib import Path
from src.procesador_streamlit.archivos import detectar_encoding

# --- FUNCIONES AUXILIARES ---

def parse_fecha(fecha_str):
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

def limpiar_nombres(df, campos=['nombres', 'apellidos']):
    for col in campos:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace(r'[\t\n\r"]+', ' ', regex=True)
                .str.replace(r'\s+', ' ', regex=True)
                .str.strip()
            )
    return df

# --- PROCESAMIENTO DE ARCHIVOS ---

def procesar_archivo_ap(archivo: Path) -> pd.DataFrame:
    try:
        encoding = detectar_encoding(archivo)
        df = pd.read_csv(archivo, sep='\t', encoding=encoding, dtype=str)

        columnas = ['nro_acuerdo', 'id_usuario', 'nombres', 'apellidos', 'valor', 'fecha_liquida', 'consecutivo_cuota']
        df = df[[col for col in columnas if col in df.columns]]

        df['fecha_liquida'] = df['fecha_liquida'].apply(parse_fecha)

        df = limpiar_nombres(df)
        if 'nombres' in df.columns and 'apellidos' in df.columns:
            df['nombre_usuario'] = df['nombres'].str.cat(df['apellidos'], sep=' ', na_rep='').str.strip()
        else:
            df['nombre_usuario'] = ''
        df.drop(columns=[c for c in ['nombres', 'apellidos'] if c in df.columns], inplace=True)

        df['nro_acuerdo'] = df['nro_acuerdo'].astype(str)
        df['documento'] = df['id_usuario'].astype(str) if 'id_usuario' in df.columns else ''
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce')

        df['archivo_origen'] = archivo.name
        df['fecha_carga'] = datetime.now()
        df['tipo_pago'] = 'AP'

        df['id_registro'] = df.apply(
            lambda x: hashlib.sha256(
                f"AP_{x.get('nro_acuerdo','')}_{x.get('documento','')}_{x.get('consecutivo_cuota','')}_{x.get('fecha_liquida','')}".encode()
            ).hexdigest(), axis=1
        )

        return df
    except Exception as e:
        print(f"Error procesando {archivo.name}: {e}")
        return pd.DataFrame()

def procesar_archivo_comparendos(archivo: Path) -> pd.DataFrame:
    try:
        encoding = detectar_encoding(archivo)
        df = pd.read_csv(archivo, sep='\t', encoding=encoding, dtype=str)

        columnas = ['nro_comparendo', 'nro_recibo', 'fecha_liquida_contrav',
                    'compute_0004', 'id_usuario', 'nombres', 'apellidos',
                    'nro_resolucion', 'intereses']
        df = df[[col for col in columnas if col in df.columns]]

        if 'fecha_liquida_contrav' in df.columns:
            df.rename(columns={'fecha_liquida_contrav': 'fecha_liquida'}, inplace=True)

        df['fecha_liquida'] = df['fecha_liquida'].apply(parse_fecha)

        if 'nro_comparendo' in df.columns and 'nro_resolucion' in df.columns:
            df['nro_comparendo'] = np.where(
                df['nro_comparendo'].isna() | (df['nro_comparendo'] == ''),
                df['nro_resolucion'], df['nro_comparendo']
            )

        df = limpiar_nombres(df)
        if 'nombres' in df.columns and 'apellidos' in df.columns:
            df['nombre_usuario'] = df['nombres'].str.cat(df['apellidos'], sep=' ', na_rep='').str.strip()
        else:
            df['nombre_usuario'] = ''
        df.drop(columns=[c for c in ['nombres', 'apellidos'] if c in df.columns], inplace=True)

        df['compute_0004'] = pd.to_numeric(df.get('compute_0004', 0), errors='coerce').fillna(0)
        df['intereses'] = pd.to_numeric(df.get('intereses', 0), errors='coerce').fillna(0)
        df['valor'] = df['compute_0004'] + df['intereses']

        df['documento'] = df['id_usuario'].astype(str) if 'id_usuario' in df.columns else ''
        df['archivo_origen'] = archivo.name
        df['fecha_carga'] = datetime.now()
        df['tipo_pago'] = 'COMP'

        df['id_registro'] = df.apply(
            lambda x: hashlib.sha256(
                f"COMP_{x.get('nro_comparendo','')}_{x.get('documento','')}_{x.get('valor','')}_{x.get('fecha_liquida','')}".encode()
            ).hexdigest(), axis=1
        )

        return df
    except Exception as e:
        print(f"Error procesando {archivo.name}: {e}")
        return pd.DataFrame()
