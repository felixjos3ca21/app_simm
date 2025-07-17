import streamlit as st
import pandas as pd
import os
from pathlib import Path
import chardet
import hashlib
from datetime import datetime
import numpy as np
import re
from tkinter import Tk, filedialog
from sqlalchemy import text
from sqlalchemy.types import String, Numeric, DateTime
from sqlalchemy.exc import SQLAlchemyError
from src.database.postgres import DatabaseManager
from src.utils.fondo import set_background

# ==============================================================================
# CONFIGURACIÓN INICIAL
# ==============================================================================
st.set_page_config(
    page_title="SIAMM - Actualización de Pagos",
    page_icon="src/utils/favicon-114x114.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuración de estilos
st.markdown("""
    <style>
    [data-testid=stSidebar] { background: #a5d6a7 !important; padding: 20px 10px; }
    .main-container { padding: 2rem; background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%); min-height: 100vh; }
    .metric-card { background: white; padding: 20px; border-radius: 15px; box-shadow: 0 8px 32px rgba(0,0,0,0.1); margin: 10px 0; transition: transform 0.3s ease; }
    .metric-card:hover { transform: translateY(-5px); }
    .section-title { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-size: 2rem; margin: 30px 0 20px 0; text-align: center; }
    .section-header { color: #2e7d32; border-bottom: 2px solid #a5d6a7; padding-bottom: 0.5rem; margin-bottom: 1rem; }
    .stButton > button { background: linear-gradient(120deg, #a5d6a7 0%, #2ecc71 100%); color: white; border: none; border-radius: 25px; padding: 12px 30px; transition: all 0.3s ease; }
    .stButton > button:hover { transform: translateY(-2px); box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6); }
    .stProgress > div > div > div > div { background-color: #2e7d32; }
    .sidebar-title { color: #2c3e50; font-size: 1.2rem; margin-bottom: 1rem; font-weight: 600; }
    .success-box { background: #d4edda; border: 1px solid #c3e6cb; border-radius: 8px; padding: 15px; margin: 10px 0; }
    .error-box { background: #f8d7da; border: 1px solid #f5c6cb; border-radius: 8px; padding: 15px; margin: 10px 0; }
    .info-box { background: #d1ecf1; border: 1px solid #b3d4d8; border-radius: 8px; padding: 15px; margin: 10px 0; }
    .file-status-new { color: #28a745; font-weight: bold; }
    .file-status-modified { color: #ffc107; font-weight: bold; }
    .file-status-existing { color: #6c757d; }
    </style>
""", unsafe_allow_html=True)

# Logo y fondo
st.image("src/utils/logo-andesbpo-359x143.png", width=150)
set_background("src/utils/bg-seccion.png")

# ==============================================================================
# CONFIGURACIÓN DE ESTADO DE LA APLICACIÓN
# ==============================================================================
def inicializar_estado():
    """Inicializa el estado de la aplicación"""
    if 'db_connected' not in st.session_state:
        st.session_state.db_connected = False
    if 'directorio_seleccionado' not in st.session_state:
        st.session_state.directorio_seleccionado = None
    if 'procesamiento_completo' not in st.session_state:
        st.session_state.procesamiento_completo = False
    if 'resultados_procesamiento' not in st.session_state:
        st.session_state.resultados_procesamiento = None
    if 'archivos_procesados' not in st.session_state:
        st.session_state.archivos_procesados = []

# ==============================================================================
# CONEXIÓN A BASE DE DATOS
# ==============================================================================
@st.cache_resource
def conectar_base_datos():
    """Establece conexión con la base de datos"""
    try:
        engine_simm = DatabaseManager.get_engine('SIMM')
        with engine_simm.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine_simm, True
    except Exception as e:
        st.error(f"🔴 Error al conectar con la base de datos SIMM: {e}")
        return None, False

# ==============================================================================
# FUNCIONES DE APOYO
# ==============================================================================
def detectar_encoding(archivo):
    """Detecta la codificación de un archivo"""
    with open(archivo, 'rb') as f:
        return chardet.detect(f.read(100000))['encoding']

def parse_fecha(fecha_str):
    """Parsea fechas en múltiples formatos"""
    if pd.isna(fecha_str) or fecha_str == '':
        return None
    
    formatos = [
        '%d/%m/%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y',
        '%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y %H:%M:%S', '%Y/%m/%d %H:%M:%S'
    ]
    
    for fmt in formatos:
        try:
            return pd.to_datetime(fecha_str, format=fmt)
        except:
            continue
    return None

def limpiar_nombres(df, campos=['nombres', 'apellidos']):
    """Limpia campos de nombres y apellidos"""
    for col in campos:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace(r'[\t\n\r"]+', ' ', regex=True)
                .str.replace(r'\s+', ' ', regex=True)
                .str.strip()
            )
    return df

def generar_id_registro(tipo, **kwargs):
    """Genera un ID único para cada registro"""
    id_string = f"{tipo}_{'_'.join(str(v) for v in kwargs.values())}"
    return hashlib.sha256(id_string.encode()).hexdigest()

# ==============================================================================
# FUNCIONES DE VERIFICACIÓN DE ARCHIVOS Y REGISTROS
# ==============================================================================
def verificar_archivo_procesado(ruta_archivo, engine):
    """Verifica si un archivo ya fue procesado"""
    try:
        ruta_normalizada = str(Path(ruta_archivo).resolve())
        if not os.path.exists(ruta_archivo):
            return None
            
        with engine.connect() as conn:
            # Obtener información del archivo
            with open(ruta_archivo, 'rb') as f:
                hash_actual = hashlib.md5(f.read()).hexdigest()
                fecha_modificacion = datetime.fromtimestamp(os.path.getmtime(ruta_archivo))
                tamaño = os.path.getsize(ruta_archivo)
                
            query = text("""
                SELECT hash_archivo, registros_procesados, fecha_procesamiento 
                FROM archivos_procesados_pagos 
                WHERE ruta_archivo = :ruta
            """)
            resultado = conn.execute(query, {'ruta': ruta_normalizada}).fetchone()
            
            if not resultado:
                return None, None  # Archivo nuevo
            elif resultado[0] == hash_actual:
                return 'igual', resultado  # Archivo idéntico
            else:
                return 'modificado', resultado  # Archivo modificado
    except Exception as e:
        st.error(f"Error verificando archivo: {e}")
        return None, None

def obtener_registros_existentes(ids_registros, tipo_pago, engine):
    """Obtiene un conjunto de IDs de registros ya existentes"""
    if not ids_registros:
        return set()
    
    try:
        with engine.connect() as conn:
            if tipo_pago == 'AP':
                query = text("""
                    SELECT id_registro FROM pagos_ap 
                    WHERE id_registro IN :ids
                """)
            elif tipo_pago == 'COMP':
                query = text("""
                    SELECT id_registro FROM pagos_comparendos 
                    WHERE id_registro IN :ids
                """)
            else:
                return set()
                
            # SQLAlchemy requiere una tupla para el operador IN
            ids_tuple = tuple(ids_registros)
            resultados = conn.execute(query, {'ids': ids_tuple}).fetchall()
            return {r[0] for r in resultados}
    except Exception as e:
        st.error(f"Error obteniendo registros existentes: {e}")
        return set()

def registrar_archivo_procesado(ruta_archivo, tipo_archivo, registros_procesados, registros_nuevos, registros_actualizados, engine):
    """Registra un archivo como procesado con estadísticas detalladas"""
    try:
        with engine.connect() as conn:
            with open(ruta_archivo, 'rb') as f:
                contenido = f.read()
            file_hash = hashlib.md5(contenido).hexdigest()
            nombre_archivo = os.path.basename(ruta_archivo)
            fecha_modificacion = datetime.fromtimestamp(os.path.getmtime(ruta_archivo))
            
            query = text("""
                INSERT INTO archivos_procesados_pagos 
                (nombre_archivo, ruta_archivo, tipo_archivo, registros_procesados, 
                 registros_nuevos, registros_actualizados, estado, hash_archivo, 
                 fecha_modificacion, tamaño_archivo)
                VALUES (:nombre, :ruta, :tipo, :total, :nuevos, :actualizados, 
                        'COMPLETADO', :hash, :fecha_mod, :tamaño)
                ON CONFLICT (ruta_archivo) 
                DO UPDATE SET 
                    fecha_procesamiento = CURRENT_TIMESTAMP,
                    nombre_archivo = EXCLUDED.nombre_archivo,
                    registros_procesados = EXCLUDED.registros_procesados,
                    registros_nuevos = EXCLUDED.registros_nuevos,
                    registros_actualizados = EXCLUDED.registros_actualizados,
                    estado = EXCLUDED.estado,
                    hash_archivo = EXCLUDED.hash_archivo,
                    fecha_modificacion = EXCLUDED.fecha_modificacion,
                    tamaño_archivo = EXCLUDED.tamaño_archivo
            """)
            conn.execute(query, {
                'nombre': nombre_archivo,
                'ruta': str(ruta_archivo),
                'tipo': tipo_archivo,
                'total': registros_procesados,
                'nuevos': registros_nuevos,
                'actualizados': registros_actualizados,
                'hash': file_hash,
                'fecha_mod': fecha_modificacion,
                'tamaño': os.path.getsize(ruta_archivo)
            })
            conn.commit()
            
            # Registrar en session state para mostrar en UI
            st.session_state.archivos_procesados.append({
                'nombre': nombre_archivo,
                'tipo': tipo_archivo,
                'registros': registros_procesados,
                'nuevos': registros_nuevos,
                'actualizados': registros_actualizados,
                'fecha': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
    except Exception as e:
        st.error(f"Error registrando archivo procesado: {e}")

# ==============================================================================
# FUNCIONES DE PROCESAMIENTO
# ==============================================================================
def procesar_archivo_ap(archivo, engine=None):
    """Procesa archivo de acuerdos de pago con verificación de registros"""
    try:
        encoding = detectar_encoding(archivo)
        df = pd.read_csv(archivo, sep='\t', encoding=encoding, dtype=str)
        
        columnas_requeridas = ['nro_acuerdo', 'id_usuario', 'nombres', 'apellidos', 
                             'valor', 'fecha_liquida', 'consecutivo_cuota']
        df = df[[col for col in columnas_requeridas if col in df.columns]]
        
        # Procesamiento específico para AP
        df['fecha_liquida'] = df['fecha_liquida'].apply(parse_fecha)
        df = limpiar_nombres(df)
        df['nombre_usuario'] = df['nombres'].str.cat(df['apellidos'], sep=' ', na_rep='').str.strip()
        df.drop(columns=[c for c in ['nombres', 'apellidos'] if c in df.columns], inplace=True)
        
        df['nro_acuerdo'] = df['nro_acuerdo'].astype(str)
        df['documento'] = df['id_usuario'].astype(str)
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
        
        # Generar ID único
        df['id_registro'] = df.apply(
            lambda x: generar_id_registro('AP', 
                                        nro_acuerdo=x.get('nro_acuerdo',''),
                                        documento=x.get('documento',''),
                                        consecutivo_cuota=x.get('consecutivo_cuota',''),
                                        fecha_liquida=x.get('fecha_liquida','')), 
            axis=1
        )
        
        # Verificar registros existentes si se proporciona engine
        if engine is not None:
            ids_registros = df['id_registro'].unique().tolist()
            existentes = obtener_registros_existentes(ids_registros, 'AP', engine)
            df['registro_existente'] = df['id_registro'].isin(existentes)
        
        df['archivo_origen'] = archivo.name
        df['fecha_carga'] = datetime.now()
        df['tipo_pago'] = 'AP'
        
        return df
        
    except Exception as e:
        st.error(f"Error procesando archivo AP {archivo.name}: {e}")
        return pd.DataFrame()

def procesar_archivo_comp(archivo, engine=None):
    """Procesa archivo de comparendos con verificación de registros"""
    try:
        encoding = detectar_encoding(archivo)
        df = pd.read_csv(archivo, sep='\t', encoding=encoding, dtype=str)
        
        columnas_requeridas = ['nro_comparendo', 'nro_recibo', 'fecha_liquida_contrav',
                             'compute_0004', 'id_usuario', 'nombres', 'apellidos',
                             'nro_resolucion', 'intereses']
        df = df[[col for col in columnas_requeridas if col in df.columns]]
        
        # Procesamiento específico para COMP
        df.rename(columns={'fecha_liquida_contrav': 'fecha_liquida'}, inplace=True)
        df['fecha_liquida'] = df['fecha_liquida'].apply(parse_fecha)
        
        df['nro_comparendo'] = np.where(
            df['nro_comparendo'].isna() | (df['nro_comparendo'] == ''),
            df['nro_resolucion'], df['nro_comparendo']
        )
        
        df = limpiar_nombres(df)
        df['nombre_usuario'] = df['nombres'].str.cat(df['apellidos'], sep=' ', na_rep='').str.strip()
        df.drop(columns=[c for c in ['nombres', 'apellidos'] if c in df.columns], inplace=True)
        
        df['compute_0004'] = pd.to_numeric(df.get('compute_0004', 0), errors='coerce').fillna(0)
        df['intereses'] = pd.to_numeric(df.get('intereses', 0), errors='coerce').fillna(0)
        df['valor'] = df['compute_0004'] + df['intereses']
        df['documento'] = df['id_usuario'].astype(str)
        
        # Generar ID único
        df['id_registro'] = df.apply(
            lambda x: generar_id_registro('COMP',
                                        nro_comparendo=x.get('nro_comparendo',''),
                                        documento=x.get('documento',''),
                                        valor=x.get('valor',''),
                                        fecha_liquida=x.get('fecha_liquida','')),
            axis=1
        )
        
        # Verificar registros existentes si se proporciona engine
        if engine is not None:
            ids_registros = df['id_registro'].unique().tolist()
            existentes = obtener_registros_existentes(ids_registros, 'COMP', engine)
            df['registro_existente'] = df['id_registro'].isin(existentes)
        
        df['archivo_origen'] = archivo.name
        df['fecha_carga'] = datetime.now()
        df['tipo_pago'] = 'COMP'
        
        return df
        
    except Exception as e:
        st.error(f"Error procesando archivo COMP {archivo.name}: {e}")
        return pd.DataFrame()

def filtrar_datos_validos(df):
    """Filtra datos válidos y elimina duplicados"""
    if df.empty:
        return df
        
    df_filtrado = (
        df.drop_duplicates(subset='id_registro')
        .query("valor > 0 and fecha_liquida.notna()", engine='python')
        .reset_index(drop=True)
    )
    
    return df_filtrado

def actualizar_registro_bd(df_registro, tipo_pago, engine):
    """Actualiza un registro existente en la base de datos"""
    try:
        with engine.connect() as conn:
            if tipo_pago == 'AP':
                update_stmt = text("""
                    UPDATE pagos_ap SET
                        nro_acuerdo = :nro_acuerdo,
                        id_usuario = :id_usuario,
                        valor = :valor,
                        fecha_liquida = :fecha_liquida,
                        consecutivo_cuota = :consecutivo_cuota,
                        nombre_usuario = :nombre_usuario,
                        documento = :documento,
                        archivo_origen = :archivo_origen,
                        fecha_carga = :fecha_carga
                    WHERE id_registro = :id_registro
                """)
            elif tipo_pago == 'COMP':
                update_stmt = text("""
                    UPDATE pagos_comparendos SET
                        nro_comparendo = :nro_comparendo,
                        nro_recibo = :nro_recibo,
                        fecha_liquida = :fecha_liquida,
                        compute_0004 = :compute_0004,
                        id_usuario = :id_usuario,
                        nro_resolucion = :nro_resolucion,
                        intereses = :intereses,
                        nombre_usuario = :nombre_usuario,
                        valor = :valor,
                        documento = :documento,
                        archivo_origen = :archivo_origen,
                        fecha_carga = :fecha_carga
                    WHERE id_registro = :id_registro
                """)
            else:
                return False
                
            # Convertir el DataFrame a diccionario para el execute
            params = df_registro.to_dict('records')[0]
            conn.execute(update_stmt, params)
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Error actualizando registro {df_registro['id_registro'].iloc[0]}: {e}")
        return False

def insertar_datos_bd(df, tipo_pago, engine):
    """Inserta o actualiza datos en la base de datos según corresponda"""
    if df.empty:
        return 0, 0  # Registros insertados, actualizados
    
    try:
        # Verificar qué registros ya existen
        ids_registros = df['id_registro'].unique().tolist()
        existentes = obtener_registros_existentes(ids_registros, tipo_pago, engine)
        
        # Separar en nuevos y existentes
        df_nuevos = df[~df['id_registro'].isin(existentes)]
        df_actualizar = df[df['id_registro'].isin(existentes)]
        
        insertados = 0
        actualizados = 0
        
        # Insertar nuevos registros
        if not df_nuevos.empty:
            # Elimina la columna 'registro_existente' si existe
            if 'registro_existente' in df_nuevos.columns:
                df_nuevos = df_nuevos.drop(columns=['registro_existente'])
            with engine.connect() as conn:
                if tipo_pago == 'AP':
                    df_nuevos.to_sql('pagos_ap', conn, if_exists='append', index=False, dtype={
                        'nro_acuerdo': String(50),
                        'id_usuario': String(20),
                        'valor': Numeric(15,2),
                        'fecha_liquida': DateTime(),
                        'consecutivo_cuota': String(50),
                        'nombre_usuario': String(150),
                        'documento': String(30),
                        'archivo_origen': String(100),
                        'tipo_pago': String(10),
                        'id_registro': String(64)
                    })
                elif tipo_pago == 'COMP':
                    df_nuevos.to_sql('pagos_comparendos', conn, if_exists='append', index=False, dtype={
                        'nro_comparendo': String(60),
                        'nro_recibo': String(30),
                        'fecha_liquida': DateTime(),
                        'compute_0004': Numeric(15,2),
                        'id_usuario': String(20),
                        'nro_resolucion': String(50),
                        'intereses': Numeric(15,2),
                        'nombre_usuario': String(150),
                        'valor': Numeric(15,2),
                        'documento': String(30),
                        'archivo_origen': String(100),
                        'tipo_pago': String(10),
                        'id_registro': String(64)
                    })
                conn.commit()
                insertados = len(df_nuevos)
        
        # Actualizar registros existentes
        if not df_actualizar.empty:
            for _, row in df_actualizar.iterrows():
                df_row = pd.DataFrame([row])
                if actualizar_registro_bd(df_row, tipo_pago, engine):
                    actualizados += 1
        
        return insertados, actualizados
        
    except Exception as e:
        st.error(f"Error insertando/actualizando datos de {tipo_pago}: {e}")
        return 0, 0

def procesar_directorio(directorio, engine):
    """Procesa todos los archivos de un directorio con verificación de registros"""
    resultados = {
        'ap_procesados': pd.DataFrame(),
        'ap_filtrados': pd.DataFrame(),
        'comp_procesados': pd.DataFrame(),
        'comp_filtrados': pd.DataFrame(),
        'archivos_nuevos': [],
        'archivos_repetidos': [],
        'archivos_modificados': [],
        'errores': [],
        'estadisticas_archivos': []
    }
    
    # Patrones para identificar tipos de archivo
    patron_ap = re.compile(r'\bap[\s_\-]*pa?g?[a-z]{2,}', re.IGNORECASE)
    patron_comp = re.compile(r'(compa|compr)[a-z]*end?[oa]s?', re.IGNORECASE)
    
    path = Path(directorio)
    archivos_txt = list(path.rglob('*.[tT][xX][tT]'))
    
    if not archivos_txt:
        resultados['errores'].append("No se encontraron archivos .txt en el directorio")
        return resultados
    
    # Clasificar archivos
    archivos_ap = [f for f in archivos_txt if patron_ap.search(f.name)]
    archivos_comp = [f for f in archivos_txt if patron_comp.search(f.name)]
    
    total_archivos = len(archivos_ap) + len(archivos_comp)
    
    if total_archivos == 0:
        resultados['errores'].append("No se encontraron archivos de pagos válidos")
        return resultados
    
    # Barra de progreso
    progreso = st.progress(0)
    status_text = st.empty()
    
    # Procesar archivos AP
    dfs_ap = []
    for i, archivo in enumerate(archivos_ap):
        status_text.text(f"Procesando AP: {archivo.name}")
        progreso.progress((i + 1) / total_archivos)
        
        estado, info_anterior = verificar_archivo_procesado(archivo, engine)
        
        df = procesar_archivo_ap(archivo, engine)
        if not df.empty:
            dfs_ap.append(df)
            total_registros = len(df)
            nuevos = len(df[~df['registro_existente']]) if 'registro_existente' in df.columns else total_registros
            actualizados = len(df[df['registro_existente']]) if 'registro_existente' in df.columns else 0

            # Solo registrar el archivo si no es repetido
            if estado != 'igual':
                registrar_archivo_procesado(
                    archivo, 'AP', total_registros, nuevos, actualizados, engine
                )
            
            resultados['estadisticas_archivos'].append({
                'nombre': archivo.name,
                'tipo': 'AP',
                'estado': estado if estado else 'nuevo',
                'registros': total_registros,
                'nuevos': nuevos,
                'actualizados': actualizados,
                'fecha_procesamiento': datetime.now()
            })
            
            # Clasificación de archivos
            if estado == 'igual':
                resultados['archivos_repetidos'].append(archivo.name)
            elif estado == 'modificado':
                resultados['archivos_modificados'].append(archivo.name)
            else:
                resultados['archivos_nuevos'].append(archivo.name)
    
    dfs_comp = []
    for i, archivo in enumerate(archivos_comp, start=len(archivos_ap)):
        status_text.text(f"Procesando COMP: {archivo.name}")
        progreso.progress((i + 1) / total_archivos)
        
        estado, info_anterior = verificar_archivo_procesado(archivo, engine)
        
        df = procesar_archivo_comp(archivo, engine)
        if not df.empty:
            dfs_comp.append(df)
            total_registros = len(df)
            nuevos = len(df[~df['registro_existente']]) if 'registro_existente' in df.columns else total_registros
            actualizados = len(df[df['registro_existente']]) if 'registro_existente' in df.columns else 0

            # Solo registrar el archivo si no es repetido
            if estado != 'igual':
                registrar_archivo_procesado(
                    archivo, 'COMP', total_registros, nuevos, actualizados, engine
                )
            
            resultados['estadisticas_archivos'].append({
                'nombre': archivo.name,
                'tipo': 'COMP',
                'estado': estado if estado else 'nuevo',
                'registros': total_registros,
                'nuevos': nuevos,
                'actualizados': actualizados,
                'fecha_procesamiento': datetime.now()
            })
            
            # Clasificación de archivos
            if estado == 'igual':
                resultados['archivos_repetidos'].append(archivo.name)
            elif estado == 'modificado':
                resultados['archivos_modificados'].append(archivo.name)
            else:
                resultados['archivos_nuevos'].append(archivo.name)
    # Consolidar resultados
    if dfs_ap:
        resultados['ap_procesados'] = pd.concat(dfs_ap, ignore_index=True)
        resultados['ap_filtrados'] = filtrar_datos_validos(resultados['ap_procesados'])
    
    if dfs_comp:
        resultados['comp_procesados'] = pd.concat(dfs_comp, ignore_index=True)
        resultados['comp_filtrados'] = filtrar_datos_validos(resultados['comp_procesados'])
    
    # Limpiar elementos de UI
    progreso.empty()
    status_text.empty()
    
    return resultados

# ==============================================================================
# INTERFAZ DE USUARIO
# ==============================================================================
def seleccionar_directorio():
    """Abre diálogo para seleccionar directorio"""
    root = Tk()
    root.withdraw()
    root.wm_attributes('-topmost', 1)
    try:
        # Usa la última ruta si existe, si no una predeterminada, si no el cwd
        initial_dir = st.session_state.get('ultima_ruta_usada', r'C:\Users\1513873.ANDESBPO\ANDES BPO S.A.S\Johan Felipe JG. Gómez Arango - SIMM\Gestiones\BASES')  # Cambia aquí tu ruta predeterminada
        dir_path = filedialog.askdirectory(
            title="Seleccione el directorio con los archivos de pagos",
            initialdir=initial_dir
        )
        root.destroy()
        if dir_path:
            st.session_state['ultima_ruta_usada'] = dir_path  # Guarda la última ruta seleccionada
        return dir_path
    except Exception as e:
        st.error(f"Error al seleccionar directorio: {e}")
        return None

def mostrar_resumen_procesamiento(resultados):
    """Muestra resumen del procesamiento incluyendo registros existentes"""
    st.subheader("📊 Resumen General del Procesamiento")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Archivos Nuevos", len(resultados['archivos_nuevos']))
    with col2:
        st.metric("Archivos Modificados", len(resultados['archivos_modificados']))
    with col3:
        st.metric("Archivos Repetidos", len(resultados['archivos_repetidos']))
    
    st.markdown("---")
    
    if 'ap_procesados' in resultados and not resultados['ap_procesados'].empty:
        existentes_ap = resultados['ap_procesados']['registro_existente'].sum() if 'registro_existente' in resultados['ap_procesados'].columns else 0
        nuevos_ap = len(resultados['ap_procesados']) - existentes_ap
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Acuerdos de Pago (AP)")
            st.metric("Registros Procesados", len(resultados['ap_procesados']))
            st.metric("Registros Nuevos", nuevos_ap)
            st.metric("Registros Existentes", existentes_ap)
            st.metric("Registros Válidos", len(resultados['ap_filtrados']))
    
    if 'comp_procesados' in resultados and not resultados['comp_procesados'].empty:
        existentes_comp = resultados['comp_procesados']['registro_existente'].sum() if 'registro_existente' in resultados['comp_procesados'].columns else 0
        nuevos_comp = len(resultados['comp_procesados']) - existentes_comp
        
        with col2:
            st.subheader("📊 Comparendos (COMP)")
            st.metric("Registros Procesados", len(resultados['comp_procesados']))
            st.metric("Registros Nuevos", nuevos_comp)
            st.metric("Registros Existentes", existentes_comp)
            st.metric("Registros Válidos", len(resultados['comp_filtrados']))
    
    st.markdown("---")
    st.subheader("📋 Detalle de Archivos Procesados")
    
    # Mostrar archivos por categoría con más detalles
    if resultados['archivos_nuevos']:
        with st.expander("✅ Archivos Nuevos Procesados", expanded=True):
            for archivo in resultados['estadisticas_archivos']:
                if archivo['estado'] == 'nuevo':
                    st.markdown(f"""
                    - **{archivo['nombre']}** ({archivo['tipo']})
                      - Registros: {archivo['registros']}
                      - Nuevos: <span class="file-status-new">{archivo['nuevos']}</span>
                      - Actualizados: <span class="file-status-modified">{archivo['actualizados']}</span>
                      - Fecha: {archivo['fecha_procesamiento'].strftime('%Y-%m-%d %H:%M:%S')}
                    """, unsafe_allow_html=True)
    
    if resultados['archivos_modificados']:
        with st.expander("🔄 Archivos Modificados", expanded=True):
            for archivo in resultados['estadisticas_archivos']:
                if archivo['estado'] == 'modificado':
                    st.markdown(f"""
                    - **{archivo['nombre']}** ({archivo['tipo']})
                      - Registros: {archivo['registros']}
                      - Nuevos: <span class="file-status-new">{archivo['nuevos']}</span>
                      - Actualizados: <span class="file-status-modified">{archivo['actualizados']}</span>
                      - Fecha: {archivo['fecha_procesamiento'].strftime('%Y-%m-%d %H:%M:%S')}
                    """, unsafe_allow_html=True)
    
    if resultados['archivos_repetidos']:
        with st.expander("📌 Archivos Ya Procesados (Omitidos)", expanded=True):
            for archivo in resultados['estadisticas_archivos']:
                if archivo['estado'] == 'repetido':
                    st.markdown(f"""
                    - **{archivo['nombre']}** ({archivo['tipo']})
                      - Registros: <span class="file-status-existing">{archivo['registros']}</span>
                      - Último procesamiento: {archivo['fecha_procesamiento'].strftime('%Y-%m-%d %H:%M:%S') if archivo['fecha_procesamiento'] else 'N/A'}
                    """, unsafe_allow_html=True)
    
    if resultados['errores']:
        with st.expander("❌ Errores Encontrados", expanded=True):
            for error in resultados['errores']:
                st.error(error)

def mostrar_datos_procesados(resultados):
    """Muestra los datos procesados en tabs"""
    if (resultados['ap_procesados'].empty and resultados['comp_procesados'].empty and
        resultados['ap_filtrados'].empty and resultados['comp_filtrados'].empty):
        st.info("No hay datos para mostrar")
        return
    
    tabs = st.tabs(["📋 AP Procesados", "✅ AP Válidos", "📋 COMP Procesados", "✅ COMP Válidos"])
    
    datasets = [
        ("AP Procesados", resultados['ap_procesados']),
        ("AP Válidos", resultados['ap_filtrados']),
        ("COMP Procesados", resultados['comp_procesados']),
        ("COMP Válidos", resultados['comp_filtrados'])
    ]
    
    for i, (nombre, df) in enumerate(datasets):
        with tabs[i]:
            if not df.empty:
                st.write(f"**{nombre}:** {len(df)} registros")
                
                if 'registro_existente' in df.columns:
                    existentes = df['registro_existente'].sum()
                    st.write(f"- Nuevos: {len(df) - existentes}")
                    st.write(f"- Existentes: {existentes}")
                
                st.dataframe(df, use_container_width=True)
                
                # Opción para descargar los datos
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label=f"Descargar {nombre} como CSV",
                    data=csv,
                    file_name=f"{nombre.lower().replace(' ', '_')}.csv",
                    mime='text/csv',
                    key=f'download_{i}'
                )
            else:
                st.info(f"No hay datos de {nombre}")

def main():
    """Función principal de la aplicación"""
    inicializar_estado()
    
    # Conexión a base de datos
    engine, st.session_state.db_connected = conectar_base_datos()
    
    if not st.session_state.db_connected:
        st.error("⚠️ No se puede continuar sin conexión a la base de datos")
        return
    
    st.title("📁 Procesador de Archivos de Pagos")
    st.markdown("---")
    
    # Selección de directorio
    col1, col2 = st.columns([3, 1])
    
    with col1:
        if st.session_state.directorio_seleccionado:
            st.info(f"📂 Directorio seleccionado: `{st.session_state.directorio_seleccionado}`")
        else:
            st.info("👆 Selecciona un directorio para comenzar")
    
    with col2:
        if st.button("🔍 Seleccionar Directorio", type="primary"):
            directorio = seleccionar_directorio()
            if directorio:
                st.session_state.directorio_seleccionado = directorio
                st.session_state.procesamiento_completo = False
                st.session_state.resultados_procesamiento = None
                st.session_state.archivos_procesados = []
                st.rerun()
    
    # Procesamiento
    if st.session_state.directorio_seleccionado:
        st.markdown("---")
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.subheader("🚀 Procesamiento de Archivos")
        
        with col2:
            if st.button("▶️ Procesar Archivos", type="primary"):
                with st.spinner("Procesando archivos..."):
                    resultados = procesar_directorio(st.session_state.directorio_seleccionado, engine)
                    st.session_state.resultados_procesamiento = resultados
                    st.session_state.procesamiento_completo = True
                    st.rerun()
    
    # Mostrar resultados
    if st.session_state.procesamiento_completo and st.session_state.resultados_procesamiento:
        st.markdown("---")
        
        mostrar_resumen_procesamiento(st.session_state.resultados_procesamiento)
        
        # Insertar en base de datos
        if (not st.session_state.resultados_procesamiento['ap_filtrados'].empty or 
            not st.session_state.resultados_procesamiento['comp_filtrados'].empty):
            
            st.markdown("---")
            st.subheader("💾 Inserción en Base de Datos")
            
            col1, col2 = st.columns(2)
            
            with col1:
                if not st.session_state.resultados_procesamiento['ap_filtrados'].empty:
                    if st.button("💾 Insertar/Actualizar AP en BD"):
                        insertados, actualizados = insertar_datos_bd(
                            st.session_state.resultados_procesamiento['ap_filtrados'], 
                            'AP', 
                            engine
                        )
                        if insertados > 0 or actualizados > 0:
                            st.success(f"""
                                ✅ Datos AP procesados correctamente:
                                - Nuevos registros insertados: {insertados}
                                - Registros existentes actualizados: {actualizados}
                            """)
            
            with col2:
                if not st.session_state.resultados_procesamiento['comp_filtrados'].empty:
                    if st.button("💾 Insertar/Actualizar COMP en BD"):
                        insertados, actualizados = insertar_datos_bd(
                            st.session_state.resultados_procesamiento['comp_filtrados'], 
                            'COMP', 
                            engine
                        )
                        if insertados > 0 or actualizados > 0:
                            st.success(f"""
                                ✅ Datos COMP procesados correctamente:
                                - Nuevos registros insertados: {insertados}
                                - Registros existentes actualizados: {actualizados}
                            """)
        
        # Mostrar datos procesados
        st.markdown("---")
        st.subheader("📋 Datos Procesados")
        mostrar_datos_procesados(st.session_state.resultados_procesamiento)
        
        # Histórico de archivos procesados en esta sesión
        if st.session_state.archivos_procesados:
            st.markdown("---")
            st.subheader("📜 Histórico de Procesamiento")
            
            df_historico = pd.DataFrame(st.session_state.archivos_procesados)
            st.dataframe(
                df_historico.sort_values('fecha', ascending=False),
                column_config={
                    "nombre": "Archivo",
                    "tipo": "Tipo",
                    "registros": st.column_config.NumberColumn("Registros"),
                    "nuevos": st.column_config.NumberColumn("Nuevos"),
                    "actualizados": st.column_config.NumberColumn("Actualizados"),
                    "fecha": "Fecha Procesamiento"
                },
                use_container_width=True
            )
        
        # Botón para reset
        if st.button("🔄 Procesar Nuevo Directorio"):
            st.session_state.directorio_seleccionado = None
            st.session_state.procesamiento_completo = False
            st.session_state.resultados_procesamiento = None
            st.session_state.archivos_procesados = []
            st.rerun()

if __name__ == "__main__":
    main()