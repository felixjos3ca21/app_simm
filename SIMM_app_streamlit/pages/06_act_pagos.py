import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date
from dateutil.relativedelta import relativedelta
from src.database.postgres import DatabaseManager
from src.utils.fondo import set_background
from sqlalchemy import text
from calendar import monthrange
import io
from src.procesador_streamlit.explorador import seleccionar_carpeta_base, obtener_txt_recursivamente
from src.procesador_streamlit.clasificadores import (
    detectar_tipo_archivo,
    calcular_hash_archivo,
    consultar_estado_archivo
)
from src.procesador_streamlit.previsualizador import cargar_previsualizacion
from pathlib import Path
from src.procesador_streamlit.clasificadores import (
    detectar_tipo_archivo,
    calcular_hash_archivo,
    consultar_estado_archivo
)

from src.procesador_streamlit.procesador import (
    procesar_archivo,
    clasificar_registros
)
from src.procesador_streamlit.consolidado import guardar_en_consolidado

from src.procesador_streamlit.insertador import (
    insertar_registros,
    actualizar_registros,
    registrar_archivo
)

# ==============================================================================
# CONFIGURACIÓN INICIAL
# ==============================================================================
st.set_page_config(
    page_title="SIAMM - Actualización de Pagos",
    page_icon="src/utils/favicon-114x114.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==============================================================================
# CONFIGURACIÓN DE ESTILOS MEJORADOS
# ==============================================================================
st.markdown("""
    <style>
    /* Sidebar mejorado */
    [data-testid=stSidebar] {
        background: #a5d6a7 !important;
        padding: 20px 10px;
    }
    
    [data-testid=stSidebar] .sidebar-content {
        background: rgba(255, 255, 255, 0.1);
        border-radius: 10px;
        padding: 15px;
        margin-bottom: 20px;
    }
    
    /* Contenedor principal */
    .main-container {
        padding: 2rem;
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        min-height: 100vh;
    }
    
    /* Tarjetas de métricas */
    .metric-card {
        background: white;
        padding: 20px;
        border-radius: 15px;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
        border: 1px solid rgba(255,255,255,0.2);
        margin: 10px 0;
        transition: transform 0.3s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-5px);
    }
    
    /* Títulos mejorados */
    .section-title {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 2rem;
        font-weight: bold;
        margin: 30px 0 20px 0;
        text-align: center;
    }
    
    .section-header {
        color: #2e7d32;
        border-bottom: 2px solid #a5d6a7;
        padding-bottom: 0.5rem;
        margin-bottom: 1rem;
    }
    
    /* Botones mejorados */
    .stButton > button {
        background: linear-gradient(120deg, #a5d6a7 0%, #2ecc71 100%);
        color: white;
        border: none;
        border-radius: 25px;
        padding: 12px 30px;
        font-weight: bold;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6);
    }
    
    /* Alertas mejoradas */
    .alert-success {
        background: linear-gradient(135deg, #a8e6cf 0%, #7fcdcd 100%);
        border-radius: 10px;
        padding: 15px;
        margin: 10px 0;
    }
    
    .alert-warning {
        background: linear-gradient(135deg, #ffd93d 0%, #ff6b6b 100%);
        border-radius: 10px;
        padding: 15px;
        margin: 10px 0;
    }
    
    /* Tabs mejorados */
    .stTabs [data-baseweb="tab-list"] {
        gap: 20px;
        background: rgba(255,255,255,0.1);
        border-radius: 25px;
        padding: 5px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        border-radius: 20px;
        padding: 10px 20px;
        font-weight: bold;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(120deg, #a5d6a7 0%, #2ecc71 100%);
        color: white;
    }
    
    /* Estilos para los filtros principales */
    .main-filters {
        background: rgba(255, 255, 255, 0.8);
        backdrop-filter: blur(10px);
        border-radius: 15px;
        padding: 20px;
        margin: 20px 0;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
        border: 1px solid rgba(255,255,255,0.2);
    }
    
    /* Mejorar selectbox y inputs */
    .stSelectbox > div > div > div {
        background: white;
        border-radius: 10px;
    }
    
    .stDateInput > div > div > input {
        background: white;
        border-radius: 10px;
        border: 2px solid #e0e0e0;
    }
    
    .stRadio > div {
        background: rgba(255, 255, 255, 0.5);
        border-radius: 10px;
        padding: 10px;
    }
    
    /* Mejorar checkboxes */
    .stCheckbox {
        background: rgba(255, 255, 255, 0.3);
        border-radius: 8px;
        padding: 5px 10px;
        margin: 5px 0;
    }
    
    /* Estilos adicionales para mejor apariencia */
    .stProgress > div > div > div > div {
        background-color: #2e7d32;
    }
    
    .stMarkdown {
        margin-bottom: 2rem;
    }
    
    /* Sidebar títulos */
    .sidebar-title {
        color: #2c3e50;
        font-size: 1.2rem;
        margin-bottom: 1rem;
        font-weight: 600;
    }
    
    .sidebar-instructions {
        color: #4a5568;
        font-size: 0.9rem;
        line-height: 1.5;
    }
    </style>
""", unsafe_allow_html=True)

# Logo y fondo
st.image("src/utils/logo-andesbpo-359x143.png", width=150)
set_background("src/utils/bg-seccion.png")

# ==============================================================================
# CONEXIÓN A BASE DE DATOS
# ==============================================================================
engine_simm = DatabaseManager.get_engine('SIMM')
try:
    engine_simm = DatabaseManager.get_engine('SIMM')
    with engine_simm.connect() as conn:
        conn.execute(text("SELECT 1"))  # prueba simple
    st.success("🟢 Conexión exitosa a la base de datos SIMM")
except Exception as e:
    st.error(f"🔴 Error al conectar con la base de datos SIMM: {e}")
# ==============================================================================
# 
# ==============================================================================


carpeta = seleccionar_carpeta_base()

if carpeta:
    archivos = obtener_txt_recursivamente(carpeta)
    st.markdown(f"### 🔍 Archivos encontrados: {len(archivos)}")

    if archivos:
        tabla_resultado = []

        for archivo in archivos:
            tipo = detectar_tipo_archivo(archivo)
            hash_archivo = calcular_hash_archivo(archivo)
            estado = consultar_estado_archivo(engine_simm, str(archivo.resolve()), hash_archivo)

            tabla_resultado.append({
                "Archivo": archivo.name,
                "Tipo": tipo,
                "Estado": estado,
                "Ruta": str(archivo)
            })

        df_resultado = pd.DataFrame(tabla_resultado)

        st.dataframe(df_resultado, use_container_width=True)

        archivo_seleccionado = st.selectbox("👁 Selecciona un archivo para previsualizar:", df_resultado["Ruta"])

        if archivo_seleccionado:
            archivo_path = Path(archivo_seleccionado)
            try:
                df_preview = cargar_previsualizacion(archivo_path, n_filas=10)
                st.markdown(f"#### Vista previa de: `{archivo_path.name}`")
                st.dataframe(df_preview, use_container_width=True)
            except Exception as e:
                st.error(f"❌ Error al cargar previsualización: {e}")

if st.button("🚀 Procesar todos los archivos válidos"):

    st.info("🔄 Procesando archivos...")

    resumen_por_tipo = {
        "AP": {
            "archivos": 0, "total": 0, "nuevos": 0,
            "actualizables": 0, "duplicados": 0,
            "valor_total": 0, "fechas": [], "ejemplo": pd.DataFrame()
        },
        "COMP": {
            "archivos": 0, "total": 0, "nuevos": 0,
            "actualizables": 0, "duplicados": 0,
            "valor_total": 0, "fechas": [], "ejemplo": pd.DataFrame()
        }
    }

    for archivo in archivos:
        tipo = detectar_tipo_archivo(archivo)
        if tipo not in ["AP", "COMP"]:
            continue

        hash_archivo = calcular_hash_archivo(archivo)
        estado = consultar_estado_archivo(engine_simm, archivo.name, hash_archivo)
        if estado not in ["nuevo", "modificado"]:
            continue

        df = procesar_archivo(archivo, tipo)

        # Guardar respaldo antes de filtrar
        guardar_en_consolidado(df, tipo, archivo.name)

        # Aplicar filtros de validez para BD
        df = df.drop_duplicates(subset="id_registro")
        df = df[(df["valor"] > 0) & df["fecha_liquida"].notna()]
        if df.empty:
            st.warning(f"⚠️ {archivo.name} no tiene registros válidos para base de datos.")
            continue

        tabla_destino = "pagos_ap" if tipo == "AP" else "pagos_comparendos"
        df_nuevos, df_actualizables, df_duplicados = clasificar_registros(df, engine_simm, tabla_destino)

        # Actualizar resumen
        resumen = resumen_por_tipo[tipo]
        resumen["archivos"] += 1
        resumen["total"] += len(df)
        resumen["nuevos"] += len(df_nuevos)
        resumen["actualizables"] += len(df_actualizables)
        resumen["duplicados"] += len(df_duplicados)
        resumen["valor_total"] += df["valor"].sum()
        resumen["fechas"].extend(df["fecha_liquida"].dropna().tolist())

        if resumen["ejemplo"].empty and not df_nuevos.empty:
            resumen["ejemplo"] = df_nuevos.head(5)

        # ⬇ Inserción y actualización real en BD
        insertados = insertar_registros(df_nuevos, tabla_destino, engine_simm)
        actualizados = actualizar_registros(df_actualizables, tabla_destino, engine_simm)
        total_cargados = insertados + actualizados

        if total_cargados > 0:
            registrar_archivo(
                nombre=archivo.name,
                tipo=tipo,
                cantidad=total_cargados,
                estado="exitoso",
                hash_archivo=hash_archivo,
                engine=engine_simm
            )
            st.success(f"📥 {archivo.name} - Insertados: {insertados}, Actualizados: {actualizados}")
        else:
            st.info(f"📄 {archivo.name} - No se insertó ni actualizó ningún registro.")

        st.divider()

    # Mostrar resumen por tipo
    for tipo in ["AP", "COMP"]:
        resumen = resumen_por_tipo[tipo]
        if resumen["total"] == 0:
            continue

        st.subheader(f"📊 Resumen {tipo}")
        col1, col2, col3 = st.columns(3)
        col1.metric("🧾 Archivos procesados", resumen["archivos"])
        col2.metric("📌 Registros totales", resumen["total"])
        col3.metric("💰 Valor total", f"${resumen['valor_total']:,.2f}")

        col4, col5, col6 = st.columns(3)
        col4.metric("🆕 Nuevos", resumen["nuevos"])
        col5.metric("♻️ Actualizables", resumen["actualizables"])
        col6.metric("🚫 Duplicados", resumen["duplicados"])

        fechas_validas = [f for f in resumen["fechas"] if pd.notna(f)]
        if fechas_validas:
            f_min = min(fechas_validas)
            f_max = max(fechas_validas)
            st.markdown(f"📆 Rango de fechas: `{f_min}` → `{f_max}`")

        if not resumen["ejemplo"].empty:
            st.markdown("🔍 Ejemplo de registros nuevos:")
            st.dataframe(resumen["ejemplo"], use_container_width=True)

        st.divider()



