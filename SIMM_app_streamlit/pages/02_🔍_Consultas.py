import sys
from pathlib import Path
import streamlit as st
import pandas as pd
from src.database.postgres import get_connection

sys.path.append(str(Path(__file__).parent))

st.title("🕵️‍♂️ Consultas de Gestiones - Optimizado")

# Widget de selección de vista
consulta_mode = st.radio(
    "Tipo de consulta:",
    ("Resumen Estadístico", "Datos Crudos", "Análisis por Campaña")
)

# Conexión y total de registros
@st.cache_data
def get_total_records():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM gestiones")
    total = cursor.fetchone()[0]
    conn.close()
    return total

total_registros = get_total_records()

# Lógica para cada vista
if consulta_mode == "Resumen Estadístico":
    conn = get_connection()
    
    st.header("KPIs Principales")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Registros", total_registros)
    
    avg_valor = pd.read_sql("SELECT AVG(valor) FROM gestiones", conn).iloc[0,0]
    col2.metric("Valor Promedio", f"${avg_valor:,.2f}")
    
    unique_comparendos = pd.read_sql(
        "SELECT COUNT(DISTINCT identificador_infraccion) FROM gestiones", 
        conn
    ).iloc[0,0]
    col3.metric("Comparendos Únicos", unique_comparendos)
    
    st.subheader("Distribución por Tipo de Llamada")
    df_tipos = pd.read_sql(
        "SELECT tipo_llamada, COUNT(*) as cantidad FROM gestiones GROUP BY tipo_llamada",
        conn
    )
    st.bar_chart(df_tipos.set_index('tipo_llamada'))
    
    conn.close()

elif consulta_mode == "Datos Crudos":
    page_size = 100
    page_number = st.number_input("Página", min_value=1, value=1)
    
    offset = (page_number - 1) * page_size
    query = f"""
        SELECT * 
        FROM gestiones
        ORDER BY fecha_gestion DESC
        LIMIT {page_size} OFFSET {offset}
    """
    
    conn = get_connection()
    df_chunk = pd.read_sql(query, conn)
    conn.close()
    
    st.dataframe(df_chunk)
    st.write(f"Mostrando registros {offset + 1} - {offset + len(df_chunk)} de {total_registros}")

elif consulta_mode == "Análisis por Campaña":
    conn = get_connection()
    
    page_size_campanas = 50
    page_campana = st.number_input("Página de Campañas", min_value=1, value=1)
    
    offset_campana = (page_campana - 1) * page_size_campanas
    campanas = pd.read_sql(
        f"SELECT DISTINCT campana FROM gestiones LIMIT {page_size_campanas} OFFSET {offset_campana}",
        conn
    )['campana'].tolist()
    
    campana = st.selectbox("Selecciona Campaña:", campanas)
    
    df_campana = pd.read_sql(
        "SELECT * FROM gestiones WHERE campana = %s LIMIT 1000",
        conn,
        params=(campana,)
    )
    
    st.dataframe(df_campana)
    conn.close()