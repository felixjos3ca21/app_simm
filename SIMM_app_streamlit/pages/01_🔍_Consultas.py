# -*- coding: utf-8 -*-
import sys
from pathlib import Path
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from src.database.postgres import get_connection
from src.utils.fondo import set_background

# Configuración inicial
sys.path.append(str(Path(__file__).parent))
st.set_page_config(
    page_title="SIAMM - Consultas", 
    page_icon="src/utils/favicon-114x114.png", 
    layout="wide"
)

# Estilos personalizados
st.markdown(f"""
    <style>
    [data-testid="stSidebar"] {{
        background-color: #A6DEB7 !important;
    }}
    .stSlider>div>div>div>div {{background: #4CAF50}}
    .metric-box {{
        background-color: #A6DEB7;
        border-radius: 10px;
        padding: 1.5rem;
        margin: 1rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        text-align: center;
    }}
    .metric-title {{
        font-size: 1.2rem;
        color: #333;
        margin-bottom: 0.5rem;
    }}
    .metric-value {{
        font-size: 2rem;
        font-weight: bold;
        color: #000;
    }}
    .period-info {{
        text-align: center; 
        margin: 1rem;
        font-size: 1.1rem;
    }}
    .filter-box {{
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 1rem;
        margin-bottom: 1rem;
    }}
    </style>
""", unsafe_allow_html=True)

# Logo y fondo
st.image("src/utils/logo-andesbpo-359x143.png", width=150)
set_background("src/utils/bg-seccion.png")

# ==============================================
# FUNCIONES DE DATOS (CON TABLA CALENDARIO)
# ==============================================
@st.cache_data(ttl=3600)
def load_base_data():
    """Carga los datos esenciales para filtros usando la tabla calendario"""
    conn = get_connection()
    try:
        # Obtener fechas mínimas/máximas desde calendario
        fecha_min, fecha_max = pd.read_sql(
            "SELECT MIN(fecha), MAX(fecha) FROM calendario", 
            conn
        ).iloc[0]
        
        # Obtener asesores desde gestiones
        asesores = pd.read_sql(
            "SELECT DISTINCT asesor FROM gestiones WHERE asesor IS NOT NULL ORDER BY asesor", 
            conn
        )['asesor'].tolist()
        
        # Obtener resultados desde gestiones
        resultados = pd.read_sql(
            "SELECT DISTINCT resultado FROM gestiones WHERE resultado IS NOT NULL ORDER BY resultado", 
            conn
        )['resultado'].tolist()
        
        # Obtener meses disponibles desde calendario
        meses_disponibles = pd.read_sql(
            "SELECT DISTINCT mes, nombre_mes FROM calendario ORDER BY mes", 
            conn
        )
        
        return {
            'fecha_min': fecha_min,  # Elimina .date() porque ya es date
            'fecha_max': min(fecha_max, datetime.now().date()),  # Elimina .date() aquí también
            'asesores': ["Todos"] + asesores,
            'resultados': ["Todos"] + resultados,
            'meses_disponibles': meses_disponibles.set_index('mes')['nombre_mes'].to_dict()
        }
    finally:
        conn.close()

@st.cache_data(ttl=300)
def get_stats(fecha_inicio, fecha_fin, asesor="Todos", resultado="Todos", mes="Todos"):
    """Obtiene estadísticas usando JOIN con la tabla calendario"""
    conn = get_connection()
    try:
        query = """
        SELECT 
            COUNT(DISTINCT g.id_gestion) as total_gestiones,
            COUNT(DISTINCT g.documento) as documentos_unicos,
            SUM(CASE WHEN g.resultado = 'Compromiso de pago' THEN 1 ELSE 0 END) as compromisos_pago,
            SUM(CASE WHEN g.resultado = 'Compromiso de acuerdo de pago' THEN 1 ELSE 0 END) as acuerdos_pago
        FROM gestiones g
        JOIN calendario c ON DATE(g.fecha_gestion) = c.fecha
        WHERE c.fecha BETWEEN %s AND %s
        """
        
        params = [fecha_inicio, fecha_fin + timedelta(days=1)]
        
        # Aplicar filtros adicionales
        if asesor != "Todos":
            query += " AND g.asesor = %s"
            params.append(asesor)
        
        if resultado != "Todos":
            query += " AND g.resultado = %s"
            params.append(resultado)
            
        if mes != "Todos":
            query += " AND c.mes = %s"
            params.append(mes)
        
        return pd.read_sql(query, conn, params=params).iloc[0]
    finally:
        conn.close()

@st.cache_data(ttl=300)
def get_gestiones_por_dia(fecha_inicio, fecha_fin, asesor="Todos", resultado="Todos", mes="Todos"):
    """Obtiene datos para gráfico usando JOIN con calendario"""
    conn = get_connection()
    try:
        query = """
        SELECT 
            c.fecha,
            COUNT(DISTINCT g.id_gestion) as cantidad
        FROM gestiones g
        JOIN calendario c ON DATE(g.fecha_gestion) = c.fecha
        WHERE c.fecha BETWEEN %s AND %s
        """
        
        params = [fecha_inicio, fecha_fin + timedelta(days=1)]
        
        # Aplicar filtros adicionales
        if asesor != "Todos":
            query += " AND g.asesor = %s"
            params.append(asesor)
        
        if resultado != "Todos":
            query += " AND g.resultado = %s"
            params.append(resultado)
            
        if mes != "Todos":
            query += " AND c.mes = %s"
            params.append(mes)
        
        query += " GROUP BY c.fecha ORDER BY c.fecha"
        
        return pd.read_sql(query, conn, params=params)
    finally:
        conn.close()

@st.cache_data(ttl=300)
def get_gestiones_por_resultado(fecha_inicio, fecha_fin, asesor="Todos", mes="Todos"):
    """Obtiene la distribución de gestiones por resultado"""
    conn = get_connection()
    try:
        query = """
        SELECT 
            g.resultado,
            COUNT(DISTINCT g.id_gestion) as cantidad
        FROM gestiones g
        JOIN calendario c ON DATE(g.fecha_gestion) = c.fecha
        WHERE c.fecha BETWEEN %s AND %s
        AND g.resultado IS NOT NULL
        AND g.resultado != ''
        """
        
        params = [fecha_inicio, fecha_fin + timedelta(days=1)]
        
        if asesor != "Todos":
            query += " AND g.asesor = %s"
            params.append(asesor)
            
        if mes != "Todos":
            query += " AND c.mes = %s"
            params.append(mes)
        
        query += " GROUP BY g.resultado HAVING COUNT(DISTINCT g.id_gestion) > 0 ORDER BY cantidad DESC"
        
        return pd.read_sql(query, conn, params=params)
    except Exception as e:
        st.error(f"Error en la consulta: {str(e)}")
        return pd.DataFrame()
    finally:
        conn.close()
# ==============================================
# BARRA LATERAL
# ==============================================
def sidebar():
    with st.sidebar:
        st.header("Consultas por Tabla")
        opcion = st.radio(
            "Tablas disponibles:",
            options=["Gestiones", "SMS", "Pagos"],
            index=0,
            label_visibility="collapsed"
        )
        
        st.markdown("---")
        st.markdown("**Configuración:**")
        st.caption("Base de datos: PostgreSQL")
        st.caption(f"Última actualización: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    
    return opcion

def view_gestiones():
    st.title("📊 Panel de Gestiones")
    
    # Cargar datos base
    data = load_base_data()
    
    # ============ FILTROS ============
    st.header("Filtros")
    
    with st.container():
        
        # 1. Filtro de rango de fechas
        rango_fechas = st.slider(
            "Seleccione el rango de fechas:",
            min_value=data['fecha_min'],
            max_value=data['fecha_max'],
            value=(data['fecha_min'], data['fecha_max']),
            format="YYYY-MM-DD"
        )
        
        cols = st.columns(3)
        
        # 2. Filtro por asesor
        with cols[0]:
            asesor = st.selectbox(
                "Asesor:",
                options=data['asesores'],
                index=0
            )
        
        # 3. Filtro por resultado
        with cols[1]:
            resultado = st.selectbox(
                "Resultado:",
                options=data['resultados'],
                index=0
            )
        
        # 4. Filtro por mes
        with cols[2]:
            meses_opciones = ["Todos"] + list(data['meses_disponibles'].keys())
            mes_seleccionado = st.selectbox(
                "Mes:",
                options=meses_opciones,
                format_func=lambda x: "Todos" if x == "Todos" else data['meses_disponibles'].get(x, x)
            )
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Ajustar rango de fechas si se selecciona un mes específico
    if mes_seleccionado != "Todos":
        conn = get_connection()
        try:
            fecha_min_mes, fecha_max_mes = pd.read_sql(
                f"SELECT MIN(fecha), MAX(fecha) FROM calendario WHERE mes = {mes_seleccionado}", 
                conn
            ).iloc[0]
            rango_fechas = (fecha_min_mes, fecha_max_mes)
        finally:
            conn.close()
    
    # ============ ESTADÍSTICAS ============
    st.header("Estadísticas")
    
    stats = get_stats(rango_fechas[0], rango_fechas[1], asesor, resultado, mes_seleccionado)
    dias_periodo = (rango_fechas[1] - rango_fechas[0]).days + 1
    total_acuerdos = stats['compromisos_pago'] + stats['acuerdos_pago']
    
    # Mostrar métricas
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(f"""<div class="metric-box"><div class="metric-title">Total Gestiones</div><div class="metric-value">{stats['total_gestiones']:,}</div></div>""", unsafe_allow_html=True)
    with col2:
        st.markdown(f"""<div class="metric-box"><div class="metric-title">Documentos Únicos</div><div class="metric-value">{stats['documentos_unicos']:,}</div></div>""", unsafe_allow_html=True)
    with col3:
        promedio = round(stats['total_gestiones'] / dias_periodo) if dias_periodo > 0 else 0
        st.markdown(f"""<div class="metric-box"><div class="metric-title">Promedio Diario</div><div class="metric-value">{promedio:,}</div></div>""", unsafe_allow_html=True)
    with col4:
        st.markdown(f"""<div class="metric-box"><div class="metric-title">Acuerdos/Compromisos</div><div class="metric-value">{total_acuerdos:,}</div></div>""", unsafe_allow_html=True)
    
    st.markdown(f"""<div class="period-info"><strong>Período analizado:</strong> {rango_fechas[0].strftime('%d/%m/%Y')} - {rango_fechas[1].strftime('%d/%m/%Y')} ({dias_periodo} días)</div>""", unsafe_allow_html=True)
    
    # ============ GRÁFICOS EN TABS ============
    st.header("Visualización de Datos")
    tab1, tab2 = st.tabs(["📈 Tendencia Diaria", "📊 Distribución por Resultado"])

    with tab1:
        st.subheader("Gestiones por Día")
        df_diario = get_gestiones_por_dia(rango_fechas[0], rango_fechas[1], asesor, resultado, mes_seleccionado)
        
        if not df_diario.empty:
            fig1 = px.bar(
                df_diario,
                x='fecha',
                y='cantidad',
                labels={'fecha': 'Fecha', 'cantidad': 'Gestiones'},
                color_discrete_sequence=['#4CAF50']
            )
            fig1.update_layout(
                xaxis_title="Fecha",
                yaxis_title="Número de Gestiones",
                hovermode="x unified"
            )
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.warning("No hay datos de gestión diaria con los filtros seleccionados")

    with tab2:
        st.subheader("Distribución por Resultado")
        
        df_resultados = get_gestiones_por_resultado(rango_fechas[0], rango_fechas[1], asesor, mes_seleccionado)
        
        if not df_resultados.empty:
            fig2 = px.bar(
                df_resultados,
                x='resultado',       # Eje X: los diferentes resultados
                y='cantidad',        # Eje Y: la cantidad de gestiones
                color='resultado',   # Color por tipo de resultado
                color_discrete_sequence=px.colors.qualitative.Pastel,
                labels={
                    'resultado': 'Tipo de Resultado',
                    'cantidad': 'Número de Gestiones'
                },
                title='Distribución de Gestiones por Resultado'
            )

            # Personalización adicional del gráfico
            fig2.update_layout(
                xaxis_title='Tipo de Resultado',
                yaxis_title='Cantidad de Gestiones',
                showlegend=False,    # Opcional: ocultar leyenda si hay muchos resultados
                hovermode='x unified'
            )

            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.warning("No hay datos de resultados con los filtros seleccionados")

        st.write("Datos crudos obtenidos:")
        st.write(df_resultados)

# ==============================================
# VISTAS DE SMS Y PAGOS (PENDIENTES)
# ==============================================
def view_sms():
    st.title("📱 Consultas de SMS")
    st.write("Esta funcionalidad estará disponible próximamente.")

def view_pagos():
    st.title("💳 Consultas de Pagos")
    st.write("Esta funcionalidad estará disponible próximamente.")

# ==============================================
# ESTRUCTURA PRINCIPAL
# ==============================================
def main():
    opcion = sidebar()
    
    if opcion == "Gestiones":
        view_gestiones()
    elif opcion == "SMS":
        view_sms()
    elif opcion == "Pagos":
        view_pagos()

if __name__ == "__main__":
    main()