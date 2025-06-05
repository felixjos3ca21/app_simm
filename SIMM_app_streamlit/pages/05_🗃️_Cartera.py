import streamlit as st
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
from src.database.postgres import DatabaseManager
from src.utils.fondo import set_background
from sqlalchemy import text
from calendar import monthrange

# ==============================================================================
# CONFIGURACIÓN INICIAL
# ==============================================================================
st.set_page_config(
    page_title="SIAMM - Análisis de Cartera",
    page_icon="src/utils/favicon-114x114.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos CSS
st.markdown("""
    <style>
    [data-testid=stSidebar] {
        background-color: #a5d6a7 !important;
    }
    .main-container {
        padding: 2rem;
    }
    div[role=radiogroup] {
        gap: 0.5rem;
    }
    .sidebar .sidebar-title {
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
    div[role=radiogroup] label:hover {
        background-color: #e2e8f0 !important;
    }
    .stProgress > div > div > div > div {
        background-color: #2e7d32;
    }
    .st-b7 {
        background-color: #e8f5e9 !important;
    }
    .stMarkdown {
        margin-bottom: 2rem;
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

# ==============================================================================
# FUNCIONES AUXILIARES
# ==============================================================================
@st.cache_data(ttl=3600)
def get_promesas(fecha_inicio, fecha_fin):
    """Obtiene las promesas de pago en el rango de fechas especificado"""
    with engine_simm.connect() as conn:
        query = text("""
        SELECT 
            numero_comparendo,
            asesor,
            fecha_compromiso,
            valor,
            documento
        FROM gestiones
        WHERE resultado IN ('Compromiso de acuerdo de pago', 'Compromiso de pago')
        AND fecha_compromiso BETWEEN :fecha_inicio AND :fecha_fin
        ORDER BY fecha_compromiso, asesor
        """)
        return pd.read_sql(query, conn, params={
            'fecha_inicio': fecha_inicio,
            'fecha_fin': fecha_fin
        })

def procesar_datos(df):
    """Procesa los datos para el reporte"""
    df['fecha_compromiso'] = pd.to_datetime(df['fecha_compromiso']).dt.date
    df['valor'] = pd.to_numeric(df['valor'])
    return df

# ==============================================================================
# INTERFAZ PRINCIPAL
# ==============================================================================
def main():
    st.title("📋 Reporte de Promesas de Pago")
    st.markdown("Visualización de compromisos de pago por asesor")
    
    # 1. Filtros en pantalla principal
    with st.container():
        st.markdown('<div class="filter-container">', unsafe_allow_html=True)
        
        # Selección de rango de fechas completo
        hoy = date.today()
        fecha_inicio_default = date(hoy.year, hoy.month, 1)
        
        col1, col2 = st.columns(2)
        with col1:
            fecha_inicio = st.date_input(
                "Fecha inicial:",
                value=fecha_inicio_default,
                min_value=date(2023, 1, 1),
                max_value=hoy
            )
        with col2:
            fecha_fin = st.date_input(
                "Fecha final:",
                value=hoy,
                min_value=date(2023, 1, 1),
                max_value=hoy
            )
        
        # Validación de fechas
        if fecha_inicio > fecha_fin:
            st.error("La fecha inicial no puede ser mayor a la fecha final")
            return
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # 2. Carga y procesamiento de datos
    with st.spinner(f"Cargando datos del {fecha_inicio} al {fecha_fin}..."):
        df = get_promesas(fecha_inicio, fecha_fin)
        
        if df.empty:
            st.warning("No se encontraron promesas de pago en el rango de fechas seleccionado")
            return
        
        df = procesar_datos(df)
        
        # Filtro por asesor
        asesores = sorted(df['asesor'].unique())
        asesor_seleccionado = st.selectbox(
            "Filtrar por asesor:",
            options=["Todos"] + asesores,
            index=0
        )
        
        if asesor_seleccionado != "Todos":
            df = df[df['asesor'] == asesor_seleccionado]
    
    # 3. Visualización de resultados
    st.header(f"Resumen de Promesas ({fecha_inicio} a {fecha_fin})")
    
    # Métricas clave
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Promesas", len(df))
    col2.metric("Monto Total", f"${df['valor'].sum():,.0f}")
    col3.metric("Asesores", df['asesor'].nunique())
    
    # Agrupación por día y asesor
    st.subheader("Detalle por Día")
    df_dia_asesor = df.groupby(['fecha_compromiso', 'asesor']).agg({
        'numero_comparendo': 'count',
        'valor': 'sum'
    }).rename(columns={
        'numero_comparendo': 'Cantidad',
        'valor': 'Total'
    }).sort_values('fecha_compromiso', ascending=False)
    
    st.dataframe(
        df_dia_asesor,
        use_container_width=True,
        column_config={
            "Total": st.column_config.NumberColumn(format="$%,.0f")
        }
    )
    
    # Detalle completo
    st.subheader("Detalle Completo de Promesas")
    st.dataframe(
        df[['fecha_compromiso', 'asesor', 'numero_comparendo', 'documento', 'valor']],
        use_container_width=True,
        hide_index=True,
        column_config={
            "valor": st.column_config.NumberColumn(format="$%,.0f"),
            "fecha_compromiso": st.column_config.DateColumn(format="DD/MM/YYYY")
        }
    )
    
    # Opción para descargar
    csv = df[['fecha_compromiso', 'asesor', 'numero_comparendo', 'documento', 'valor']]\
        .to_csv(index=False).encode('utf-8')
    
    st.download_button(
        label="Descargar Reporte Completo (CSV)",
        data=csv,
        file_name=f"reporte_promesas_{fecha_inicio}_a_{fecha_fin}.csv",
        mime="text/csv",
        use_container_width=True
    )

if __name__ == "__main__":
    main()