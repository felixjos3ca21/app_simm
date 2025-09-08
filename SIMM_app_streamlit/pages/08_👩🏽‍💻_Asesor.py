# 08_Asesor.py
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import logging
from assets.fondo import set_background
import calendar
import pathlib

# ==============================================================================
# CONFIGURACIÓN INICIAL
# ==============================================================================
st.set_page_config(
    page_title="SIAMM - Consulta Asesores",
    page_icon="assets/images/favicon-114x114.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Cargar CSS global si existe
css_path = pathlib.Path("assets/css/global.css")
if css_path.exists():
    with open(css_path, encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# Logo y fondo
st.image("assets/images/logo-andesbpo-359x143.png", width=350)
set_background("assets/images/bg-seccion.png")

# ==============================================================================

# Cargar variables de entorno
load_dotenv()

# Configuración de base de datos 
DB_CONFIG = {
    'host': os.getenv('DB_SIMM_HOST', 'localhost'),
    'database': os.getenv('DB_SIMM_NAME', 'SIMM'),
    'user': os.getenv('DB_SIMM_USER', 'postgres'),
    'password': os.getenv('DB_SIMM_PASSWORD', 'admin'),
    'port': os.getenv('DB_SIMM_PORT', '5432')
}

def get_db_engine():
    """Crea y retorna una conexión a la base de datos"""
    connection_string = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(connection_string)

def format_time(time_value):
    """Formatea valores de tiempo para mostrar en la interfaz"""
    if pd.isna(time_value):
        return "00:00:00"
    if isinstance(time_value, (int, float)):
        hours = int(time_value // 3600)
        minutes = int((time_value % 3600) // 60)
        seconds = int(time_value % 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return str(time_value)

@st.cache_data(ttl=300)  # Cache por 5 minutos
def load_temporal_data():
    """Carga datos temporales de los últimos 2 meses para optimizar consultas"""
    try:
        engine = get_db_engine()
        
        # Calcular fechas del mes actual y anterior
        today = datetime.now().date()
        current_month_start = today.replace(day=1)
        
        # Mes anterior
        if current_month_start.month == 1:
            previous_month_start = current_month_start.replace(year=current_month_start.year - 1, month=12, day=1)
        else:
            previous_month_start = current_month_start.replace(month=current_month_start.month - 1, day=1)
        
        # Query optimizada para cargar solo datos de 2 meses
        query = """
        SELECT 
            agent_id,
            agent_name,
            agent_status,
            time,
            date_ini,
            date_end,
            conn_id,
            type_interaction,
            destiny,
            telephone,
            campaign_id,
            agent_dni,
            module,
            day,
            month,
            month_name,
            week,
            date,
            archivo_origen
        FROM asesor_2
        WHERE date >= :start_date AND date <= :end_date
        ORDER BY date DESC, agent_id
        """
        
        params = {
            'start_date': previous_month_start,
            'end_date': today
        }
        
        df = pd.read_sql(text(query), engine, params=params)
        
        # Conversión a segundos si es necesario
        if 'time' in df.columns and not df.empty:
            # Si los valores son muy grandes, probablemente están en nanosegundos
            if df['time'].max() > 1e7:  # 10 millones de segundos ~115 días
                df['time'] = (df['time'] / 1e9).astype(int)
        
        # Formatear tiempo
        if 'time' in df.columns and not df.empty:
            df['time_formatted'] = df['time'].apply(format_time)
        
        # Agregar información de carga
        st.session_state.data_load_info = {
            'total_records': len(df),
            'date_range': f"{previous_month_start} a {today}",
            'load_time': datetime.now().strftime("%H:%M:%S")
        }
        
        return df
        
    except Exception as e:
        st.error(f"Error al cargar datos temporales: {str(e)}")
        return pd.DataFrame()

def get_available_dates_from_temp_data(temp_df):
    """Obtiene las fechas disponibles del DataFrame temporal"""
    if temp_df.empty:
        return []
    
    # Convertir columna date a datetime si no lo está
    temp_df['date'] = pd.to_datetime(temp_df['date']).dt.date
    # Obtener fechas únicas y ordenarlas
    unique_dates = sorted(temp_df['date'].unique(), reverse=True)
    return unique_dates

def get_agents_from_temp_data(temp_df):
    """Obtiene lista de agentes únicos del DataFrame temporal"""
    if temp_df.empty:
        return pd.DataFrame(columns=['agent_id', 'agent_name'])
    
    agents = temp_df[['agent_id', 'agent_name']].drop_duplicates().sort_values('agent_name')
    return agents

def filter_data_by_date(temp_df, selected_date):
    """Filtra el DataFrame temporal por fecha específica"""
    if temp_df.empty:
        return pd.DataFrame()
    
    # Asegurar que la columna date esté en formato datetime
    temp_df = temp_df.copy()
    temp_df['date'] = pd.to_datetime(temp_df['date']).dt.date
    
    # Filtrar por fecha
    filtered_df = temp_df[temp_df['date'] == selected_date].copy()
    
    return filtered_df

def time_to_seconds(time_str):
    """Convierte formato HH:MM:SS a segundos"""
    if pd.isna(time_str) or time_str == '00:00:00':
        return 0
    try:
        parts = str(time_str).split(':')
        if len(parts) == 3:
            hours, minutes, seconds = map(int, parts)
            return hours * 3600 + minutes * 60 + seconds
        return 0
    except:
        return 0

def create_pivot_table(df, agent_filter=None):
    """Crea tabla pivotada con asesores como filas y estados como columnas"""
    if df.empty:
        return pd.DataFrame()
    
    # Aplicar filtro de agente si se especifica
    filtered_df = df.copy()
    if agent_filter and agent_filter != "Todos":
        filtered_df = filtered_df[filtered_df['agent_name'] == agent_filter]
    
    if filtered_df.empty:
        return pd.DataFrame()
    
    # Agrupar por agente y estado, sumando los tiempos
    grouped = filtered_df.groupby(['agent_name', 'agent_status']).agg({
        'time': 'sum'
    }).reset_index()
    
    # Crear tabla pivotada
    pivot_table = grouped.pivot(index='agent_name', columns='agent_status', values='time')
    pivot_table = pivot_table.fillna(0)
    
    # Formatear tiempos y crear DataFrame final
    final_df = pd.DataFrame(index=pivot_table.index)
    
    # Agregar columnas formateadas
    for col in pivot_table.columns:
        final_df[col] = pivot_table[col].apply(format_time)
    
    # Resetear índice para mostrar agent_name como columna
    final_df.reset_index(inplace=True)
    final_df.rename(columns={'agent_name': 'Nombre Agente'}, inplace=True)
    
    # Calcular tiempo total por asesor (suma de segundos y luego formatear)
    time_cols = [col for col in final_df.columns if col != 'Nombre Agente']
    total_seconds_per_agent = []
    
    for idx, row in final_df.iterrows():
        total_seconds = sum([time_to_seconds(row[col]) for col in time_cols])
        total_seconds_per_agent.append(format_time(total_seconds))
    
    final_df['Tiempo Total'] = total_seconds_per_agent
    
    return final_df

def main():
    """Función principal de la aplicación"""
    st.markdown("<h1 class='section-title'>👤 Consulta de Asesores</h1>", unsafe_allow_html=True)
    
    # Sidebar con módulos
    with st.sidebar:
        st.markdown("**📋 Seleccionar Módulo**")
        
        # Radio buttons para módulos
        selected_module = st.radio(
            "Módulo:",
            options=["⏰ Tiempos Auxiliares", "📞 Gestiones", "💰 Pagos"],
            index=0
        )
        
        st.markdown("---")
        st.markdown("**ℹ️ Información de Datos**")
        
        # Mostrar información de carga si existe
        if 'data_load_info' in st.session_state:
            info = st.session_state.data_load_info
            st.info(f"📊 {info['total_records']} registros cargados")
            st.info(f"📅 Período: {info['date_range']}")
            st.info(f"🕐 Última carga: {info['load_time']}")
    
    # Verificar módulo seleccionado
    if "Gestiones" in selected_module:
        st.markdown("<div class='warning-box'>", unsafe_allow_html=True)
        st.warning("📞 **Módulo Gestiones**")
        st.write("Este módulo estará disponible próximamente. Actualmente en desarrollo.")
        st.markdown("</div>", unsafe_allow_html=True)
        return
    
    if "Pagos" in selected_module:
        st.markdown("<div class='warning-box'>", unsafe_allow_html=True)
        st.warning("💰 **Módulo Pagos**")
        st.write("Este módulo estará disponible próximamente. Actualmente en desarrollo.")
        st.markdown("</div>", unsafe_allow_html=True)
        return
    
    # Cargar datos temporales (últimos 2 meses)
    with st.spinner("Cargando datos de los últimos 2 meses..."):
        temp_df = load_temporal_data()
    
    if temp_df.empty:
        st.error("No se pudieron cargar los datos. Verifica la conexión a la base de datos.")
        return
    
    # Obtener fechas disponibles del DataFrame temporal
    available_dates = get_available_dates_from_temp_data(temp_df)
    
    if not available_dates:
        st.error("No se encontraron fechas disponibles en los datos cargados.")
        return
    
    # Filtros principales
    st.markdown("<h2 class='section-header'>🔍 Filtros de Consulta</h2>", unsafe_allow_html=True)
    
    # Crear columnas para los filtros
    filter_col1, filter_col2 = st.columns(2)
    
    with filter_col1:
        # Filtro de fecha (único día)
        selected_date = st.selectbox(
            "📅 Seleccionar Fecha",
            options=available_dates,
            index=0,  # Por defecto la fecha más reciente
            format_func=lambda x: x.strftime("%Y-%m-%d (%A)") if hasattr(x, 'strftime') else str(x)
        )
    
    # Filtrar datos por fecha seleccionada
    daily_df = filter_data_by_date(temp_df, selected_date)
    
    if daily_df.empty:
        st.error(f"No se encontraron datos para la fecha {selected_date}")
        return
    
    with filter_col2:
        # Filtro por asesor
        agents_df = get_agents_from_temp_data(daily_df)
        
        if not agents_df.empty:
            agent_options = ["Todos"] + agents_df['agent_name'].tolist()
            selected_agent = st.selectbox(
                "🧑‍💼 Seleccionar Asesor",
                options=agent_options,
                index=0
            )
        else:
            st.warning("No hay agentes disponibles")
            selected_agent = "Todos"
    st.markdown("""---""")
    # Crear tabla pivotada
    try:
        pivot_data = create_pivot_table(daily_df, selected_agent)
        
        # Mostrar datos en tabla pivotada
        st.markdown("<h2 class='section-header'>📋 Tiempos por Asesor y Estado</h2>", unsafe_allow_html=True)
        
        if not pivot_data.empty:
            # Mostrar información resumida
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("👥 Asesores mostrados", len(pivot_data))
            with col2:
                total_estados = len([col for col in pivot_data.columns if col not in ['Nombre Agente', 'Tiempo Total']])
                st.metric("📊 Estados encontrados", total_estados)
            with col3:
                st.metric("📅 Fecha consultada", str(selected_date))
            
            st.markdown("---")
            
            # Mostrar tabla pivotada
            st.dataframe(
                pivot_data,
                use_container_width=True,
                hide_index=True
            )
            
            # Información adicional
            st.info(f"📊 Mostrando tiempos agrupados por estado para {len(pivot_data)} asesor(es) en la fecha {selected_date}")
            
            # Botón de descarga
            csv = pivot_data.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Descargar Datos (CSV)",
                data=csv,
                file_name=f"tiempos_asesores_{selected_date}_{datetime.now().strftime('%H%M%S')}.csv",
                mime="text/csv"
            )
            
        else:
            st.info("No se encontraron registros con los filtros seleccionados")
    
    except Exception as e:
        st.error(f"Error al procesar datos: {str(e)}")
    
    # Botón para recargar datos
    if st.button("🔄 Recargar Datos"):
        st.cache_data.clear()
        st.rerun()
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; padding: 20px 0;'>
        <p><strong>🗂️ Sistema SIAMM - Tiempos por Asesor y Estado</strong></p>
        <p>Consulta diaria de tiempos agrupados por estado</p>
        <p><em>Versión Streamlit - Desarrollado para AndesBPO</em></p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()