# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
from src.database.postgres import get_connection
from io import BytesIO
import openpyxl
from src.utils.fondo import set_background

# Configuración de la página
st.set_page_config(
    page_title="SIAMM - Consultas",
    page_icon="src/utils/favicon-114x114.png",
    layout="wide"
)

# Estilos CSS personalizados 
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
    .stMultiSelect [data-baseweb=tag] {{
        background-color: #4CAF50;
    }}
    .filter-header {{
        font-size: 1.2rem;
        font-weight: bold;
        margin-bottom: 0.5rem;
        color: #333;
    }}
    .filter-container {{
        display: flex;
        flex-wrap: wrap;
        gap: 1rem;
        margin-bottom: 1rem;
    }}
    .filter-item {{
        flex: 1;
        min-width: 200px;
    }}
    </style>
""", unsafe_allow_html=True)

# Logo y fondo
st.image("src/utils/logo-andesbpo-359x143.png", width=150)
set_background("src/utils/bg-seccion.png")

# Función para cargar datos base
@st.cache_data(ttl=3600)
def load_base_data():
    conn = get_connection()
    try:
        # Obtener fechas mínimas/máximas
        fecha_min, fecha_max = pd.read_sql(
            "SELECT MIN(fecha_gestion), MAX(fecha_gestion) FROM mv_id_gestiones", 
            conn
        ).iloc[0]
        
        # Obtener asesores únicos
        asesores = pd.read_sql(
            "SELECT DISTINCT asesor FROM mv_id_gestiones WHERE asesor IS NOT NULL ORDER BY asesor", 
            conn
        )['asesor'].tolist()
        
        # Obtener resultados únicos
        resultados = pd.read_sql(
            "SELECT DISTINCT resultado FROM mv_id_gestiones WHERE resultado IS NOT NULL ORDER BY resultado", 
            conn
        )['resultado'].tolist()
        
        # Obtener meses disponibles con nombres completos
        meses_db = pd.read_sql(
            """
            SELECT DISTINCT 
                TO_CHAR(fecha_gestion, 'YYYY-MM') as mes_codigo,
                TO_CHAR(fecha_gestion, 'YYYY') as año,
                TO_CHAR(fecha_gestion, 'Month') as mes_nombre
            FROM mv_id_gestiones 
            ORDER BY mes_codigo
            """, 
            conn
        )
        
        # Crear lista de meses en formato "Mes Año" (ej: "Enero 2023")
        meses = ["Todos"] + [
            f"{row['mes_nombre'].strip()} {row['año']}" 
            for _, row in meses_db.iterrows()
        ]
        
        # Crear mapeo de mes mostrado a mes en formato YYYY-MM
        mes_mapping = {"Todos": "Todos"}
        for _, row in meses_db.iterrows():
            mes_mapping[f"{row['mes_nombre'].strip()} {row['año']}"] = row['mes_codigo']
        
        return {
            'fecha_min': fecha_min,
            'fecha_max': fecha_max,
            'asesores': ["Todos"] + asesores,
            'resultados': ["Todos"] + resultados,
            'meses': meses,
            'mes_mapping': mes_mapping
        }
    finally:
        conn.close()


@st.cache_data(ttl=300)
def get_stats_by_asesor(fecha_inicio, fecha_fin, resultado="Todos", mes="Todos"):
    conn = get_connection()
    try:
        query = """
        SELECT 
            asesor,
            COUNT(DISTINCT id_gestion) as total_gestiones,
            COUNT(DISTINCT documento) as documentos_unicos,
            SUM(CASE WHEN categoria_resultado = 'RPC' THEN 1 ELSE 0 END) as rpc_count,
            SUM(CASE WHEN categoria_resultado = 'WPC' THEN 1 ELSE 0 END) as wpc_count,
            SUM(CASE WHEN categoria_resultado = 'NPC' THEN 1 ELSE 0 END) as npc_count,
            SUM(CASE WHEN resultado IN ('Compromiso de pago', 'Compromiso de acuerdo de pago') THEN 1 ELSE 0 END) as compromisos
        FROM mv_id_gestiones
        WHERE fecha_gestion >= %s::timestamp
        AND fecha_gestion <= (%s::date + INTERVAL '1 day - 1 second')
        """
        
        params = [fecha_inicio, fecha_fin]
        
        if resultado != "Todos":
            if isinstance(resultado, list):
                query += " AND resultado IN %s"
                params.append(tuple(resultado))
            else:
                query += " AND resultado = %s"
                params.append(resultado)
            
        if mes != "Todos":
            query += " AND TO_CHAR(fecha_gestion, 'YYYY-MM') = %s"
            params.append(mes)
            
        query += " GROUP BY asesor ORDER BY total_gestiones DESC"
        
        df = pd.read_sql(query, conn, params=params)
        
        # Calcular porcentajes y métricas adicionales
        if not df.empty:
            df['rpc_pct'] = round((df['rpc_count'] / df['total_gestiones'] * 100), 1)
            df['wpc_pct'] = round((df['wpc_count'] / df['total_gestiones'] * 100), 1)
            df['npc_pct'] = round((df['npc_count'] / df['total_gestiones'] * 100), 1)
            df['efectividad'] = round((df['compromisos'] / df['total_gestiones'] * 100), 1)
            df['gestiones_x_doc'] = round(df['total_gestiones'] / df['documentos_unicos'], 1)
        
        return df
    finally:
        conn.close()


# Función para obtener estadísticas con todos los filtros
@st.cache_data(ttl=300)
def get_stats(fecha_inicio, fecha_fin, asesor="Todos", resultado="Todos", mes="Todos"):
    conn = get_connection()
    try:
        query = """
        SELECT 
            COUNT(DISTINCT id_gestion) as total_gestiones,
            COUNT(DISTINCT documento) as documentos_unicos,
            SUM(CASE WHEN categoria_resultado = 'RPC' THEN 1 ELSE 0 END) as rpc_count,
            SUM(CASE WHEN categoria_resultado = 'WPC' THEN 1 ELSE 0 END) as wpc_count,
            SUM(CASE WHEN categoria_resultado = 'NPC' THEN 1 ELSE 0 END) as npc_count,
            SUM(CASE WHEN resultado IN ('Compromiso de pago', 'Compromiso de acuerdo de pago') THEN 1 ELSE 0 END) as compromisos
        FROM mv_id_gestiones
        WHERE fecha_gestion >= %s::timestamp
        AND fecha_gestion <= (%s::date + INTERVAL '1 day - 1 second')
        """
        
        params = [fecha_inicio, fecha_fin]
        
        if asesor != "Todos":
            query += " AND asesor = %s"
            params.append(asesor)
            
        if resultado != "Todos":
            query += " AND resultado = %s"
            params.append(resultado)
            
        if mes != "Todos":
            query += " AND TO_CHAR(fecha_gestion, 'YYYY-MM') = %s"
            params.append(mes)
            
        stats = pd.read_sql(query, conn, params=params).iloc[0]
        
        # Calcular porcentajes
        total = stats['total_gestiones']
        stats['rpc_pct'] = round((stats['rpc_count'] / total * 100), 1) if total > 0 else 0
        stats['wpc_pct'] = round((stats['wpc_count'] / total * 100), 1) if total > 0 else 0
        stats['npc_pct'] = round((stats['npc_count'] / total * 100), 1) if total > 0 else 0
        stats['efectividad'] = round((stats['compromisos'] / total * 100), 1) if total > 0 else 0
        
        return stats
    finally:
        conn.close()

# Función para obtener gestión diaria con todos los filtros
@st.cache_data(ttl=300)
def get_daily_counts(fecha_inicio, fecha_fin, asesor="Todos", resultado="Todos", mes="Todos"):
    conn = get_connection()
    try:
        query = """
        SELECT 
            fecha_gestion::date as fecha,
            COUNT(DISTINCT id_gestion) as gestiones
        FROM mv_id_gestiones
        WHERE fecha_gestion >= %s::timestamp
        AND fecha_gestion <= (%s::date + INTERVAL '1 day - 1 second')
        """
        
        params = [fecha_inicio, fecha_fin]
        
        if asesor != "Todos":
            query += " AND asesor = %s"
            params.append(asesor)
            
        if resultado != "Todos":
            if isinstance(resultado, list):
                query += " AND resultado IN %s"
                params.append(tuple(resultado))
            else:
                query += " AND resultado = %s"
                params.append(resultado)
            
        if mes != "Todos":
            query += " AND TO_CHAR(fecha_gestion, 'YYYY-MM') = %s"
            params.append(mes)
            
        query += " GROUP BY fecha_gestion::date ORDER BY fecha"
        
        # Obtener datos y asegurar que 'fecha' es datetime
        df = pd.read_sql(query, conn, params=params)
        df['fecha'] = pd.to_datetime(df['fecha'])
        
        # Crear rango completo de fechas como datetime
        rango_fechas = pd.date_range(start=fecha_inicio, end=fecha_fin, freq='D')
        df_fechas = pd.DataFrame({'fecha': rango_fechas})
        
        # Combinar los DataFrames
        df_completo = df_fechas.merge(df, on='fecha', how='left')
        
        # Rellenar NA con 0
        df_completo['gestiones'] = df_completo['gestiones'].fillna(0)
        
        return df_completo
    finally:
        conn.close()
# Función para obtener datos diarios categorizados con todos los filtros
@st.cache_data(ttl=300)
def get_daily_data(fecha_inicio, fecha_fin, asesor="Todos", resultado="Todos", mes="Todos"):
    conn = get_connection()
    try:
        query = """
        SELECT 
            fecha_gestion::date as fecha,
            COUNT(DISTINCT id_gestion) as total_gestiones,
            SUM(CASE WHEN categoria_resultado = 'RPC' THEN 1 ELSE 0 END) as rpc_count,
            SUM(CASE WHEN categoria_resultado = 'WPC' THEN 1 ELSE 0 END) as wpc_count,
            SUM(CASE WHEN categoria_resultado = 'NPC' THEN 1 ELSE 0 END) as npc_count
        FROM mv_id_gestiones
        WHERE fecha_gestion >= %s::timestamp
        AND fecha_gestion <= (%s::date + INTERVAL '1 day - 1 second')
        """
        
        params = [fecha_inicio, fecha_fin]
        
        if asesor != "Todos":
            query += " AND asesor = %s"
            params.append(asesor)
            
        if resultado != "Todos":
            query += " AND resultado = %s"
            params.append(resultado)
            
        if mes != "Todos":
            query += " AND TO_CHAR(fecha_gestion, 'YYYY-MM') = %s"
            params.append(mes)
            
        query += " GROUP BY fecha_gestion::date ORDER BY fecha"
        
        df = pd.read_sql(query, conn, params=params)
        
        # Calcular porcentajes diarios
        if not df.empty:
            df['rpc_pct'] = round((df['rpc_count'] / df['total_gestiones'] * 100), 1)
            df['wpc_pct'] = round((df['wpc_count'] / df['total_gestiones'] * 100), 1)
            df['npc_pct'] = round((df['npc_count'] / df['total_gestiones'] * 100), 1)
        
        return df
    finally:
        conn.close()

# Función para obtener distribución por resultado con todos los filtros
@st.cache_data(ttl=300)
def get_result_distribution(fecha_inicio, fecha_fin, asesor="Todos", resultado="Todos", mes="Todos"):
    conn = get_connection()
    try:
        query = """
        SELECT 
            resultado,
            categoria_resultado,
            COUNT(DISTINCT id_gestion) as cantidad
        FROM mv_id_gestiones
        WHERE fecha_gestion >= %s::timestamp
        AND fecha_gestion <= (%s::date + INTERVAL '1 day - 1 second')
        """
        
        params = [fecha_inicio, fecha_fin]
        
        if asesor != "Todos":
            query += " AND asesor = %s"
            params.append(asesor)
            
        if resultado != "Todos":
            query += " AND resultado = %s"
            params.append(resultado)
            
        if mes != "Todos":
            query += " AND TO_CHAR(fecha_gestion, 'YYYY-MM') = %s"
            params.append(mes)
            
        query += " GROUP BY resultado, categoria_resultado ORDER BY cantidad DESC"
        
        return pd.read_sql(query, conn, params=params)
    finally:
        conn.close()



# Interfaz principal
def main():
    # Sidebar con opciones de módulo
    with st.sidebar:
        st.header("Módulos")
        modulo = st.radio(
            "Seleccione módulo:",
            options=["📊 Gestiones", "📱 SMS", "💰 Pagos"],
            index=0
        )
        
        # Mostrar información adicional según el módulo seleccionado
        if modulo == "📊 Gestiones":
            st.info("Visualización de datos de gestiones telefónicas")
        elif modulo == "📱 SMS":
            st.info("Visualización de datos de envíos SMS")
        else:
            st.info("Visualización de datos de pagos recibidos")

    # Título principal
    st.title(f"Consulta a la tabla Gestiones")
    
    # Cargar datos base (solo para módulo de Gestiones)
    if modulo == "📊 Gestiones":
        data = load_base_data()
        
        # Sección de filtros en el área principal
        st.markdown("---")
        st.markdown("### Filtros")
        
        # Contenedor de filtros
        with st.container():
            # Primera fila de filtros
            col1, col2 = st.columns(2)
            with col1:
                fecha_inicio = st.date_input(
                    "Fecha inicio",
                    value=data['fecha_min'],
                    min_value=data['fecha_min'],
                    max_value=data['fecha_max']
                )
            with col2:
                fecha_fin = st.date_input(
                    "Fecha fin",
                    value=data['fecha_max'],
                    min_value=data['fecha_min'],
                    max_value=data['fecha_max']
                )
            
            # Segunda fila de filtros
            col3, col4, col5 = st.columns(3)
            with col3:
                asesor = st.selectbox(
                    "Asesor:",
                    options=data['asesores'],
                    index=0
                )
            with col4:
                mes_seleccionado = st.selectbox(
                    "Mes:",
                    options=data['meses'],
                    index=0
                )
                
                # Convertir el mes seleccionado al formato YYYY-MM para las consultas
                mes = data['mes_mapping'][mes_seleccionado]
            with col5:
                resultados_seleccionados = st.multiselect(
                    "Resultado:",
                    options=data['resultados'][1:],  # Excluye "Todos"
                    default=[]
                )
            
            # Convertir a "Todos" si no hay selección
            resultado = "Todos" if len(resultados_seleccionados) == 0 else resultados_seleccionados
        
        # Obtener datos con todos los filtros
        # (Ajuste para manejar múltiples resultados seleccionados)
        if resultado == "Todos":
            stats = get_stats(fecha_inicio, fecha_fin, asesor, "Todos", mes)
            daily_counts = get_daily_counts(fecha_inicio, fecha_fin, asesor, "Todos", mes)
            daily_data = get_daily_data(fecha_inicio, fecha_fin, asesor, "Todos", mes)
            result_dist = get_result_distribution(fecha_inicio, fecha_fin, asesor, "Todos", mes)
        else:
            # Para múltiples resultados, necesitamos una lógica diferente
            stats_list = []
            daily_counts_list = []
            daily_data_list = []
            result_dist_list = []
            
            for res in resultado:
                stats_list.append(get_stats(fecha_inicio, fecha_fin, asesor, res, mes))
                daily_counts_list.append(get_daily_counts(fecha_inicio, fecha_fin, asesor, res, mes))
                daily_data_list.append(get_daily_data(fecha_inicio, fecha_fin, asesor, res, mes))
                result_dist_list.append(get_result_distribution(fecha_inicio, fecha_fin, asesor, res, mes))
            
            # Combinar resultados
            def combine_stats(stats_list):
                combined = {
                    'total_gestiones': sum(s['total_gestiones'] for s in stats_list),
                    'documentos_unicos': sum(s['documentos_unicos'] for s in stats_list),
                    'rpc_count': sum(s['rpc_count'] for s in stats_list),
                    'wpc_count': sum(s['wpc_count'] for s in stats_list),
                    'npc_count': sum(s['npc_count'] for s in stats_list),
                    'compromisos': sum(s['compromisos'] for s in stats_list)
                }
                
                total = combined['total_gestiones']
                combined['rpc_pct'] = round((combined['rpc_count'] / total * 100), 1) if total > 0 else 0
                combined['wpc_pct'] = round((combined['wpc_count'] / total * 100), 1) if total > 0 else 0
                combined['npc_pct'] = round((combined['npc_count'] / total * 100), 1) if total > 0 else 0
                combined['efectividad'] = round((combined['compromisos'] / total * 100), 1) if total > 0 else 0
                
                return combined
            
            stats = combine_stats(stats_list)
            daily_counts = pd.concat(daily_counts_list).groupby('fecha').sum().reset_index()
            daily_data = pd.concat(daily_data_list).groupby('fecha').sum().reset_index()
            result_dist = pd.concat(result_dist_list).groupby(['resultado', 'categoria_resultado']).sum().reset_index()
        
        # Mostrar métricas
        st.markdown("---")
        st.header("Métricas Principales")
        
        # Primera fila de métricas
        cols = st.columns(3)
        with cols[0]:
            st.markdown(f"""
                <div class="metric-box">
                    <div class="metric-title">Total Gestiones</div>
                    <div class="metric-value">{stats['total_gestiones']:,}</div>
                </div>
            """, unsafe_allow_html=True)
        
        # with cols[1]:
        #     st.markdown(f"""
        #         <div class="metric-box">
        #             <div class="metric-title">Documentos Únicos</div>
        #             <div class="metric-value">{stats['documentos_unicos']:,}</div>
        #         </div>
        #     """, unsafe_allow_html=True)
        
        with cols[1]:
            st.markdown(f"""
                <div class="metric-box">
                    <div class="metric-title">Efectividad</div>
                    <div class="metric-value">{stats['efectividad']}%</div>
                </div>
            """, unsafe_allow_html=True)
        
        with cols[2]:
            st.markdown(f"""
                <div class="metric-box">
                    <div class="metric-title">Compromisos</div>
                    <div class="metric-value">{stats['compromisos']:,}</div>
                </div>
            """, unsafe_allow_html=True)

        # Segunda fila de métricas (RPC/WPC/NPC)
        cols2 = st.columns(3)
        with cols2[0]:
            st.markdown(f"""
                <div class="metric-box">
                    <div class="metric-title">RPC</div>
                    <div class="metric-value" style="color: #FFA500;">{stats['rpc_pct']}%</div>
                    <div style="font-size: 1rem; margin-top: 5px;">({stats['rpc_count']:,} gestiones)</div>
                </div>
            """, unsafe_allow_html=True)
        
        with cols2[1]:
            st.markdown(f"""
                <div class="metric-box">
                    <div class="metric-title">WPC</div>
                    <div class="metric-value" style="color: #1F77B4;">{stats['wpc_pct']}%</div>
                    <div style="font-size: 1rem; margin-top: 5px;">({stats['wpc_count']:,} gestiones)</div>
                </div>
            """, unsafe_allow_html=True)
        
        with cols2[2]:
            st.markdown(f"""
                <div class="metric-box">
                    <div class="metric-title">NPC</div>
                    <div class="metric-value" style="color: #FF0000;">{stats['npc_pct']}%</div>
                    <div style="font-size: 1rem; margin-top: 5px;">({stats['npc_count']:,} gestiones)</div>
                </div>
            """, unsafe_allow_html=True)

        # Información del período y filtros aplicados
        dias_periodo = (fecha_fin - fecha_inicio).days + 1
        resultados_filtro = ", ".join(resultado) if resultado != "Todos" else "Todos"
        mes_filtro = mes_seleccionado if mes != "Todos" else "Todos"
        st.markdown(f"""
            <div class="period-info">
                <strong>Período analizado:</strong> 
                {fecha_inicio.strftime('%d/%m/%Y')} - {fecha_fin.strftime('%d/%m/%Y')} 
                ({dias_periodo} días) | 
                <strong>Filtros:</strong> 
                Asesor: {asesor} | Mes: {mes_filtro} | Resultados: {resultados_filtro}
            </div>
        """, unsafe_allow_html=True)

        st.markdown("---")
        st.header("Resumen por Asesor")
        
        # Obtener datos por asesor
        df_asesores = get_stats_by_asesor(fecha_inicio, fecha_fin, resultado, mes)
        
        if not df_asesores.empty:
            # Formatear el DataFrame para visualización
            df_display = df_asesores.copy()
            df_display = df_display[[
                'asesor', 'total_gestiones', 'documentos_unicos', 'gestiones_x_doc',
                'rpc_count', 'rpc_pct', 'wpc_count', 'wpc_pct', 
                'npc_count', 'npc_pct', 'compromisos', 'efectividad'
            ]]
            
            # Renombrar columnas para mejor visualización
            df_display.columns = [
                'Asesor', 'Total Gestiones', 'Doc. Únicos', 'Gestiones/Doc',
                'RPC (Cant)', 'RPC (%)', 'WPC (Cant)', 'WPC (%)',
                'NPC (Cant)', 'NPC (%)', 'Compromisos', 'Efectividad (%)'
            ]
            
            # Formatear números
            df_display['Total Gestiones'] = df_display['Total Gestiones'].apply(lambda x: f"{x:,}")
            df_display['Doc. Únicos'] = df_display['Doc. Únicos'].apply(lambda x: f"{x:,}")
            df_display['RPC (Cant)'] = df_display['RPC (Cant)'].apply(lambda x: f"{x:,}")
            df_display['WPC (Cant)'] = df_display['WPC (Cant)'].apply(lambda x: f"{x:,}")
            df_display['NPC (Cant)'] = df_display['NPC (Cant)'].apply(lambda x: f"{x:,}")
            df_display['Compromisos'] = df_display['Compromisos'].apply(lambda x: f"{x:,}")
            
            # Configurar altura dinámica
            altura_tabla = min(400, 35 * (len(df_display) + 35))
            
            # Mostrar tabla con estilo
            st.dataframe(
                df_display.style.format({
                    'RPC (%)': '{:.1f}%',
                    'WPC (%)': '{:.1f}%',
                    'NPC (%)': '{:.1f}%',
                    'Efectividad (%)': '{:.1f}%',
                    'Gestiones/Doc': '{:.1f}'
                }),
                use_container_width=True,
                height=altura_tabla
            )
            
            
            # Crear columnas para los botones
            col1, col2 = st.columns(2)
            
            with col1:
                # Botón para exportar a Excel
                def to_excel(df):
                    output = BytesIO()
                    writer = pd.ExcelWriter(output, engine='openpyxl')
                    df.to_excel(writer, index=False, sheet_name='Gestiones')
                    workbook = writer.book
                    worksheet = writer.sheets['Gestiones']
                    
                    # Formato de columnas
                    worksheet.column_dimensions['A'].width = 15
                    worksheet.column_dimensions['B'].width = 15
                    
                    writer.close()
                    processed_data = output.getvalue()
                    return processed_data
                
                excel_data = to_excel(df_display)
                
                st.download_button(
                    label="📊 Exportar a Excel",
                    data=excel_data,
                    file_name=f"gestiones_diarias_{fecha_inicio}_{fecha_fin}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    help="Descargar datos en formato Excel (.xlsx)"
                )
            
            with col2:
                # Botón para exportar a CSV
                csv_data = df_display.to_csv(index=False).encode('utf-8')
                
                st.download_button(
                    label="📝 Exportar a CSV",
                    data=csv_data,
                    file_name=f"gestiones_diarias_{fecha_inicio}_{fecha_fin}.csv",
                    mime="text/csv",
                    help="Descargar datos en formato CSV (valores separados por comas)"
                )
        else:
            st.warning("No hay datos de asesores para el período seleccionado")

        # Gráficos en pestañas
        tab1, tab2, tab3, tab4 = st.tabs([
            "📈 Gestiones por Día", 
            "📊 Tendencia Categorías", 
            "📉 Distribución RPC/WPC/NPC", 
            "📌 Resultados Detallados"
        ])
        
        with tab1:
            st.subheader("Gestiones por Día")
            if not daily_counts.empty:
                # Convertir fecha a string para el gráfico (opcional)
                daily_counts['fecha_str'] = daily_counts['fecha'].dt.strftime('%Y-%m-%d')
                
                # Crear gráfico solo con días que tienen datos
                fig = px.bar(
                    daily_counts[daily_counts['gestiones'] > 0],  # Filtrar días con gestiones > 0
                    x='fecha_str',  # Usar la columna de string para el eje x
                    y='gestiones',
                    labels={'fecha_str': 'Fecha', 'gestiones': 'Número de Gestiones'},
                    color_discrete_sequence=['#4CAF50']
                )
                
                # Configuración adicional del gráfico
                fig.update_layout(
                    xaxis_title="Fecha",
                    yaxis_title="Gestiones Únicas",
                    hovermode="x unified",
                    xaxis=dict(
                        type='category',  # Tratar fechas como categorías discretas
                        tickmode='linear',
                        dtick=1  # Mostrar todas las fechas
                    )
                )
                
                # Rotar etiquetas del eje x para mejor visualización
                fig.update_xaxes(tickangle=45)
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos para el período seleccionado")
        with tab2:
            st.subheader("Tendencia Diaria de Categorías")
            if not daily_data.empty:
                fig = px.line(
                    daily_data,
                    x='fecha',
                    y=['rpc_pct', 'wpc_pct', 'npc_pct'],
                    labels={'value': 'Porcentaje', 'variable': 'Categoría'},
                    title='Evolución Diaria',
                    color_discrete_map={
                        'rpc_pct': '#FFA500',
                        'wpc_pct': '#1F77B4',
                        'npc_pct': '#FF0000'
                    }
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos para el período seleccionado")
        
        with tab3:
            st.subheader("Distribución por Categoría")
            if not result_dist.empty:
                cat_dist = result_dist.groupby('categoria_resultado')['cantidad'].sum().reset_index()
                
                fig = px.pie(
                    cat_dist,
                    names='categoria_resultado',
                    values='cantidad',
                    title='Distribución',
                    color='categoria_resultado',
                    color_discrete_map={
                        'RPC': '#FFA500',
                        'WPC': '#1F77B4',
                        'NPC': '#FF0000',
                        'Sin etiqueta': '#808080'
                    }
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos para el período seleccionado")
        
        with tab4:
            st.subheader("Resultados Detallados")
            if not result_dist.empty:
                fig = px.bar(
                    result_dist.head(20),
                    x='resultado',
                    y='cantidad',
                    color='categoria_resultado',
                    title='Top 20 Resultados',
                    color_discrete_map={
                        'RPC': '#FFA500',
                        'WPC': '#1F77B4',
                        'NPC': '#FF0000',
                        'Sin etiqueta': '#808080'
                    }
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No hay datos para el período seleccionado")
    
    elif modulo == "📱 SMS":
        st.warning("Módulo SMS en desarrollo - Próximamente")
        st.info("Aquí se mostrarán las estadísticas de envíos SMS")
    
    elif modulo == "💰 Pagos":
        st.warning("Módulo Pagos en desarrollo - Próximamente")
        st.info("Aquí se mostrarán las estadísticas de pagos recibidos")

if __name__ == "__main__":
    main()


