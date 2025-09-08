import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date
from dateutil.relativedelta import relativedelta
from src.database.postgres import DatabaseManager
from assets.fondo import set_background
from sqlalchemy import text
from calendar import monthrange
import io
import pathlib

# ==============================================================================
# CONFIGURACIÓN INICIAL
# ==============================================================================
# Configuración de página
st.set_page_config(
    page_title="SIAMM - Análisis de Cartera",
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
# CONEXIÓN A BASE DE DATOS
# ==============================================================================
engine_simm = DatabaseManager.get_engine('SIMM')

# ==============================================================================
# FUNCIONES AUXILIARES
# ==============================================================================
@st.cache_data(ttl=3600)
def get_promesas(fecha_inicio, fecha_fin):
    """Obtiene las promesas de pago únicas en el rango de fechas especificado"""
    with engine_simm.connect() as conn:
        query = text("""
        WITH promesas_unicas AS (
            SELECT 
                id_gestion,  -- Asumiendo que existe un campo identificador único
                numero_comparendo,
                asesor,
                fecha_compromiso,
                valor,
                documento,
                ROW_NUMBER() OVER (
                    PARTITION BY documento, numero_comparendo 
                    ORDER BY fecha_compromiso DESC
                ) as rn
            FROM gestiones
            WHERE resultado IN ('Compromiso de acuerdo de pago', 'Compromiso de pago')
            AND fecha_compromiso BETWEEN :fecha_inicio AND :fecha_fin
        )
        SELECT 
            numero_comparendo,
            asesor,
            fecha_compromiso,
            valor,
            documento
        FROM promesas_unicas
        WHERE rn = 1  -- Solo tomamos el registro más reciente por combinación documento/comparendo
        ORDER BY fecha_compromiso DESC, asesor
        """)
        return pd.read_sql(query, conn, params={
            'fecha_inicio': fecha_inicio,
            'fecha_fin': fecha_fin
        })

@st.cache_data(ttl=3600)
def get_promesas_pendientes(fecha_inicio=None, fecha_fin=None):
    """Obtiene las promesas de pago pendientes (vencidas y por vencer)"""
    with engine_simm.connect() as conn:
        # Si no se especifican fechas, usamos los últimos 30 días por defecto
        fecha_filtro_inicio = "fecha_compromiso >= CURRENT_DATE - INTERVAL '30 days'"
        params = {}
        
        if fecha_inicio and fecha_fin:
            fecha_filtro_inicio = "fecha_compromiso BETWEEN :fecha_inicio AND :fecha_fin"
            params = {
                'fecha_inicio': fecha_inicio,
                'fecha_fin': fecha_fin
            }
        elif fecha_inicio:
            fecha_filtro_inicio = "fecha_compromiso >= :fecha_inicio"
            params = {'fecha_inicio': fecha_inicio}
        elif fecha_fin:
            fecha_filtro_inicio = "fecha_compromiso <= :fecha_fin"
            params = {'fecha_fin': fecha_fin}
            
        query = text(f"""
        WITH promesas_unicas AS (
            SELECT 
                id_gestion,
                numero_comparendo,
                asesor,
                fecha_compromiso,
                valor,
                documento,
                ROW_NUMBER() OVER (
                    PARTITION BY documento, numero_comparendo 
                    ORDER BY fecha_compromiso DESC
                ) as rn
            FROM gestiones g
            WHERE resultado IN ('Compromiso de acuerdo de pago', 'Compromiso de pago')
            AND {fecha_filtro_inicio}
            AND NOT EXISTS (
                SELECT 1 FROM pagos p 
                WHERE p.documento = g.documento
                AND p.fecha_pago >= g.fecha_compromiso
            )
        )
        SELECT 
            numero_comparendo,
            asesor,
            fecha_compromiso,
            valor,
            documento,
            CASE 
                WHEN fecha_compromiso < CURRENT_DATE THEN 'Vencida'
                WHEN fecha_compromiso = CURRENT_DATE THEN 'Vence Hoy'
                ELSE 'Por Vencer'
            END as estado,
            CURRENT_DATE - fecha_compromiso as dias_vencido
        FROM promesas_unicas
        WHERE rn = 1
        ORDER BY fecha_compromiso, asesor
        """)
        return pd.read_sql(query, conn, params=params)

def procesar_datos(df):
    """Procesa los datos para el reporte"""
    df['fecha_compromiso'] = pd.to_datetime(df['fecha_compromiso']).dt.date
    df['valor'] = pd.to_numeric(df['valor'])
    return df

def crear_grafico_barras(df, x_col, y_col, title, color_col=None):
    """Crea un gráfico de barras con Plotly"""
    if color_col:
        fig = px.bar(df, x=x_col, y=y_col, color=color_col, title=title)
    else:
        fig = px.bar(df, x=x_col, y=y_col, title=title)
    
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#2c3e50')
    )
    return fig

def crear_grafico_lineas(df, x_col, y_col, title, group_col=None):
    """Crea un gráfico de líneas con Plotly"""
    if group_col:
        fig = px.line(df, x=x_col, y=y_col, color=group_col, title=title, markers=True)
    else:
        fig = px.line(df, x=x_col, y=y_col, title=title, markers=True)
    
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#2c3e50')
    )
    return fig

# ==============================================================================
# SECCIÓN REPORTE
# ==============================================================================
def mostrar_reporte():
    st.markdown('<h2 class="section-header">📊 Reporte de Compromisos</h2>', unsafe_allow_html=True)
    
    # Obtener el rango de fechas real de los compromisos
    with engine_simm.connect() as conn:
        fecha_min_max = pd.read_sql(text("""
            SELECT 
                MIN(fecha_compromiso) as min_fecha,
                MAX(fecha_compromiso) as max_fecha
            FROM gestiones
            WHERE resultado IN ('Compromiso de acuerdo de pago', 'Compromiso de pago')
        """), conn)

    hoy = date.today()
    fecha_fin_default = fecha_min_max['max_fecha'].iloc[0]  # Eliminar .date() aquí
    
    # Filtros de fecha con estilo mejorado
    col1, col2 = st.columns(2)

    with col1:
        fecha_inicio = st.date_input(
            "📅 Fecha inicial:",
            value=hoy,
            disabled=False
        )

    with col2:
        fecha_fin = st.date_input(
            "📅 Fecha final:",
            value=fecha_fin_default,
            min_value=hoy,
            max_value=fecha_fin_default
        )

    # Validación de fechas
    if fecha_inicio > fecha_fin:
        st.error("La fecha inicial no puede ser mayor a la fecha final")
        st.stop()

    # Carga de datos con las fechas seleccionadas
    with st.spinner("Cargando datos..."):
        df_compromisos = get_promesas(fecha_inicio, fecha_fin)
        df_pendientes = get_promesas_pendientes(fecha_inicio, fecha_fin)
        
        if df_compromisos.empty and df_pendientes.empty:
            st.warning("No se encontraron compromisos pendientes desde hoy hasta la fecha seleccionada")
            return
        
        if not df_compromisos.empty:
            df_compromisos = procesar_datos(df_compromisos)
        
        if not df_pendientes.empty:
            df_pendientes = procesar_datos(df_pendientes)
    st.markdown("""---""")
    # COMPROMISOS PENDIENTES
    if not df_pendientes.empty:
        st.markdown('<h3 class="section-header">🔴 Compromisos Pendientes</h3>', unsafe_allow_html=True)
        
        # Métricas de pendientes con estilo mejorado
        
        col1, col2, col3, col4 = st.columns(4)
        vencidas = df_pendientes[df_pendientes['estado'] == 'Vencida']
        vence_hoy = df_pendientes[df_pendientes['estado'] == 'Vence Hoy']
        por_vencer = df_pendientes[df_pendientes['estado'] == 'Por Vencer']
        
        col1.metric("📋 Total Pendientes", len(df_pendientes), f"${df_pendientes['valor'].sum():,.0f}")
        col2.metric("🔴 Vencidas", len(vencidas), f"${vencidas['valor'].sum():,.0f}" if not vencidas.empty else "$0")
        col3.metric("⚠️ Vencen Hoy", len(vence_hoy), f"${vence_hoy['valor'].sum():,.0f}" if not vence_hoy.empty else "$0")
        col4.metric("🟢 Por Vencer", len(por_vencer), f"${por_vencer['valor'].sum():,.0f}" if not por_vencer.empty else "$0")
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Análisis por asesor de pendientes
        pendientes_asesor = df_pendientes.groupby(['asesor', 'estado']).agg({
            'numero_comparendo': 'count',
            'valor': 'sum'
        }).reset_index()
        
        if not pendientes_asesor.empty:
            fig_pendientes = px.bar(
                pendientes_asesor, 
                x='asesor', 
                y='valor', 
                color='estado',
                title='Valor de Compromisos Pendientes por Asesor y Estado',
                color_discrete_map={
                    'Vencida': '#dc3545',
                    'Vence Hoy': '#ffc107', 
                    'Por Vencer': '#28a745'
                }
            )
            fig_pendientes.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='#2c3e50')
            )
            st.plotly_chart(fig_pendientes, use_container_width=True)
        
        # Tabla detallada de pendientes
        st.subheader("Detalle de Compromisos Pendientes")
        st.dataframe(
            df_pendientes.sort_values(['estado', 'fecha_compromiso']),
            use_container_width=True,
            hide_index=True,
            column_config={
                "valor": st.column_config.NumberColumn(format="$%,.0f"),
                "fecha_compromiso": st.column_config.DateColumn(format="DD/MM/YYYY"),
                "estado": st.column_config.TextColumn()
            }
        )
    
    # ANÁLISIS DE COMPROMISOS DEL PERÍODO
    if not df_compromisos.empty:
        st.markdown('<h3 class="section-header">📈 Análisis del Período Seleccionado</h3>', unsafe_allow_html=True)
        
        # Métricas generales con estilo mejorado
        col1, col2, col3 = st.columns(3)
        col1.metric("📊 Total Compromisos", len(df_compromisos))
        col2.metric("💰 Valor Total", f"${df_compromisos['valor'].sum():,.0f}")
        col3.metric("📈 Valor Promedio", f"${df_compromisos['valor'].mean():,.0f}")
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Análisis por día
        st.subheader("📅 Análisis por Día")
        analisis_dia = df_compromisos.groupby('fecha_compromiso').agg({
            'numero_comparendo': 'count',
            'valor': ['sum', 'mean']
        }).round(0)
        analisis_dia.columns = ['Cantidad', 'Valor Total', 'Valor Promedio']
        analisis_dia = analisis_dia.sort_index(ascending=False)
        
        # Gráfico de evolución diaria
        analisis_dia_reset = analisis_dia.reset_index()
        fig_dia = crear_grafico_lineas(
            analisis_dia_reset, 
            'fecha_compromiso', 
            'Valor Total',
            'Evolución Diaria del Valor de Compromisos'
        )
        st.plotly_chart(fig_dia, use_container_width=True)
        
        col1, col2 = st.columns(2)
        with col1:
            st.dataframe(
                analisis_dia,
                use_container_width=True,
                column_config={
                    "Valor Total": st.column_config.NumberColumn(format="$%,.0f"),
                    "Valor Promedio": st.column_config.NumberColumn(format="$%,.0f")
                }
            )
        
        # Análisis por asesor
        st.subheader("👤 Análisis por Asesor")
        analisis_asesor = df_compromisos.groupby('asesor').agg({
            'numero_comparendo': 'count',
            'valor': ['sum', 'mean']
        }).round(0)
        analisis_asesor.columns = ['Cantidad', 'Valor Total', 'Valor Promedio']
        analisis_asesor = analisis_asesor.sort_values('Valor Total', ascending=False)
        
        # Gráfico por asesor
        analisis_asesor_reset = analisis_asesor.reset_index()
        fig_asesor = crear_grafico_barras(
            analisis_asesor_reset,
            'asesor',
            'Valor Total',
            'Valor Total de Compromisos por Asesor'
        )
        st.plotly_chart(fig_asesor, use_container_width=True)
        
        with col2:
            st.dataframe(
                analisis_asesor,
                use_container_width=True,
                column_config={
                    "Valor Total": st.column_config.NumberColumn(format="$%,.0f"),
                    "Valor Promedio": st.column_config.NumberColumn(format="$%,.0f")
                }
            )
        
        # Análisis por comparendo (top 10)
        st.subheader("🏆 Top 10 Comparendos por Valor")
        top_comparendos = df_compromisos.nlargest(10, 'valor')[['numero_comparendo', 'asesor', 'fecha_compromiso', 'valor', 'documento']]
        st.dataframe(
            top_comparendos,
            use_container_width=True,
            hide_index=True,
            column_config={
                "valor": st.column_config.NumberColumn(format="$%,.0f"),
                "fecha_compromiso": st.column_config.DateColumn(format="DD/MM/YYYY")
            }
        )
        
        # Matriz de correlación día-asesor
        st.subheader("📊 Matriz Día-Asesor")
        matriz_dia_asesor = df_compromisos.groupby(['fecha_compromiso', 'asesor'])['valor'].sum().unstack(fill_value=0)
        
        if not matriz_dia_asesor.empty:
            fig_heatmap = px.imshow(
                matriz_dia_asesor.values,
                x=matriz_dia_asesor.columns,
                y=[str(fecha) for fecha in matriz_dia_asesor.index],
                aspect="auto",
                title="Mapa de Calor: Valor de Compromisos por Día y Asesor"
            )
            fig_heatmap.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='#2c3e50')
            )
            st.plotly_chart(fig_heatmap, use_container_width=True)

# ==============================================================================
# SECCIÓN DESCARGA
# ==============================================================================
def mostrar_descarga():
    st.markdown('<h2 class="section-header">📥 Generador de Descargas</h2>', unsafe_allow_html=True)
    
    # Filtros de fecha con estilo mejorado
    
    col1, col2 = st.columns(2)
    hoy = date.today()
    fecha_inicio_default = date(hoy.year, hoy.month, 1)
    
    with col1:
        fecha_inicio = st.date_input(
            "📅 Fecha inicial:",
            value=fecha_inicio_default,
            min_value=date(2023, 1, 1),
            max_value=hoy,
            key="desc_fecha_inicio"
        )
    with col2:
        fecha_fin = st.date_input(
            "📅 Fecha final:",
            value=hoy,
            min_value=date(2023, 1, 1),
            max_value=hoy,
            key="desc_fecha_fin"
        )
    st.markdown('</div>', unsafe_allow_html=True)
    
    if fecha_inicio > fecha_fin:
        st.error("La fecha inicial no puede ser mayor a la fecha final")
        return
    
    # Carga de datos
    with st.spinner("Cargando datos..."):
        df = get_promesas(fecha_inicio, fecha_fin)
        
        if df.empty:
            st.warning("No se encontraron datos en el período seleccionado")
            return
        
        df = procesar_datos(df)
    
    # Configuración de descarga con estilo mejorado
    
    st.subheader("⚙️ Configurar Descarga")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Selección de campos
        campos_disponibles = {
            'fecha_compromiso': 'Fecha Compromiso',
            'asesor': 'Asesor',
            'numero_comparendo': 'Número Comparendo',
            'documento': 'Documento',
            'valor': 'Valor'
        }
        
        campos_seleccionados = st.multiselect(
            "📋 Seleccionar campos a incluir:",
            options=list(campos_disponibles.keys()),
            default=['fecha_compromiso', 'asesor', 'numero_comparendo', 'valor'],
            format_func=lambda x: campos_disponibles[x]
        )
    
    with col2:
        # Opciones de agrupación
        tipo_reporte = st.radio(
            "📊 Tipo de reporte:",
            options=['Detallado', 'Agrupado por Asesor', 'Agrupado por Día', 'Agrupado por Asesor y Día'],
            index=0
        )
        
        # Filtro por asesor
        asesores = ['Todos'] + sorted(df['asesor'].unique().tolist())
        asesor_filtro = st.selectbox(
            "👤 Filtrar por asesor:",
            options=asesores,
            index=0
        )
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Aplicar filtros
    df_filtrado = df.copy()
    if asesor_filtro != 'Todos':
        df_filtrado = df_filtrado[df_filtrado['asesor'] == asesor_filtro]
    
    # Generar DataFrame según el tipo de reporte
    if tipo_reporte == 'Detallado':
        df_descarga = df_filtrado[campos_seleccionados].copy()
        
    elif tipo_reporte == 'Agrupado por Asesor':
        # Agrupar por asesor manteniendo algunos campos relevantes
        df_descarga = df_filtrado.groupby('asesor').agg({
            'numero_comparendo': 'count',
            'valor': 'sum',
            'documento': lambda x: ', '.join(x.unique()[:5]) + ('...' if len(x.unique()) > 5 else '')  # Primeros 5 documentos
        }).rename(columns={
            'numero_comparendo': 'Cantidad_Promesas',
            'valor': 'Valor_Total',
            'documento': 'Documentos_Muestra'
        }).reset_index()
        
    elif tipo_reporte == 'Agrupado por Día':
        # Agrupar por día manteniendo información de documentos
        df_descarga = df_filtrado.groupby('fecha_compromiso').agg({
            'numero_comparendo': 'count',
            'valor': 'sum',
            'asesor': 'nunique',
            'documento': lambda x: ', '.join(x.unique()[:5]) + ('...' if len(x.unique()) > 5 else '')  # Primeros 5 documentos
        }).rename(columns={
            'numero_comparendo': 'Cantidad_Promesas',
            'valor': 'Valor_Total',
            'asesor': 'Cantidad_Asesores',
            'documento': 'Documentos_Muestra'
        }).reset_index()
        
    else:  # Agrupado por Asesor y Día
        # Agrupar por asesor y día manteniendo información de documentos
        df_descarga = df_filtrado.groupby(['fecha_compromiso', 'asesor']).agg({
            'numero_comparendo': 'count',
            'valor': 'sum',
            'documento': lambda x: ', '.join(x.unique()[:3]) + ('...' if len(x.unique()) > 3 else '')  # Primeros 3 documentos
        }).rename(columns={
            'numero_comparendo': 'Cantidad_Promesas',
            'valor': 'Valor_Total',
            'documento': 'Documentos_Muestra'
        }).reset_index()
    
    # Vista previa en tiempo real con estilo mejorado
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.subheader("👀 Vista Previa del Archivo")
    st.write(f"**📊 Registros a descargar:** {len(df_descarga)}")
    
    # Mostrar métricas de la vista previa
    if 'Valor_Total' in df_descarga.columns:
        col1, col2, col3 = st.columns(3)
        col1.metric("📋 Total Registros", len(df_descarga))
        col2.metric("💰 Valor Total", f"${df_descarga['Valor_Total'].sum():,.0f}")
        col3.metric("📈 Valor Promedio", f"${df_descarga['Valor_Total'].mean():,.0f}")
    elif 'valor' in df_descarga.columns:
        col1, col2, col3 = st.columns(3)
        col1.metric("📋 Total Registros", len(df_descarga))
        col2.metric("💰 Valor Total", f"${df_descarga['valor'].sum():,.0f}")
        col3.metric("📈 Valor Promedio", f"${df_descarga['valor'].mean():,.0f}")
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Mostrar tabla de vista previa
    st.dataframe(
        df_descarga.head(20),
        use_container_width=True,
        hide_index=True,
        column_config={
            "valor": st.column_config.NumberColumn(format="$%,.0f"),
            "Valor_Total": st.column_config.NumberColumn(format="$%,.0f"),
            "fecha_compromiso": st.column_config.DateColumn(format="DD/MM/YYYY")
        }
    )
    
    if len(df_descarga) > 20:
        st.info(f"Mostrando los primeros 20 registros de {len(df_descarga)} totales")
    
    # Botón de descarga
    if not df_descarga.empty:
        # Convertir a Excel
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_descarga.to_excel(writer, sheet_name='Reporte_Compromisos', index=False)
            
            # Formatear el archivo Excel
            workbook = writer.book
            worksheet = writer.sheets['Reporte_Compromisos']
            
            # Formato para moneda
            money_format = workbook.add_format({'num_format': '$#,##0'})
            date_format = workbook.add_format({'num_format': 'dd/mm/yyyy'})
            
            # Aplicar formatos
            for idx, col in enumerate(df_descarga.columns):
                if 'valor' in col.lower() or 'total' in col.lower():
                    worksheet.set_column(idx, idx, 15, money_format)
                elif 'fecha' in col.lower():
                    worksheet.set_column(idx, idx, 12, date_format)
                else:
                    worksheet.set_column(idx, idx, 15)
        
        processed_data = output.getvalue()
        
        nombre_archivo = f"reporte_compromisos_{tipo_reporte.lower().replace(' ', '_')}_{fecha_inicio}_a_{fecha_fin}.xlsx"
        
        st.download_button(
            label="📥 Descargar Reporte en Excel",
            data=processed_data,
            file_name=nombre_archivo,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary"
        )

# ==============================================================================
# INTERFAZ PRINCIPAL
# ==============================================================================
def main():
    st.markdown("<h1 class='section-title'>💼 Análisis de Cartera </h1>", unsafe_allow_html=True)
    st.markdown("---")
    # Sidebar para navegación
    with st.sidebar:
        
        # Inicializar el estado de sesión si no existe
        if 'current_page' not in st.session_state:
            st.session_state.current_page = "📊 Reporte"
        
        # Botón para Reporte
        if st.button("📊 Reporte", 
                    use_container_width=True,
                    type="primary" if st.session_state.current_page == "📊 Reporte" else "secondary"):
            st.session_state.current_page = "📊 Reporte"
        
        # Botón para Descarga
        if st.button("📥 Descarga", 
                    use_container_width=True,
                    type="primary" if st.session_state.current_page == "📥 Descarga" else "secondary"):
            st.session_state.current_page = "📥 Descarga"
        
        st.markdown("---")
        st.markdown("""
        <div class="sidebar-instructions">
        <strong>📊 Reporte:</strong><br>
        - Compromisos pendientes<br>
        - Análisis por día/asesor<br>
        - Gráficos interactivos<br><br>
        
        <strong>📥 Descarga:</strong><br>
        - Configurar campos<br>
        - Vista previa en tiempo real<br>
        - Descarga en Excel
        </div>
        """, unsafe_allow_html=True)
    
    # Mostrar la sección correspondiente
    if st.session_state.current_page == "📊 Reporte":
        mostrar_reporte()
    else:
        mostrar_descarga()

if __name__ == "__main__":
    main()