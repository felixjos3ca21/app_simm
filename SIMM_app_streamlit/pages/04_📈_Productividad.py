import streamlit as st  
from sqlalchemy import create_engine, text
from src.database.postgres import get_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from decimal import Decimal
import pandas as pd
from datetime import datetime, timedelta
from contextlib import contextmanager
from functools import lru_cache
import numpy as np
from assets.fondo import set_background
from io import BytesIO
import pathlib


# ==============================================================================
# CONFIGURACIÓN INICIAL
# ==============================================================================

st.set_page_config(
    page_title="SIAMM - Análisis de Productividad",
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



col1, col2, col3 = st.columns([1, 2, 1])
# ==============================================================================
# FILTROS PRINCIPALES EN LA PANTALLA
# ==============================================================================
st.markdown("<h1 class='section-title'>📈  Análisis de Productividad</h1>", unsafe_allow_html=True)

st.markdown("---")
st.markdown("## 🎯 Configuración de Análisis")

# Crear columnas para organizar los filtros
col1, col2, col3 = st.columns([2, 2, 1])

with col1:
    st.markdown("#### 📅 Período de Análisis")
    
    # Opciones predefinidas
    periodo_predefinido = st.selectbox(
        "Seleccionar período:",
        ["Personalizado", "Hoy", "Últimos 7 días", "Últimos 30 días", "Este mes", "Mes anterior"],
        key="periodo_select"
    )
    
    # Calcular fechas según selección
    hoy = datetime.now().date()
    if periodo_predefinido == "Hoy":
        fecha_inicio = fecha_fin = hoy
    elif periodo_predefinido == "Últimos 7 días":
        fecha_inicio = hoy - timedelta(days=7)
        fecha_fin = hoy
    elif periodo_predefinido == "Últimos 30 días":
        fecha_inicio = hoy - timedelta(days=30)
        fecha_fin = hoy
    elif periodo_predefinido == "Este mes":
        fecha_inicio = hoy.replace(day=1)
        fecha_fin = hoy
    elif periodo_predefinido == "Mes anterior":
        primer_dia_mes_actual = hoy.replace(day=1)
        fecha_fin = primer_dia_mes_actual - timedelta(days=1)
        fecha_inicio = fecha_fin.replace(day=1)
    else:  # Personalizado
        subcol1, subcol2 = st.columns(2)
        with subcol1:
            fecha_inicio = st.date_input("Desde:", hoy - timedelta(days=30), key="fecha_inicio")
        with subcol2:
            fecha_fin = st.date_input("Hasta:", hoy, key="fecha_fin")

with col2:
    st.markdown("#### 📊 Tipo de Análisis")
    tipo_analisis = st.radio(
        "Seleccionar:",
        ["📈 Análisis de Bases", "📋 Análisis de Gestiones", "🔄 Análisis Completo"],
        index=2,
        key="tipo_analisis"
    )
    
    st.markdown("#### ⚙️ Opciones Avanzadas")
    opciones_col1, opciones_col2 = st.columns(2)
    with opciones_col1:
        mostrar_graficos_detallados = st.checkbox("Gráficos detallados", value=True, key="graficos_detallados")
    with opciones_col2:
        incluir_metricas_avanzadas = st.checkbox("Métricas avanzadas", value=False, key="metricas_avanzadas")



if st.button("🚀 Ejecutar análisis", type="primary"):
    st.session_state.ejecutar_analisis = True
# Definir engine_simm para todo el análisis
engine_simm = get_engine()

# ==============================================
# Logica para el status de bases
# ==============================================

def mostrar_status_bases():
    st.header("📊 Status de Bases vs Gestiones")

    conn = engine_simm.connect()

    # 1. Cargar tabla bases con normalización
    df_bases = pd.read_sql("SELECT documento, base, fecha_entrega FROM bases", conn)
    df_bases['fecha_entrega'] = pd.to_datetime(df_bases['fecha_entrega']).dt.date
    df_bases['documento'] = df_bases['documento'].astype(str).str.strip().str.upper()

    # 2. Seleccionar fecha y base
    fechas = sorted(df_bases['fecha_entrega'].unique(), reverse=True)
    fecha_seleccionada = st.selectbox("📅 Seleccionar fecha de entrega", fechas)
    
    bases_disponibles = df_bases[df_bases['fecha_entrega'] == fecha_seleccionada]['base'].unique()
    base_seleccionada = st.selectbox("📂 Seleccionar base", bases_disponibles)

    # 3. Filtrar documentos de la base seleccionada 
    df_base_filtrada = df_bases[
        (df_bases['fecha_entrega'] == fecha_seleccionada) &
        (df_bases['base'] == base_seleccionada)
    ].drop_duplicates(subset='documento')

    # 4. Determinar rango de fechas de la base (solo informativo)
    fecha_seleccionada_dt = pd.to_datetime(fecha_seleccionada).date()
    st.info(f"📦 Fecha de entrega de la base: {fecha_seleccionada_dt.strftime('%d/%m/%Y')}")

    # 5. Filtro de rango de fechas para gestiones
    hoy = datetime.now().date()
    default_fin = hoy
    default_ini = hoy - pd.Timedelta(days=6)
    fecha_gestiones = st.date_input(
        "Selecciona el rango de fechas de gestiones a analizar",
        value=(default_ini, default_fin),
        min_value=fecha_seleccionada_dt.replace(year=fecha_seleccionada_dt.year-5),
        max_value=hoy,
        format="DD/MM/YYYY"
    )
    if isinstance(fecha_gestiones, tuple) and len(fecha_gestiones) == 2:
        fecha_inicio, fecha_fin = fecha_gestiones
    else:
        fecha_inicio = default_ini
        fecha_fin = default_fin
    st.info(f"🔍 Rango de búsqueda de gestiones: {fecha_inicio.strftime('%d/%m/%Y')} a {fecha_fin.strftime('%d/%m/%Y')}")

    # ===== NUEVO BOTÓN DE BÚSQUEDA =====
    if st.button("🔍 Ejecutar Búsqueda", type="primary"):
        with st.spinner("Buscando datos..."):
            documentos = df_base_filtrada['documento'].tolist()
            fecha_fin_ajustada = fecha_fin + pd.Timedelta(hours=23, minutes=59, seconds=59)

            query_gestiones = """
                WITH jerarquia(resultado, prioridad) AS (
                    VALUES 
                        ('Paz Y Salvo', 1),
                        ('Compromiso de pago', 2),
                        ('Compromiso de acuerdo de pago', 3),
                        ('Caso Especial', 4),
                        ('No Define Fecha De Pago', 5),
                        ('Sin voluntad de pago', 6),
                        ('Mensaje con terceros', 7),
                        ('Mensaje', 8),
                        ('Volver a llamar', 9),
                        ('Entrega Comunicado', 10),
                        ('Localizado', 12),
                        ('Envio De E-Mail', 13),
                        ('No localizado', 14),
                        ('Fallecido', 15),
                        ('Nuevos Datos', 16),
                        ('Nro. inhabilitado', 17),
                        ('Equivocado', 18),
                        ('No contestan', 19),
                        ('Conmutador', 20),
                        ('Ocupado', 21),
                        ('Otros', 22)
                ),
                gestiones_filtradas AS (
                    SELECT 
                        g.documento,
                        g.resultado,
                        g.asesor,
                        g.fecha_gestion,
                        g.id_gestion,
                        COALESCE(j.prioridad, 22) as prioridad
                    FROM gestiones g
                    LEFT JOIN jerarquia j ON g.resultado = j.resultado
                    WHERE g.documento = ANY(%s)
                    AND g.fecha_gestion BETWEEN %s AND %s
                ),
                conteo_gestiones AS (
                    SELECT 
                        documento,
                        COUNT(DISTINCT id_gestion) as total_gestiones
                    FROM gestiones_filtradas
                    GROUP BY documento
                ),
                ultimas_gestiones AS (
                    SELECT 
                        documento,
                        resultado,
                        asesor,
                        fecha_gestion,
                        ROW_NUMBER() OVER (PARTITION BY documento ORDER BY fecha_gestion DESC) as rn_ultima
                    FROM gestiones_filtradas
                ),
                gestiones_positivas AS (
                    SELECT 
                        documento,
                        resultado,
                        asesor,
                        fecha_gestion,
                        ROW_NUMBER() OVER (PARTITION BY documento ORDER BY prioridad ASC, fecha_gestion DESC) as rn_positiva
                    FROM gestiones_filtradas
                )
                SELECT 
                    u.documento,
                    u.resultado as ultimo_resultado,
                    u.asesor as asesor_ultimo,
                    u.fecha_gestion as fecha_ultima_gestion,
                    p.resultado as resultado_positivo,
                    p.asesor as asesor_positivo,
                    p.fecha_gestion as fecha_gestion_positiva,
                    c.total_gestiones
                FROM ultimas_gestiones u
                LEFT JOIN gestiones_positivas p ON u.documento = p.documento AND p.rn_positiva = 1
                LEFT JOIN conteo_gestiones c ON u.documento = c.documento
                WHERE u.rn_ultima = 1
            """

            query_sms = """
                SELECT 
                    documento,
                    COUNT(telefono) as total_sms
                FROM sms
                WHERE documento = ANY(%s)
                AND fecha_sms BETWEEN %s AND %s
                GROUP BY documento
            """

            # Ejecutar consultas
            df_gestiones = pd.read_sql(query_gestiones, conn, 
                                     params=(documentos, fecha_inicio, fecha_fin_ajustada))
            df_gestiones['documento'] = df_gestiones['documento'].astype(str).str.strip().str.upper()

            df_sms = pd.read_sql(query_sms, conn,
                              params=(documentos, fecha_inicio, fecha_fin_ajustada))
            df_sms['documento'] = df_sms['documento'].astype(str).str.strip().str.upper()

            # 6. Unir todos los datos
            df_resultado = df_base_filtrada.merge(
                df_gestiones,
                on='documento',
                how='left'
            ).merge(
                df_sms,
                on='documento',
                how='left'
            )

            # Rellenar valores para documentos sin gestiones en el rango
            df_resultado['ultimo_resultado'] = df_resultado['ultimo_resultado'].fillna('Sin gestiones en rango')
            df_resultado['asesor_ultimo'] = df_resultado['asesor_ultimo'].fillna('N/A')
            df_resultado['fecha_ultima_gestion'] = df_resultado['fecha_ultima_gestion'].fillna(pd.NaT)
            df_resultado['resultado_positivo'] = df_resultado['resultado_positivo'].fillna('Sin gestiones en rango')
            df_resultado['asesor_positivo'] = df_resultado['asesor_positivo'].fillna('N/A')
            df_resultado['fecha_gestion_positiva'] = df_resultado['fecha_gestion_positiva'].fillna(pd.NaT)
            df_resultado['total_gestiones'] = df_resultado['total_gestiones'].fillna(0)
            df_resultado['total_sms'] = df_resultado['total_sms'].fillna(0)

            # 7. Ordenar columnas y datos
            column_order = [
                'documento', 'base', 'fecha_entrega',
                'resultado_positivo', 'asesor_positivo', 'fecha_gestion_positiva',
                'ultimo_resultado', 'asesor_ultimo', 'fecha_ultima_gestion',
                'total_gestiones', 'total_sms'
            ]
            df_resultado = df_resultado[column_order]

            # ===== SECCIÓN DE ESTADÍSTICAS =====
            st.subheader(f"📌 Estadísticas de la Base '{base_seleccionada}'")
            
            # Calcular métricas clave
            n_documentos = len(df_resultado)
            n_con_gestion = len(df_resultado[df_resultado['ultimo_resultado'] != 'Sin gestiones en rango'])
            docs_sin_gestion = n_documentos - n_con_gestion
            total_gestiones = int(df_resultado['total_gestiones'].sum())  
            total_sms = int(df_resultado['total_sms'].sum())              
            
            compromisos = len(df_resultado[
                df_resultado['resultado_positivo'].isin(['Compromiso de pago', 'Compromiso de acuerdo de pago'])
            ])
            
            asesores_unicos = set()
            asesores_unicos.update(df_resultado['asesor_positivo'].unique())
            asesores_unicos.update(df_resultado['asesor_ultimo'].unique())
            asesores_unicos.discard('N/A')
            n_asesores = len(asesores_unicos)

            # Función para renderizar tarjetas KPI
            def render_kpi(icon, label, value, color="#ffffff", text_color="#000000"):
                st.markdown(f"""
                    <div style="padding: 1rem; margin-bottom: 1rem; background-color: {color}; 
                    border-radius: 10px; box-shadow: 1px 1px 6px rgba(0,0,0,0.1); text-align:center;">
                        <div style="font-size: 2rem;">{icon}</div>
                        <div style="font-size: 1.5rem; font-weight: bold; color:{text_color}">{value:,}</div>
                        <div style="color: #555;">{label}</div>
                    </div>
                """, unsafe_allow_html=True)

            # Mostrar tarjetas KPI
            col1, col2, col3 = st.columns(3)
            
            with col1:
                render_kpi("📄", "Total Registros", n_documentos, "#e8f5e9")
                render_kpi("📉", "Sin Gestión en Rango", docs_sin_gestion, "#e8f5e9")
            
            with col2:
                render_kpi("📞", "Cantidad de Gestiones", total_gestiones, "#e8f5e9")
                render_kpi("🤝", "Total Compromisos", compromisos, "#e8f5e9")
            
            with col3:
                render_kpi("✉️", "SMS Enviados", total_sms, "#e8f5e9")
                render_kpi("👥", "Asesores", n_asesores if n_asesores > 0 else "N/A", "#e8f5e9")

            st.subheader(f"📋 Resultados para base '{base_seleccionada}'")
            st.dataframe(
                df_resultado,
                use_container_width=True,
                column_config={
                    "fecha_entrega": st.column_config.DateColumn(format="DD/MM/YYYY"),
                    "fecha_ultima_gestion": st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm"),
                    "fecha_gestion_positiva": st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm"),
                    "total_gestiones": st.column_config.NumberColumn("Total Gestiones"),
                    "total_sms": st.column_config.NumberColumn("Total SMS")
                }
            )

            # Descargar Excel
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                df_resultado.to_excel(writer, index=False, sheet_name="resultados")
            
            st.download_button(
                label="📥 Descargar reporte completo",
                data=excel_buffer.getvalue(),
                file_name=f"reporte_completo_{base_seleccionada}_{fecha_seleccionada}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    conn.close()
# ==============================================================================
# FUNCIONES DE ANÁLISIS MEJORADAS
# ==============================================================================

def crear_grafico_metricas(df, titulo, color_scheme="viridis"):
    """Crear gráfico de métricas con diseño mejorado"""
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=("Evolución Temporal", "Distribución", "Tendencia", "Comparativo"),
        specs=[[{"secondary_y": True}, {"type": "pie"}],
               [{"colspan": 2}, None]],
        vertical_spacing=0.1
    )
    
    # Configurar cada subplot según los datos disponibles
    # (Aquí agregarías la lógica específica para cada gráfico)
    
    fig.update_layout(
        height=600,
        showlegend=True,
        title_text=titulo,
        title_x=0.5,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

def analisis_bases_mejorado(fecha_inicio, fecha_fin):
    """Análisis mejorado de bases de datos"""
    
    st.markdown("## 📈 Análisis de Bases de Datos")
    
    fecha_fin_ajustada = fecha_fin + timedelta(days=1)
    
    with st.spinner('🔄 Analizando datos de bases...'):
        try:
            with engine_simm.connect() as conn:
                # Consulta principal mejorada
                query_principal = text("""
                    SELECT
                        COUNT(DISTINCT base) as cantidad_bases,
                        COUNT(*) as total_registros,
                        COUNT(DISTINCT DATE(fecha_entrega)) as dias_con_bases,
                        AVG(CASE WHEN estado_telefono LIKE 'VALIDO:%' THEN 1.0 ELSE 0.0 END) * 100 as promedio_validez
                    FROM bases
                    WHERE fecha_entrega BETWEEN :fecha_inicio AND :fecha_fin
                """)
                
                query_detalle = text("""
                    SELECT
                        DATE(fecha_entrega) AS fecha,
                        base AS tipo_de_bd,
                        COUNT(*) AS cantidad_registros,
                        COUNT(CASE WHEN estado_telefono LIKE 'VALIDO:%' THEN 1 END) AS registros_validos,
                        COUNT(CASE WHEN estado_telefono LIKE 'AUSENTE:%' THEN 1 END) AS registros_sin_telefono,
                        COUNT(CASE WHEN estado_telefono LIKE 'ERRADO:%' THEN 1 END) AS registros_errados
                    FROM bases
                    WHERE fecha_entrega BETWEEN :fecha_inicio AND :fecha_fin
                    GROUP BY DATE(fecha_entrega), base
                    ORDER BY fecha DESC, tipo_de_bd
                """)
                
                params = {'fecha_inicio': fecha_inicio, 'fecha_fin': fecha_fin_ajustada}
                
                # Ejecutar consultas
                result_principal = conn.execute(query_principal, params)
                stats = result_principal.fetchone()
                
                result_detalle = conn.execute(query_detalle, params)
                df_bases = pd.DataFrame(result_detalle.fetchall(), columns=result_detalle.keys())
            
            if not df_bases.empty:
                # Procesamiento de datos
                df_bases['fecha'] = pd.to_datetime(df_bases['fecha'])
                df_bases['porcentaje_validos'] = (df_bases['registros_validos'] / df_bases['cantidad_registros'] * 100).round(2)
                
                # Métricas principales en cards
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric(
                        label="📚 Total de Bases",
                        value=f"{stats[0]:,}",
                        delta=f"{stats[2]} días activos"
                    )
                
                with col2:
                    st.metric(
                        label="📦 Registros Totales",
                        value=f"{stats[1]:,}",
                        delta=f"{stats[1]//stats[2]:,} prom/día" if stats[2] > 0 else "0"
                    )
                
                with col3:
                    total_validos = int(df_bases['registros_validos'].sum())
                    porcentaje_validos = round(stats[3], 2) if stats[3] else 0
                    st.metric(
                        label="✅ Registros Válidos",
                        value=f"{total_validos:,}",
                        delta=f"{porcentaje_validos}%"
                    )
                
                with col4:
                    total_problemas = int(df_bases['registros_sin_telefono'].sum() + df_bases['registros_errados'].sum())
                    porcentaje_problemas = round((total_problemas/stats[1]*100), 2) if stats[1] > 0 else 0
                    st.metric(
                        label="⚠️ Registros con Problemas",
                        value=f"{total_problemas:,}",
                        delta=f"{porcentaje_problemas}%"
                    )
                
                # Pestañas organizadas
                tab1, tab2, tab3, tab4 = st.tabs(["📊 Dashboard", "📈 Tendencias", "📋 Datos Detallados", "💾 Exportar"])
                
                with tab1:
                    # Dashboard principal con gráficos
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Gráfico de barras por tipo de base
                        fig_bases = px.bar(
                            df_bases.groupby('tipo_de_bd').agg({
                                'cantidad_registros': 'sum',
                                'registros_validos': 'sum'
                            }).reset_index(),
                            x='tipo_de_bd',
                            y=['cantidad_registros', 'registros_validos'],
                            title="Registros por Tipo de Base",
                            labels={'value': 'Cantidad', 'variable': 'Tipo'},
                            color_discrete_sequence=['#667eea', '#764ba2']
                        )
                        fig_bases.update_layout(height=400)
                        st.plotly_chart(fig_bases, use_container_width=True)
                    
                    with col2:
                        # Gráfico de dona para calidad
                        calidad_data = {
                            'Válidos': int(df_bases['registros_validos'].sum()),
                            'Sin Teléfono': int(df_bases['registros_sin_telefono'].sum()),
                            'Erróneos': int(df_bases['registros_errados'].sum())
                        }
                        
                        fig_calidad = px.pie(
                            values=list(calidad_data.values()),
                            names=list(calidad_data.keys()),
                            title="Calidad de Registros",
                            hole=0.4,
                            color_discrete_sequence=['#a8e6cf', '#ffaaa5', '#ff8b94']
                        )
                        fig_calidad.update_layout(height=400)
                        st.plotly_chart(fig_calidad, use_container_width=True)
                
                with tab2:
                    # Gráficos de tendencias
                    df_tendencia = df_bases.groupby('fecha').agg({
                        'cantidad_registros': 'sum',
                        'registros_validos': 'sum',
                        'porcentaje_validos': 'mean'
                    }).reset_index()
                    
                    # Gráfico de líneas múltiples
                    fig_tendencia = make_subplots(
                        rows=2, cols=1,
                        subplot_titles=("Volumen de Registros", "Porcentaje de Validez"),
                        vertical_spacing=0.1
                    )
                    
                    # Volumen
                    fig_tendencia.add_trace(
                        go.Scatter(x=df_tendencia['fecha'], y=df_tendencia['cantidad_registros'],
                                 mode='lines+markers', name='Total Registros'),
                        row=1, col=1
                    )
                    
                    fig_tendencia.add_trace(
                        go.Scatter(x=df_tendencia['fecha'], y=df_tendencia['registros_validos'],
                                 mode='lines+markers', name='Registros Válidos'),
                        row=1, col=1
                    )
                    
                    # Porcentaje
                    fig_tendencia.add_trace(
                        go.Scatter(x=df_tendencia['fecha'], y=df_tendencia['porcentaje_validos'],
                                 mode='lines+markers', name='% Validez', line=dict(color='orange')),
                        row=2, col=1
                    )
                    
                    fig_tendencia.update_layout(height=600, showlegend=True)
                    st.plotly_chart(fig_tendencia, use_container_width=True)
                
                with tab3:
                    # Tabla interactiva mejorada
                    st.subheader("📋 Detalle Completo por Fecha y Base")
                    
                    # Filtros adicionales para la tabla
                    col1, col2 = st.columns(2)
                    with col1:
                        bases_filtro = st.multiselect(
                            "Filtrar por base:",
                            options=df_bases['tipo_de_bd'].unique(),
                            default=df_bases['tipo_de_bd'].unique()
                        )
                    
                    with col2:
                        min_registros = st.number_input(
                            "Mínimo de registros:",
                            min_value=0,
                            value=0,
                            step=1
                        )
                    
                    # Aplicar filtros
                    df_filtrado = df_bases[
                        (df_bases['tipo_de_bd'].isin(bases_filtro)) &
                        (df_bases['cantidad_registros'] >= min_registros)
                    ]
                    
                    st.dataframe(
                        df_filtrado,
                        column_config={
                            "fecha": st.column_config.DateColumn("Fecha", format="DD/MM/YYYY"),
                            "tipo_de_bd": "Base de Datos",
                            "cantidad_registros": st.column_config.NumberColumn("Total", format="%d"),
                            "registros_validos": st.column_config.NumberColumn("Válidos", format="%d"),
                            "registros_sin_telefono": st.column_config.NumberColumn("Sin Tel.", format="%d"),
                            "registros_errados": st.column_config.NumberColumn("Erróneos", format="%d"),
                            "porcentaje_validos": st.column_config.NumberColumn("% Válidos", format="%.2f%%")
                        },
                        hide_index=True,
                        use_container_width=True,
                        height=500
                    )
                
                with tab4:
                    # Opciones de exportación mejoradas
                    st.subheader("💾 Opciones de Exportación")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("**📊 Reporte Detallado**")
                        csv_detallado = df_bases.to_csv(index=False, sep=';', encoding='utf-8-sig')
                        st.download_button(
                            "📥 Descargar CSV Detallado",
                            data=csv_detallado,
                            file_name=f"bases_detallado_{fecha_inicio}_{fecha_fin}.csv",
                            mime="text/csv"
                        )
                    
                    with col2:
                        st.markdown("**📈 Resumen Ejecutivo**")
                        df_resumen = df_bases.groupby('tipo_de_bd').agg({
                            'cantidad_registros': 'sum',
                            'registros_validos': 'sum',
                            'registros_sin_telefono': 'sum',
                            'registros_errados': 'sum'
                        }).reset_index()
                        df_resumen['porcentaje_validos'] = (df_resumen['registros_validos'] / df_resumen['cantidad_registros'] * 100).round(2)
                        
                        csv_resumen = df_resumen.to_csv(index=False, sep=';', encoding='utf-8-sig')
                        st.download_button(
                            "📥 Descargar Resumen CSV",
                            data=csv_resumen,
                            file_name=f"bases_resumen_{fecha_inicio}_{fecha_fin}.csv",
                            mime="text/csv"
                        )
            
            else:
                st.warning("⚠️ No se encontraron datos de bases en el período seleccionado")
                
        except Exception as e:
            st.error(f"❌ Error en análisis de bases: {str(e)}")

def analisis_gestiones_mejorado(fecha_inicio, fecha_fin):
    """Análisis mejorado de gestiones"""
    
    st.markdown("## 📋 Análisis de Gestiones")
    
    fecha_fin_ajustada = fecha_fin + timedelta(days=1)
    
    with st.spinner('🔄 Analizando datos de gestiones...'):
        try:
            # Consultas a ambas bases de datos
            params = {'fecha_inicio': fecha_inicio, 'fecha_fin': fecha_fin_ajustada}
            
            # Datos de SIMM
            with engine_simm.connect() as conn:
                query_gestiones = text("""
                    SELECT 
                        DATE(fecha_gestion_sencilla) AS fecha,
                        COUNT(DISTINCT id_gestion) AS gestiones_unicas,
                        COUNT(DISTINCT documento) AS documentos_unicos
                    FROM gestiones
                    WHERE fecha_gestion_sencilla BETWEEN :fecha_inicio AND :fecha_fin
                    GROUP BY DATE(fecha_gestion_sencilla)
                    ORDER BY fecha DESC
                """)
                
                result_gestiones = conn.execute(query_gestiones, params)
                df_gestiones = pd.DataFrame(result_gestiones.fetchall(), columns=result_gestiones.keys())
                
                query_sms = text("""
                    SELECT 
                        DATE(fecha_sms) AS fecha,
                        COUNT(*) AS sms_enviados
                    FROM sms
                    WHERE fecha_sms BETWEEN :fecha_inicio AND :fecha_fin
                    GROUP BY DATE(fecha_sms)
                    ORDER BY fecha DESC
                """)
                
                result_sms = conn.execute(query_sms, params)
                df_sms = pd.DataFrame(result_sms.fetchall(), columns=result_sms.keys())

                query_Whatsapp = text("""
                    SELECT 
                        DATE(fecha_gestion_sencilla) AS fecha,
                        COUNT(DISTINCT id_gestion) AS gestion_whatsapp
                    FROM gestiones
                    WHERE fecha_gestion_sencilla BETWEEN :fecha_inicio AND :fecha_fin
                    AND tipo_chat ILIKE '%whatsapp%'
                    GROUP BY DATE(fecha_gestion_sencilla)
                    ORDER BY fecha DESC
                """)
                
                result_gestion_whatsapp = conn.execute(query_Whatsapp, params)
                df_gestion_whatsaap = pd.DataFrame(result_gestion_whatsapp.fetchall(), columns=result_gestion_whatsapp.keys())

                query_Chat = text("""
                    SELECT 
                        DATE(fecha_gestion_sencilla) AS fecha,
                        COUNT(DISTINCT id_gestion) AS gestion_chat
                    FROM gestiones
                    WHERE fecha_gestion_sencilla BETWEEN :fecha_inicio AND :fecha_fin
                    AND tipo_chat ILIKE '%Chat alcaldia%'
                    GROUP BY DATE(fecha_gestion_sencilla)
                    ORDER BY fecha DESC
                """)
                
                result_gestion_chat = conn.execute(query_Chat, params)
                df_gestion_chat = pd.DataFrame(result_gestion_chat.fetchall(), columns=result_gestion_chat.keys())
            
            # Datos de Andes-Wolkvox (ahora en SIMM)
            with engine_simm.connect() as conn:
                query_campanas = text("""
                    SELECT 
                        DATE(date) AS fecha,
                        COUNT(telephone) AS contactos_predictivos
                    FROM tipificaciones_3
                    WHERE module = 'andes-movilidadtigo'
                    AND type_interaction = 'out_pre'
                    AND date BETWEEN :fecha_inicio AND :fecha_fin
                    GROUP BY DATE(date)
                    ORDER BY fecha DESC
                """)
                
                result_campanas = conn.execute(query_campanas, params)
                df_campanas = pd.DataFrame(result_campanas.fetchall(), columns=result_campanas.keys())
                
                # CORRECCIÓN: Nueva consulta en el mismo bloque de conexión
                query_no_conectadas = text("""
                    SELECT 
                        DATE(date) AS fecha,
                        COUNT(telephone) AS contactos_no_conectadas
                    FROM cdr_5
                    WHERE module = 'andes-movilidadtigo'
                    AND date BETWEEN :fecha_inicio AND :fecha_fin
                    GROUP BY DATE(date)
                    ORDER BY fecha DESC
                """)
                
                result_no_conectadas = conn.execute(query_no_conectadas, params)
                df_no_conectadas = pd.DataFrame(result_no_conectadas.fetchall(), columns=result_no_conectadas.keys())

                query_manual = text("""
                    SELECT 
                        DATE(date) AS fecha,
                        COUNT(telephone) AS contactos_manual
                    FROM tipificaciones_3
                    WHERE module = 'andes-movilidadtigo'
                    AND type_interaction = 'outbound_ma'               
                    AND date BETWEEN :fecha_inicio AND :fecha_fin
                    GROUP BY DATE(date)
                    ORDER BY fecha DESC
                """)
                
                result_manual = conn.execute(query_manual, params)
                df_manual = pd.DataFrame(result_manual.fetchall(), columns=result_manual.keys())
            
            # Consolidar datos
            df_final = pd.DataFrame()
            
            # CORRECCIÓN: Verificar si hay datos antes de procesar
            dataframes_disponibles = []
            
            if not df_gestiones.empty:
                dataframes_disponibles.append(df_gestiones)
            if not df_sms.empty:
                dataframes_disponibles.append(df_sms)
            if not df_campanas.empty:
                dataframes_disponibles.append(df_campanas)
            if not df_no_conectadas.empty:
                dataframes_disponibles.append(df_no_conectadas)
            if not df_manual.empty:
                dataframes_disponibles.append(df_manual)
            if not df_gestion_whatsaap.empty:
                dataframes_disponibles.append(df_gestion_whatsaap)
            if not df_gestion_chat.empty:
                dataframes_disponibles.append(df_gestion_chat)
            
            if dataframes_disponibles:
                # Comenzar con el primer DataFrame disponible
                df_final = dataframes_disponibles[0].copy()
                
                # Merge con el resto de DataFrames
                for df in dataframes_disponibles[1:]:
                    df_final = pd.merge(df_final, df, on='fecha', how='outer')
                
                # Limpiar datos
                df_final = df_final.fillna(0)
                numeric_cols = df_final.select_dtypes(include=[np.number]).columns
                df_final[numeric_cols] = df_final[numeric_cols].astype(int)
                df_final['fecha'] = pd.to_datetime(df_final['fecha'])
                df_final = df_final.sort_values('fecha', ascending=False)
                
                # CORRECCIÓN: Dashboard de métricas con 4 columnas
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    if 'gestiones_unicas' in df_final.columns:
                        total_gestiones = df_final['gestiones_unicas'].sum()
                        st.metric("📌 Gestiones Únicas", f"{total_gestiones:,}")
                    else:
                        st.metric("📌 Gestiones Únicas", "N/A")
                
                with col2:
                    if 'documentos_unicos' in df_final.columns:
                        total_documentos = df_final['documentos_unicos'].sum()
                        st.metric("📄 Documentos Únicos", f"{total_documentos:,}")
                    else:
                        st.metric("📄 Documentos Únicos", "N/A")
                
                with col3:
                    if 'sms_enviados' in df_final.columns:
                        total_sms = df_final['sms_enviados'].sum()
                        st.metric("📱 SMS Enviados", f"{total_sms:,}")
                    else:
                        st.metric("📱 SMS Enviados", "N/A")
                
                with col4:
                    if 'contactos_no_conectadas' in df_final.columns:
                        total_no_conectadas = df_final['contactos_no_conectadas'].sum()
                        st.metric("🚫 No Conectadas", f"{total_no_conectadas:,}")
                    else:
                        st.metric("🚫 No Conectadas", "N/A")
                
                # Agregar métrica de contactos predictivos si existe
                if 'contactos_predictivos' in df_final.columns:
                    col5, col6, col7, col8 = st.columns(4)

                    with col5:
                        total_predictivos = df_final['contactos_predictivos'].sum()
                        st.metric("🤖 Contactos Predictivos", f"{total_predictivos:,}")
                    with col6:
                        if 'contactos_manual' in df_final.columns:
                            total_manual = df_final['contactos_manual'].sum()
                            st.metric("✋Contactos Manual", f"{total_manual:,}")
                        else:
                            st.metric("✋ Contactos Manual", "N/A")
                    with col7:
                        if 'gestion_whatsapp' in df_final.columns:
                            total_whatsapp = df_final['gestion_whatsapp'].sum()
                            st.metric("🟢 Gestion WhatsApp", f"{total_whatsapp:,}")
                        else:
                            st.metric("🟢 Gestion WhatsApp", "N/A")
                    with col8:
                        if 'gestion_chat' in df_final.columns:
                            total_chat = df_final['gestion_chat'].sum()
                            st.metric("📲 Gestion Chat Alcaldia", f"{total_chat:,}")
                        else:
                            st.metric("📲 Gestion Chat Alcaldia", "N/A")
                
                # Gráfico principal
                columnas_disponibles = [col for col in df_final.columns if col != 'fecha']

                if columnas_disponibles:
                    # --- Widget para seleccionar columnas ---
                    columnas_seleccionadas = st.multiselect(
                        "Selecciona las métricas a visualizar:",
                        options=columnas_disponibles,
                        default=columnas_disponibles,  # Todas seleccionadas por defecto
                        key="filtro_columnas_grafico"  # Opcional: evita conflictos con otros widgets
                    )
                    
                    # --- Generar gráfico solo con las columnas seleccionadas ---
                    if columnas_seleccionadas:  # Verificar que al menos una columna esté seleccionada
                        fig = px.line(
                            df_final,
                            x='fecha',
                            y=columnas_seleccionadas,  # ¡Aquí usamos las columnas filtradas!
                            title="Evolución de Gestiones por Día",
                            labels={'value': 'Cantidad', 'fecha': 'Fecha'},
                            height=500
                        )
                        
                        fig.update_layout(
                            hovermode='x unified',
                            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("⚠️ Selecciona al menos una métrica para generar el gráfico.")
                else:
                    st.error("No hay datos disponibles para graficar.")
                # Tabla de datos
                st.subheader("📅 Detalle por Fecha")
                
                # Formatear columnas para mejor visualización
                column_config = {
                    "fecha": st.column_config.DateColumn("Fecha", format="DD/MM/YYYY"),
                }
                
                # Agregar configuración para columnas numéricas
                for col in numeric_cols:
                    if col in df_final.columns:
                        column_config[col] = st.column_config.NumberColumn(
                            col.replace('_', ' ').title(),
                            format="%d"
                        )
                
                st.dataframe(
                    df_final, 
                    use_container_width=True, 
                    height=400,
                    column_config=column_config,
                    hide_index=True
                )
                
                # Descarga
                csv = df_final.to_csv(index=False, sep=';', encoding='utf-8-sig')
                st.download_button(
                    "📥 Descargar Datos de Gestiones",
                    data=csv,
                    file_name=f"gestiones_{fecha_inicio}_{fecha_fin}.csv",
                    mime="text/csv"
                )
            
            else:
                st.warning("⚠️ No se encontraron datos de gestiones en el período seleccionado")
                
        except Exception as e:
            st.error(f"❌ Error en análisis de gestiones: {str(e)}")
            # Agregar información de debug
            st.error(f"Detalles del error: {type(e).__name__}")
            import traceback
            st.error(f"Traceback: {traceback.format_exc()}")

# ==============================================================================
# EJECUCIÓN PRINCIPAL
# ==============================================================================

# Verificar si se debe ejecutar el análisis
if hasattr(st.session_state, 'ejecutar_analisis') and st.session_state.ejecutar_analisis:
    
    # Mostrar información del período seleccionado
    st.info(f"📅 Analizando período: {fecha_inicio} hasta {fecha_fin}")
    
    # Ejecutar análisis según selección
    if tipo_analisis == "📈 Análisis de Bases":
        analisis_bases_mejorado(fecha_inicio, fecha_fin) 
    
    elif tipo_analisis == "📋 Análisis de Gestiones":
        analisis_gestiones_mejorado(fecha_inicio, fecha_fin)
    
    elif tipo_analisis == "🔄 Análisis Completo":
        st.markdown("# 🎯 Análisis Completo de Productividad")
        # Crear tabs para cada análisis, incluyendo Status de Bases
        tab1, tab2, tab3 = st.tabs(["📈 Bases de Datos", "📋 Gestiones", "📊 Status de Bases"])
        with tab1:
            analisis_bases_mejorado(fecha_inicio, fecha_fin)
        with tab2:
            analisis_gestiones_mejorado(fecha_inicio, fecha_fin)
        with tab3:
            mostrar_status_bases()
    # Limpiar el estado
    #st.session_state.ejecutar_analisis = False

else:
    # Pantalla de información cuando no hay análisis ejecutándose
    st.markdown("""
    <div style='text-align: center; padding: 30px;'>
        <h3> Configura los filtros y genera tu análisis</h3>
        <p style='font-size: 1.1em; color: #666;'>
            Selecciona el período, tipo de análisis y opciones, luego presiona "Generar Análisis"
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Información del sistema en formato más compacto
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **📈 Análisis de Bases**
        - Monitoreo de calidad y volumen
        - Registros válidos vs erróneos
        - Tendencias temporales
        """)
        
        st.markdown("""
        **📋 Análisis de Gestiones**
        - Seguimiento de gestiones
        - SMS enviados
        - Campañas predictivas
        """)
    
    with col2:
        st.markdown("""
        **🗃️ Fuentes de Datos**
        - Base SIMM: Gestiones y SMS
        - Base Andes-Wolkvox: Campañas
        """)
        
        st.markdown("""
        **✨ Características**
        - Gráficos interactivos
        - Exportación de datos
        - Filtros inteligentes
        """)
