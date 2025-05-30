import streamlit as st  

st.set_page_config(
    page_title="SIAMM - Conulta Productividad",
    page_icon="src/utils/favicon-114x114.png",
    layout="wide",
    initial_sidebar_state="expanded"
)
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os
import plotly.express as px 
from decimal import Decimal
from sqlalchemy import text
import pandas as pd
from datetime import datetime
from sqlalchemy import inspect, text
import os
import tempfile
from src.utils.fondo import set_background
from contextlib import contextmanager
from datetime import timedelta, datetime
from functools import lru_cache

# ==============================================================================
# ESTILOS PERSONALIZADOS 
# ==============================================================================
st.markdown("""
    <style>
    /* Sidebar */
    [data-testid=stSidebar] {
        background-color: #A6DEB7 !important;
    }
    .main-container {
        padding: 2rem;
            
    /* Radio buttons */
    div[role=radiogroup] {
        gap: 0.5rem;
    }
    
    /* Títulos */
    .sidebar .sidebar-title {
        color: #2c3e50;
        font-size: 1.2rem;
        margin-bottom: 1rem;
        font-weight: 600;
    }
    
    /* Instrucciones */
    .sidebar-instructions {
        color: #4a5568;
        font-size: 0.9rem;
        line-height: 1.5;
    }
    
    /* Hover effects */
    div[role=radiogroup] label:hover {
        background-color: #e2e8f0 !important;
    }
    </style>
""", unsafe_allow_html=True)
st.image("src/utils/logo-andesbpo-359x143.png", width=150)
set_background("src/utils/bg-seccion.png")


# ============================================================================== #
# ⚙️ Configuración de conexiones
# ============================================================================== #

# Cargar variables de entorno
load_dotenv()

# Construcción de URLs para las bases de datos
def get_db_url(db_name):
    return f"postgresql://{os.getenv(f'DB_{db_name}_USER')}:{os.getenv(f'DB_{db_name}_PASSWORD')}" \
           f"@{os.getenv(f'DB_{db_name}_HOST')}:{os.getenv(f'DB_{db_name}_PORT')}/{os.getenv(f'DB_{db_name}_NAME')}"

# Crear motores de conexión
try:
    engine_simm = create_engine(get_db_url('SIMM'))
    engine_andes = create_engine(get_db_url('ANDES'))
except Exception as e:
    st.error(f"❌ Error creando motores de conexión: {e}")

# Context manager para conexiones
@contextmanager
def db_connection(engine):
    conn = None
    try:
        conn = engine.connect()
        yield conn
    except SQLAlchemyError as e:
        st.error(f"Error de base de datos: {e}")
        raise
    finally:
        if conn:
            conn.close()
# ============================================================================== #
# 📊 Consultas
# ============================================================================== #

# Función para convertir Decimal a float
def convert_decimals(data):
    if isinstance(data, Decimal):
        return float(data)
    elif isinstance(data, dict):
        return {k: convert_decimals(v) for k, v in data.items()}
    elif isinstance(data, (list, tuple)):
        return [convert_decimals(item) for item in data]
    return data

# Consulta con cache
@lru_cache(maxsize=32)
def execute_cached_query(engine, query):
    try:
        with db_connection(engine) as conn:
            result = conn.execute(text(query))
            return convert_decimals(result.fetchall())
    except Exception as e:
        st.error(f"Error en consulta: {e}")
        return None



# # Interfaz de usuario
# st.subheader("🔌 Verificación de conexión a bases de datos")

# if st.button("🔄 Probar conexiones"):
#     try:
#         with db_connection(engine_simm) as conn:
#             conn.execute(text("SELECT 1"))
#         st.success("✅ Conexión a SIMM exitosa")
#     except SQLAlchemyError as e:
#         st.error(f"❌ Error de conexión a SIMM: {e}")

#     try:
#         with db_connection(engine_andes) as conn:
#             conn.execute(text("SELECT 1"))
#         st.success("✅ Conexión a Andes-Wolkvox exitosa")
#     except SQLAlchemyError as e:
#         st.error(f"❌ Error de conexión a Andes-Wolkvox: {e}")

# # ============================================================================== #
# # 📊 Consulta Avanzada de Bases (Versión con Pestañas)
# # ============================================================================== #

# st.subheader("📈 Análisis de Bases por Fecha")

# # Opciones para el usuario
# col1, col2 = st.columns(2)
# with col1:
#     fecha_inicio_bases = st.date_input("Fecha inicio análisis", datetime.now().replace(day=1), key='fecha_inicio_bases')
# with col2:
#     fecha_fin_bases = st.date_input("Fecha fin análisis", datetime.now(), key='fecha_fin_bases')

# # Botón de consulta
# if st.button("📊 Consultar bases", type="primary"):
#     with st.spinner('Analizando datos...'):
#         try:
#             fecha_fin_ajustada = fecha_fin_bases + timedelta(days=1)
            
#             query = text("""
#                 SELECT
#                     DATE(fecha_entrega) AS fecha,
#                     base AS tipo_de_bd,
#                     COUNT(CASE WHEN telefono IS NOT NULL AND telefono != '' THEN 1 END) AS registros_con_telefono,
#                     COUNT(CASE WHEN telefono IS NULL OR telefono = '' THEN 1 END) AS registros_sin_telefono,
#                     COUNT(CASE WHEN telefono ~ '^[0-9]{10}$' = FALSE AND telefono IS NOT NULL AND telefono != '' THEN 1 END) AS registros_numero_errados,
#                     COUNT(*) AS cantidad_registros,
#                     ROUND(COUNT(CASE WHEN telefono IS NOT NULL AND telefono != '' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS porcentaje_avance
#                 FROM bases
#                 WHERE fecha_entrega BETWEEN :fecha_inicio AND :fecha_fin
#                 GROUP BY DATE(fecha_entrega), base
#                 ORDER BY fecha DESC, tipo_de_bd
#             """)
            
#             with engine_simm.connect() as conn:
#                 result = conn.execute(query, {
#                     'fecha_inicio': fecha_inicio_bases,
#                     'fecha_fin': fecha_fin_ajustada
#                 })
#                 df_bases = pd.DataFrame(result.fetchall(), columns=result.keys())
            
#             if not df_bases.empty:
#                 # Procesamiento de datos
#                 df_bases['fecha'] = pd.to_datetime(df_bases['fecha'])
#                 df_bases['% avance'] = df_bases['porcentaje_avance'].apply(lambda x: f"{x}%")
                
#                 # Cálculo de totales
#                 total_con_telefono = int(df_bases['registros_con_telefono'].sum())
#                 total_sin_telefono = int(df_bases['registros_sin_telefono'].sum())
#                 total_errados = int(df_bases['registros_numero_errados'].sum())
#                 total_registros = int(df_bases['cantidad_registros'].sum())
#                 porcentaje_total = round((total_con_telefono / total_registros * 100) if total_registros > 0 else 0, 2)
                
#                 # Mostrar métricas
#                 st.markdown("### 📊 Totales Generales")
#                 cols = st.columns(4)
#                 cols[0].metric("📱 Con teléfono", f"{total_con_telefono:,}", f"{porcentaje_total}%")
#                 cols[1].metric("🚫 Sin teléfono", f"{total_sin_telefono:,}", f"{round((total_sin_telefono/total_registros*100), 2)}%")
#                 cols[2].metric("⚠️ Errados", f"{total_errados:,}", f"{round((total_errados/total_registros*100), 2)}%")
#                 cols[3].metric("📦 Total registros", f"{total_registros:,}")
                
#                 # Crear pestañas para los gráficos
#                 tab1, tab2, tab3 = st.tabs(["📈 Evolución", "🧩 Distribución", "📊 Comparación"])
                
#                 with tab1:
#                     # Gráfico 1: Evolución del % de avance
#                     fig1 = px.line(
#                         df_bases,
#                         x='fecha',
#                         y='porcentaje_avance',
#                         color='tipo_de_bd',
#                         markers=True,
#                         title='Evolución del % de registros con teléfono válido',
#                         labels={'porcentaje_avance': '% Avance', 'fecha': 'Fecha', 'tipo_de_bd': 'Tipo de BD'}
#                     )
#                     fig1.update_layout(yaxis=dict(ticksuffix='%'))
#                     st.plotly_chart(fig1, use_container_width=True)
                
#                 with tab2:
#                     # Gráfico 2: Composición de registros
#                     fig2 = px.pie(
#                         df_bases,
#                         values='cantidad_registros',
#                         names='tipo_de_bd',
#                         title='Distribución de registros por tipo de base',
#                         hole=0.4
#                     )
#                     st.plotly_chart(fig2, use_container_width=True)
                
#                 with tab3:
#                     # Gráfico 3: Comparación teléfonos válidos/inválidos
#                     df_summary = df_bases.groupby('tipo_de_bd')[['registros_con_telefono', 'registros_sin_telefono', 'registros_numero_errados']].sum().reset_index()
#                     fig3 = px.bar(
#                         df_summary.melt(id_vars='tipo_de_bd', var_name='tipo', value_name='cantidad'),
#                         x='tipo_de_bd',
#                         y='cantidad',
#                         color='tipo',
#                         barmode='group',
#                         title='Comparación de registros por estado de teléfono',
#                         labels={'cantidad': 'Cantidad', 'tipo_de_bd': 'Tipo de BD', 'tipo': 'Estado'}
#                     )
#                     st.plotly_chart(fig3, use_container_width=True)
                
#                 # Tabla detallada
#                 st.markdown("### 📋 Detalle por Fecha y Base")
#                 st.dataframe(
#                     df_bases[['fecha', 'tipo_de_bd', 'registros_con_telefono', 
#                             'registros_sin_telefono', 'registros_numero_errados',
#                             'cantidad_registros', '% avance']],
#                     column_config={
#                         "fecha": st.column_config.DateColumn("Fecha", format="DD/MM/YYYY"),
#                         "tipo_de_bd": st.column_config.TextColumn("Tipo de BD"),
#                         "registros_con_telefono": st.column_config.NumberColumn(
#                             "Con Teléfono",
#                             format="%d"
#                         ),
#                         "registros_sin_telefono": st.column_config.NumberColumn(
#                             "Sin Teléfono",
#                             format="%d"
#                         ),
#                         "registros_numero_errados": st.column_config.NumberColumn(
#                             "Números Errados",
#                             format="%d"
#                         ),
#                         "cantidad_registros": st.column_config.NumberColumn(
#                             "Total Registros",
#                             format="%d"
#                         ),
#                         "% avance": st.column_config.TextColumn("% Avance")
#                     },
#                     hide_index=True,
#                     use_container_width=True
#                 )
                
#                 # Descarga de datos
#                 csv = df_bases.to_csv(index=False, sep=';', encoding='utf-8-sig').encode('utf-8-sig')
#                 st.download_button(
#                     "💾 Descargar reporte completo",
#                     data=csv,
#                     file_name=f"reporte_bases_{fecha_inicio_bases}_{fecha_fin_bases}.csv",
#                     mime="text/csv"
#                 )
                
#             else:
#                 st.warning("⚠️ No se encontraron bases en el rango de fechas seleccionado")
                
#         except Exception as e:
#             st.error(f"❌ Error en la consulta: {str(e)}")

# # ============================================================================== #
# # 📊 Consulta de Gestiones 
# # ============================================================================== #

# st.subheader("📋 Consulta de Gestiones por Fecha")

# # Opciones para el usuario
# col1, col2 = st.columns(2)
# with col1:
#     fecha_inicio = st.date_input("Fecha de inicio", datetime.now().replace(day=1))
# with col2:
#     fecha_fin = st.date_input("Fecha de fin", datetime.now())

# if st.button("📊 Consultar gestiones"):
#     try:
#         query = text("""
#             SELECT 
#                 DATE(fecha_gestion_sencilla) AS fecha,
#                 COUNT(DISTINCT id_gestion) AS gestiones_unicas,
#                 COUNT(DISTINCT documento) AS documentos_unicos
#             FROM gestiones
#             WHERE fecha_gestion_sencilla BETWEEN :fecha_inicio AND :fecha_fin
#             GROUP BY DATE(fecha_gestion_sencilla)
#             ORDER BY fecha DESC
#         """)
        
#         with engine_simm.connect() as conn:
#             result = conn.execute(query, {
#                 'fecha_inicio': fecha_inicio,
#                 'fecha_fin': fecha_fin + timedelta(days=1)  # Incluir todo el día final
#             })
#             df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
#         if not df.empty:
#             # Asegurar que la columna 'fecha' sea tipo datetime
#             df['fecha'] = pd.to_datetime(df['fecha'])
            
#             # Mostrar métricas resumen
#             total_gestiones = df['gestiones_unicas'].sum()
#             total_documentos = df['documentos_unicos'].sum()
            
#             col1, col2 = st.columns(2)
#             col1.metric("📌 Total de gestiones únicas", int(total_gestiones))
#             col2.metric("📄 Total de documentos únicos", int(total_documentos))
            
#             # Crear una copia para mostrar (con fechas formateadas como strings)
#             df_show = df.copy()
#             df_show['fecha'] = df_show['fecha'].dt.strftime('%Y-%m-%d')
            
#             # Mostrar tabla con los datos
#             st.dataframe(
#                 df_show,
#                 column_config={
#                     "fecha": "Fecha",
#                     "gestiones_unicas": st.column_config.NumberColumn(
#                         "Gestiones Únicas",
#                         format="%d"
#                     ),
#                     "documentos_unicos": st.column_config.NumberColumn(
#                         "Documentos Únicos",
#                         format="%d"
#                     )
#                 },
#                 hide_index=True,
#                 use_container_width=True
#             )
            
#             # Gráfico opcional
#             if st.checkbox("Mostrar gráfico de tendencia"):
#                 df_chart = df.set_index('fecha')
#                 st.line_chart(df_chart[['gestiones_unicas', 'documentos_unicos']])
            
#             # Opción para descargar los datos
#             csv = df_show.to_csv(index=False).encode('utf-8')
#             st.download_button(
#                 label="📥 Descargar datos como CSV",
#                 data=csv,
#                 file_name=f"gestiones_{fecha_inicio}_{fecha_fin}.csv",
#                 mime="text/csv"
#             )
#         else:
#             st.warning("⚠️ No se encontraron gestiones en el rango de fechas seleccionado")
            
#     except Exception as e:
#         st.error(f"❌ Error en la consulta: {str(e)}")

# ============================================================================== #
# 🗓️ Filtros Unificados
# ============================================================================== #

st.subheader("🛠️ Configuración de Reportes")

# Opciones para el usuario - Fechas compartidas
col1, col2 = st.columns(2)
with col1:
    fecha_inicio = st.date_input("Fecha de inicio", datetime.now().replace(day=1))
with col2:
    fecha_fin = st.date_input("Fecha de fin", datetime.now())

# Selector de reportes
reportes = st.multiselect(
    "Seleccione los reportes a generar",
    options=["Análisis de Bases", "Consulta de Gestiones"],
    default=["Análisis de Bases", "Consulta de Gestiones"]
)

# Botón principal de ejecución
if st.button("🚀 Generar Reportes", type="primary"):
    fecha_fin_ajustada = fecha_fin + timedelta(days=1)
    
    # ========================================================================== #
    # 📊 ANÁLISIS DE BASES
    # ========================================================================== #
    if "Análisis de Bases" in reportes:
        st.subheader("📈 Análisis de Bases por Fecha")
        
        with st.spinner('Analizando datos de bases...'):
            try:
                query_bases = text("""
                    SELECT
                        DATE(fecha_entrega) AS fecha,
                        base AS tipo_de_bd,
                        COUNT(CASE WHEN telefono IS NOT NULL AND telefono != '' THEN 1 END) AS registros_con_telefono,
                        COUNT(CASE WHEN telefono IS NULL OR telefono = '' THEN 1 END) AS registros_sin_telefono,
                        COUNT(CASE WHEN telefono ~ '^[0-9]{10}$' = FALSE AND telefono IS NOT NULL AND telefono != '' THEN 1 END) AS registros_numero_errados,
                        COUNT(*) AS cantidad_registros,
                        ROUND(COUNT(CASE WHEN telefono IS NOT NULL AND telefono != '' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS porcentaje_avance
                    FROM bases
                    WHERE fecha_entrega BETWEEN :fecha_inicio AND :fecha_fin
                    GROUP BY DATE(fecha_entrega), base
                    ORDER BY fecha DESC, tipo_de_bd
                """)
                
                with engine_simm.connect() as conn:
                    result = conn.execute(query_bases, {
                        'fecha_inicio': fecha_inicio,
                        'fecha_fin': fecha_fin_ajustada
                    })
                    df_bases = pd.DataFrame(result.fetchall(), columns=result.keys())
                
                if not df_bases.empty:
                    # Procesamiento y visualización de bases
                    df_bases['fecha'] = pd.to_datetime(df_bases['fecha'])
                    df_bases['% avance'] = df_bases['porcentaje_avance'].apply(lambda x: f"{x}%")
                    
                    # Cálculo de totales
                    total_con_telefono = int(df_bases['registros_con_telefono'].sum())
                    total_sin_telefono = int(df_bases['registros_sin_telefono'].sum())
                    total_errados = int(df_bases['registros_numero_errados'].sum())
                    total_registros = int(df_bases['cantidad_registros'].sum())
                    porcentaje_total = round((total_con_telefono / total_registros * 100) if total_registros > 0 else 0, 2)
                    
                    # Mostrar métricas
                    st.markdown("###  📊  Metricas del período")
                    cols = st.columns(4)
                    cols[0].metric("📱 Con teléfono", f"{total_con_telefono:,}", f"{porcentaje_total}%")
                    cols[1].metric("🚫 Sin teléfono", f"{total_sin_telefono:,}", f"{round((total_sin_telefono/total_registros*100), 2)}%")
                    cols[2].metric("⚠️ Errados", f"{total_errados:,}", f"{round((total_errados/total_registros*100), 2)}%")
                    cols[3].metric("📦 Total registros", f"{total_registros:,}")
                    
                    st.markdown("### 📈 Gráficos")
                    # Crear pestañas para los gráficos
                    tab1, tab2, tab3 = st.tabs(["📈 Evolución", "🧩 Distribución", "📊 Comparación"])
                    
                    with tab1:
                        # Gráfico 1: Evolución del % de avance
                        fig1 = px.line(
                            df_bases,
                            x='fecha',
                            y='porcentaje_avance',
                            color='tipo_de_bd',
                            markers=True,
                            title='Evolución del % de registros con teléfono válido',
                            labels={'porcentaje_avance': '% Avance', 'fecha': 'Fecha', 'tipo_de_bd': 'Tipo de BD'}
                        )
                        fig1.update_layout(yaxis=dict(ticksuffix='%'))
                        st.plotly_chart(fig1, use_container_width=True)
                    
                    with tab2:
                        # Gráfico 2: Composición de registros
                        fig2 = px.pie(
                            df_bases,
                            values='cantidad_registros',
                            names='tipo_de_bd',
                            title='Distribución de registros por tipo de base',
                            hole=0.4
                        )
                        st.plotly_chart(fig2, use_container_width=True)
                    
                    with tab3:
                        # Gráfico 3: Comparación teléfonos válidos/inválidos
                        df_summary = df_bases.groupby('tipo_de_bd')[['registros_con_telefono', 'registros_sin_telefono', 'registros_numero_errados']].sum().reset_index()
                        fig3 = px.bar(
                            df_summary.melt(id_vars='tipo_de_bd', var_name='tipo', value_name='cantidad'),
                            x='tipo_de_bd',
                            y='cantidad',
                            color='tipo',
                            barmode='group',
                            title='Comparación de registros por estado de teléfono',
                            labels={'cantidad': 'Cantidad', 'tipo_de_bd': 'Tipo de BD', 'tipo': 'Estado'}
                        )
                        st.plotly_chart(fig3, use_container_width=True)
                    
                    # Tabla detallada
                    st.markdown("### 📋 Detalle por Fecha y Base")
                    st.dataframe(
                        df_bases[['fecha', 'tipo_de_bd', 'registros_con_telefono', 
                                'registros_sin_telefono', 'registros_numero_errados',
                                'cantidad_registros', '% avance']],
                        column_config={
                            "fecha": st.column_config.DateColumn("Fecha", format="DD/MM/YYYY"),
                            "tipo_de_bd": st.column_config.TextColumn("Tipo de BD"),
                            "registros_con_telefono": st.column_config.NumberColumn(
                                "Con Teléfono",
                                format="%d"
                            ),
                            "registros_sin_telefono": st.column_config.NumberColumn(
                                "Sin Teléfono",
                                format="%d"
                            ),
                            "registros_numero_errados": st.column_config.NumberColumn(
                                "Números Errados",
                                format="%d"
                            ),
                            "cantidad_registros": st.column_config.NumberColumn(
                                "Total Registros",
                                format="%d"
                            ),
                            "% avance": st.column_config.TextColumn("% Avance")
                        },
                        hide_index=True,
                        use_container_width=True
                    )
                    
                    # Descarga de datos
                    csv = df_bases.to_csv(index=False, sep=';', encoding='utf-8-sig').encode('utf-8-sig')
                    st.download_button(
                        "💾 Descargar reporte completo",
                        data=csv,
                        file_name=f"reporte_bases_{fecha_inicio}_{fecha_fin}.csv",
                        mime="text/csv"
                    )
                    
                else:
                    st.warning("⚠️ No se encontraron bases en el rango de fechas seleccionado")
                    
            except Exception as e:
                st.error(f"❌ Error en consulta de bases: {str(e)}")
    
    # ============================================================================== #
    # 📋 CONSULTA DE GESTIONES AMPLIADA (CON ORIGENES CORRECTOS)
    # ============================================================================== #
    if "Consulta de Gestiones" in reportes:
        st.subheader("📋 Análisis de Gestiones por Fecha")
        
        with st.spinner('Analizando datos de gestiones...'):
            try:
                # Consultas a SIMM
                with engine_simm.connect() as conn:
                    # Consulta principal de gestiones (SIMM)
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
                    result_gestiones = conn.execute(query_gestiones, {
                        'fecha_inicio': fecha_inicio,
                        'fecha_fin': fecha_fin_ajustada
                    })
                    df_gestiones = pd.DataFrame(result_gestiones.fetchall(), columns=result_gestiones.keys())
                    
                    # Consulta de SMS enviados (SIMM)
                    query_sms = text("""
                        SELECT 
                            DATE(fecha_sms) AS fecha,
                            COUNT(*) AS sms_enviados
                        FROM sms
                        WHERE fecha_sms BETWEEN :fecha_inicio AND :fecha_fin
                        GROUP BY DATE(fecha_sms)
                        ORDER BY fecha DESC
                    """)
                    result_sms = conn.execute(query_sms, {
                        'fecha_inicio': fecha_inicio,
                        'fecha_fin': fecha_fin_ajustada
                    })
                    df_sms = pd.DataFrame(result_sms.fetchall(), columns=result_sms.keys())
                
                # Consultas a Andes-Wolkvox
                with engine_andes.connect() as conn:
                    # Consulta de campañas predictivas (Andes-Wolkvox)
                    query_campanas = text("""
                        SELECT 
                            DATE(date) AS fecha,
                            COUNT(DISTINCT telephone) AS contactos_predictivos
                        FROM campanas_3
                        WHERE module = 'andes-movilidadtigo'
                        AND date BETWEEN :fecha_inicio AND :fecha_fin
                        GROUP BY DATE(date)
                        ORDER BY fecha DESC
                    """)
                    result_campanas = conn.execute(query_campanas, {
                        'fecha_inicio': fecha_inicio,
                        'fecha_fin': fecha_fin_ajustada
                    })
                    df_campanas = pd.DataFrame(result_campanas.fetchall(), columns=result_campanas.keys())
                    
                    # Consulta de llamadas no conectadas (Andes-Wolkvox)
                    query_cdr = text("""
                        SELECT 
                            DATE(date) AS fecha,
                            COUNT(DISTINCT telephone) AS llamadas_no_conectadas
                        FROM cdr_5
                        WHERE module = 'andes-movilidadtigo'
                        AND date BETWEEN :fecha_inicio AND :fecha_fin
                        GROUP BY DATE(date)
                        ORDER BY fecha DESC
                    """)
                    result_cdr = conn.execute(query_cdr, {
                        'fecha_inicio': fecha_inicio,
                        'fecha_fin': fecha_fin_ajustada
                    })
                    df_cdr = pd.DataFrame(result_cdr.fetchall(), columns=result_cdr.keys())
                    
                    # Consulta de resultados predictivos (Andes-Wolkvox)
                    query_resultados = text("""
                        SELECT 
                            DATE(fechafinreg) AS fecha,
                            SUM(llamadas) AS llamadas_predictivas
                        FROM resultados_predictivo
                        WHERE fechafinreg BETWEEN :fecha_inicio AND :fecha_fin
                        GROUP BY DATE(fechafinreg)
                        ORDER BY fecha DESC
                    """)
                    result_resultados = conn.execute(query_resultados, {
                        'fecha_inicio': fecha_inicio,
                        'fecha_fin': fecha_fin_ajustada
                    })
                    df_resultados = pd.DataFrame(result_resultados.fetchall(), columns=result_resultados.keys())
                
                # Unificar todos los DataFrames
                dfs = [df_gestiones, df_sms, df_campanas, df_cdr, df_resultados]
                df_final = dfs[0].copy()
                
                for df in dfs[1:]:
                    if not df.empty:
                        df_final = pd.merge(df_final, df, on='fecha', how='left')
                
                # Rellenar NaN con 0 y convertir a enteros
                if not df_final.empty:
                    numeric_cols = df_final.columns.difference(['fecha'])
                    df_final[numeric_cols] = df_final[numeric_cols].fillna(0).astype(int)
                    
                    # Convertir fecha a datetime
                    df_final['fecha'] = pd.to_datetime(df_final['fecha'])
                    
                    # Mostrar métricas resumen
                    st.markdown("### 📈 Metricas del período")
                    cols = st.columns(5)
                    cols[0].metric("📌 Gestiones únicas", f"{df_final['gestiones_unicas'].sum():,}")
                    cols[1].metric("📄 Documentos únicos", f"{df_final['documentos_unicos'].sum():,}")
                    cols[2].metric("📱 SMS enviados", f"{df_final['sms_enviados'].sum():,}")
                    cols[3].metric("🤖 Contactos predictivos", f"{df_final['contactos_predictivos'].sum():,}")
                    cols[4].metric("📞 Llamadas predictivas", f"{df_final['llamadas_predictivas'].sum():,}")
                    
                    st.markdown("### 📊 Gráfico")
                    # Gráfico de tendencias
                    fig = px.line(
                        df_final,
                        x='fecha',
                        y=['gestiones_unicas', 'documentos_unicos', 'sms_enviados', 
                        'contactos_predictivos', 'llamadas_predictivas'],
                        markers=True,
                        title='Tendencias de Contacto por Día',
                        labels={'value': 'Cantidad', 'fecha': 'Fecha', 'variable': 'Métrica'},
                        color_discrete_sequence=px.colors.qualitative.Plotly
                    )
                    fig.update_layout(
                        yaxis_title='Cantidad',
                        legend_title='Métrica',
                        hovermode='x unified'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Tabla detallada
                    st.markdown("### 📋 Detalle por Fecha")
                    df_show = df_final.copy()
                    df_show['fecha'] = df_show['fecha'].dt.strftime('%Y-%m-%d')
                    
                    st.dataframe(
                        df_show,
                        column_config={
                            "fecha": st.column_config.TextColumn("Fecha"),
                            "gestiones_unicas": st.column_config.NumberColumn("Gestiones Únicas", format="%d"),
                            "documentos_unicos": st.column_config.NumberColumn("Documentos Únicos", format="%d"),
                            "sms_enviados": st.column_config.NumberColumn("SMS Enviados", format="%d"),
                            "contactos_predictivos": st.column_config.NumberColumn("Contactos Predictivos", format="%d"),
                            "llamadas_no_conectadas": st.column_config.NumberColumn("Llamadas No Conectadas", format="%d"),
                            "llamadas_predictivas": st.column_config.NumberColumn("Llamadas Predictivas", format="%d")
                        },
                        hide_index=True,
                        use_container_width=True
                    )
                    
                    # Opción para descargar los datos
                    csv = df_show.to_csv(index=False, sep=';').encode('utf-8')
                    st.download_button(
                        label="📥 Descargar datos como CSV",
                        data=csv,
                        file_name=f"gestiones_completas_{fecha_inicio}_{fecha_fin}.csv",
                        mime="text/csv"
                    )
                else:
                    st.warning("⚠️ No se encontraron datos en el rango de fechas seleccionado")
                    
            except Exception as e:
                st.error(f"❌ Error en consulta de gestiones: {str(e)}")