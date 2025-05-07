import sys
from pathlib import Path
import streamlit as st
from io import BytesIO
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

# # Lógica para cada vista
# if consulta_mode == "Resumen Estadístico":
#     conn = get_connection()
    
#     st.header("KPIs Principales")
#     col1, col2, col3 = st.columns(3)
#     col1.metric("Total Registros", total_registros)
    
#     avg_valor = pd.read_sql("SELECT AVG(valor) FROM gestiones", conn).iloc[0,0]
#     col2.metric("Valor Promedio", f"${avg_valor:,.2f}")
    
#     unique_comparendos = pd.read_sql(
#         "SELECT COUNT(DISTINCT identificador_infraccion) FROM gestiones", 
#         conn
#     ).iloc[0,0]
#     col3.metric("Comparendos Únicos", unique_comparendos)
    
#     st.subheader("Distribución por Tipo de Llamada")
#     df_tipos = pd.read_sql(
#         "SELECT tipo_llamada, COUNT(*) as cantidad FROM gestiones GROUP BY tipo_llamada",
#         conn
#     )
#     st.bar_chart(df_tipos.set_index('tipo_llamada'))
    
#     conn.close()

# elif consulta_mode == "Datos Crudos":
#     page_size = 100
#     page_number = st.number_input("Página", min_value=1, value=1)
    
#     offset = (page_number - 1) * page_size
#     query = f"""
#         SELECT * 
#         FROM gestiones
#         ORDER BY fecha_gestion DESC
#         LIMIT {page_size} OFFSET {offset}
#     """
    
#     conn = get_connection()
#     df_chunk = pd.read_sql(query, conn)
#     conn.close()
    
#     st.dataframe(df_chunk)
#     st.write(f"Mostrando registros {offset + 1} - {offset + len(df_chunk)} de {total_registros}")

# elif consulta_mode == "Análisis por Campaña":
#     conn = get_connection()
    
#     page_size_campanas = 50
#     page_campana = st.number_input("Página de Campañas", min_value=1, value=1)
    
#     offset_campana = (page_campana - 1) * page_size_campanas
#     campanas = pd.read_sql(
#         f"SELECT DISTINCT campana FROM gestiones LIMIT {page_size_campanas} OFFSET {offset_campana}",
#         conn
#     )['campana'].tolist()
    
#     campana = st.selectbox("Selecciona Campaña:", campanas)
    
#     df_campana = pd.read_sql(
#         "SELECT * FROM gestiones WHERE campana = %s LIMIT 1000",
#         conn,
#         params=(campana,)
#     )
    
#     st.dataframe(df_campana)
#     conn.close()

    # ==============================================
# NUEVA SECCIÓN DE CRUCE EN SIDEBAR
# ==============================================

def ejecutar_cruce(df_input):
    """Ejecuta cruces separados y devuelve resultados + métricas"""
    conn = get_connection()
    
    try:
        # 1. Inicialización ---------------------------------------------------
        resultados = {
            'por_codcliente': pd.DataFrame(),
            'por_nitcliente': pd.DataFrame(),
            'metricas': {}
        }
        
        total_registros = len(df_input)
        resultados['metricas']['total_procesados'] = total_registros
        
        # 2. Cruce por codcliente ---------------------------------------------
        codigos = df_input['codcliente'].astype(str).unique().tolist()
        
        if codigos:
            query_cod = """
                SELECT DISTINCT ON (identificador_infraccion)
                    identificador_infraccion AS codcliente,
                    fecha_gestion AS fecha_gestion_cod,
                    id_gestion AS id_gestion_cod,
                    resultado AS resultado_cod,
                    archivo_origen AS archivo_cod
                FROM gestiones
                WHERE identificador_infraccion = ANY(%s)
                ORDER BY identificador_infraccion, fecha_gestion DESC
            """
            df_cod = pd.read_sql(query_cod, conn, params=(codigos,))
            
            resultados['por_codcliente'] = pd.merge(
                left=df_input,
                right=df_cod,
                on='codcliente',
                how='left'
            )
            coincidencias_cod = resultados['por_codcliente']['id_gestion_cod'].notna().sum()
            resultados['metricas']['coincidencias_cod'] = coincidencias_cod
            
        # 3. Cruce por nitcliente ---------------------------------------------
        nits = df_input['nitcliente'].astype(str).unique().tolist()
        
        if nits:
            query_nit = """
                SELECT DISTINCT ON (documento)
                    documento AS nitcliente,
                    fecha_gestion AS fecha_gestion_nit,
                    id_gestion AS id_gestion_nit,
                    resultado AS resultado_nit,
                    archivo_origen AS archivo_nit
                FROM gestiones
                WHERE documento = ANY(%s)
                ORDER BY documento, fecha_gestion DESC
            """
            df_nit = pd.read_sql(query_nit, conn, params=(nits,))
            
            resultados['por_nitcliente'] = pd.merge(
                left=df_input,
                right=df_nit,
                on='nitcliente',
                how='left'
            )
            coincidencias_nit = resultados['por_nitcliente']['id_gestion_nit'].notna().sum()
            resultados['metricas']['coincidencias_nit'] = coincidencias_nit
            
        # 4. Cálculo de métricas finales --------------------------------------
        resultados['metricas']['sin_coincidencia'] = total_registros - (
            resultados['metricas'].get('coincidencias_cod', 0) +
            resultados['metricas'].get('coincidencias_nit', 0)
        )
        
        return resultados

    except Exception as e:
        st.error(f"🚨 Error crítico durante el cruce: {str(e)}")
        return None
    finally:
        conn.close()

def generar_reporte_metricas(metricas):
    """Crea DataFrame con estadísticas para Excel"""
    return pd.DataFrame({
        'Métrica': [
            'Registros procesados',
            'Coincidencias por codcliente',
            'Coincidencias por nitcliente',
            'Sin coincidencias'
        ],
        'Valor': [
            metricas['total_procesados'],
            metricas.get('coincidencias_cod', 0),
            metricas.get('coincidencias_nit', 0),
            metricas['sin_coincidencia']
        ]
    })

def descargar_excel(dfs_dict):
    """Genera archivo Excel para descarga"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in dfs_dict.items():
            df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    return output.getvalue()

st.sidebar.header("🔀 Cruce de Bases")
uploaded_file = st.sidebar.file_uploader(
    "Subir archivo para cruce (Excel)",
    type=["xlsx"],
    help="El archivo debe contener las columnas: codcliente, Tipo de documento, nitcliente, numobligacion, fechapago, valorpago"
)

if uploaded_file:
    # Validar y procesar archivo
    try:
        df_cruce = pd.read_excel(uploaded_file)
        
        # Validación de columnas
        required_columns = ['codcliente', 'Tipo de documento', 'nitcliente', 
                          'numobligacion', 'fechapago', 'valorpago']
        df_cruce['codcliente'] = df_cruce['codcliente'].astype(str)
        if not all(col in df_cruce.columns for col in required_columns):
            missing = [col for col in required_columns if col not in df_cruce.columns]
            st.sidebar.error(f"❌ Faltan columnas requeridas: {', '.join(missing)}")
        else:
            st.sidebar.success("✔️ Archivo validado correctamente")
            
            # Mostrar previsualización
            with st.expander("Vista previa archivo subido"):
                st.dataframe(df_cruce.head(3))
                
            # Botón para ejecutar cruce
            if st.button("🚀 Ejecutar cruce ahora"):
                with st.spinner("🔍 Buscando coincidencias en 3.5M+ registros..."):
                    resultados = ejecutar_cruce(df_cruce)
                    
                    if resultados:
                        # Almacenar en sesión para descarga posterior
                        st.session_state.resultados_cruce = resultados
                        
                        # Mostrar reporte visual
                        st.subheader("📊 Reporte de Ejecución")
                        
                        col1, col2, col3, col4 = st.columns(4)
                        col1.metric("Total procesados", resultados['metricas']['total_procesados'])
                        col2.metric("Coincidencias codcliente", resultados['metricas'].get('coincidencias_cod', 0))
                        col3.metric("Coincidencias nitcliente", resultados['metricas'].get('coincidencias_nit', 0))
                        col4.metric("Sin coincidencias", resultados['metricas']['sin_coincidencia'])
                        
                        # Vista previa avanzada
                        with st.expander("🔎 Detalle de coincidencias por codcliente"):
                            st.dataframe(resultados['por_codcliente'].head(10))
                        
                        with st.expander("🔎 Detalle de coincidencias por nitcliente"):
                            st.dataframe(resultados['por_nitcliente'].head(10))

        # Descarga separada ----------------------------------------------------------
        if 'resultados_cruce' in st.session_state:
            st.divider()
            st.subheader("📤 Exportación de Resultados")
            
            if st.button("💾 Generar archivo Excel completo"):
                with st.spinner("⏳ Construyendo archivo de 3 hojas..."):
                    # Crear hoja de métricas
                    df_metricas = generar_reporte_metricas(st.session_state.resultados_cruce['metricas'])
                    
                    # Construir diccionario para Excel
                    excel_data = {
                        'METRICAS': df_metricas,
                        'POR_CODCLIENTE': st.session_state.resultados_cruce['por_codcliente'],
                        'POR_NITCLIENTE': st.session_state.resultados_cruce['por_nitcliente']
                    }
                    
                    # Generar y descargar
                    excel_file = descargar_excel(excel_data)
                    
                    st.download_button(
                        label="⬇️ Descargar Reporte Completo",
                        data=excel_file,
                        file_name=f"CRUCE_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                
    except Exception as e:
        st.sidebar.error(f"Error al leer archivo: {str(e)}")
