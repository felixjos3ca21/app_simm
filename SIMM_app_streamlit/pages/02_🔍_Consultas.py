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

# Funciones primero
def ejecutar_cruce(df_input):
    """Ejecuta el cruce con la base de datos"""
    conn = get_connection()
    
    try:
        # Convertir a string y obtener códigos únicos
        codigos = df_input['codcliente'].astype(str).unique().tolist()
        
        if not codigos:
            st.warning("⚠️ No hay códigos para buscar")
            return None

        query = """
            SELECT 
                identificador_infraccion AS codcliente,
                fecha_gestion,
                id_gestion,
                resultado,
                archivo_origen
            FROM gestiones
            WHERE identificador_infraccion = ANY(%s)
        """
        
        df_db = pd.read_sql(query, conn, params=(codigos,))
        
        # Hacer merge evitando duplicados
        if not df_db.empty:
            df_db = df_db.drop_duplicates(subset=['codcliente'])
        
        df_resultado = pd.merge(
            left=df_input,
            right=df_db,
            how='left',
            on='codcliente',
            suffixes=('', '_bd')
        )
        
        return df_resultado
        
    except Exception as e:
        st.error(f"Error en cruce: {str(e)}")
        return None
    finally:
        conn.close()

def descargar_excel(df):
    """Genera archivo Excel para descarga"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='ResultadoCruce')
    
    processed_data = output.getvalue()
    return processed_data

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
            if st.button("Ejecutar cruce"):
               
                # Dentro del if st.button("Ejecutar cruce"):
                with st.spinner("Buscando coincidencias en la base de datos..."):
                    df_resultado = ejecutar_cruce(df_cruce)
                    
                    if df_resultado is not None:
                        coincidencias = len(df_resultado.dropna(subset=['fecha_gestion']))
                        total = len(df_resultado)
                        
                        st.success(f"""
                        ✅ Cruce completado:
                        - Registros con coincidencia: {coincidencias}
                        - Registros sin coincidencia: {total - coincidencias}
                        """)
                        
                        # Mostrar resultados
                        st.dataframe(df_resultado.head(50))
                        
                        # Preparar descarga
                        excel_file = descargar_excel(df_resultado)
                        st.download_button(
                            label="📥 Descargar resultado",
                            data=excel_file,
                            file_name="resultado_cruce.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                
    except Exception as e:
        st.sidebar.error(f"Error al leer archivo: {str(e)}")
