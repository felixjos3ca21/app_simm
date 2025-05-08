# -*- coding: utf-8 -*-
import sys
from pathlib import Path
import streamlit as st
from io import BytesIO
import pandas as pd
from src.database.postgres import get_connection

sys.path.append(str(Path(__file__).parent))

# ==============================================
# CONFIGURACIÓN GENERAL
# ==============================================
st.set_page_config(page_title="Consultas de Gestiones", page_icon="🔍", layout="wide")

# Estilos CSS personalizados
st.markdown("""
    <style>
    .main-container {
        padding: 2rem;
    }
    .metric-card {
        padding: 1.5rem;
        border-radius: 10px;
        background: #ffffff;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
    }
    .stRadio [role=radiogroup]{
        gap: 0.5rem;
    }
    </style>
""", unsafe_allow_html=True)

# ==============================================
# FUNCIONES DE DATOS
# ==============================================
@st.cache_data
def get_total_records():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM gestiones")
    total = cursor.fetchone()[0]
    conn.close()
    return total

total_registros = get_total_records()

def ejecutar_cruce(df_input):
    """Cruza los pagos con gestiones anteriores manteniendo estructura original"""
    conn = get_connection()
    
    try:
        # 1. Preparar datos manteniendo estructura original
        df = df_input.copy()
        
        # Convertir fecha y validar
        df['fechapago'] = pd.to_datetime(df['fechapago'], dayfirst=True, errors='coerce')

        # Verificar fechas inválidas ANTES de convertir a date
        if df['fechapago'].isna().any():
            st.error("❌ Fechas de pago inválidas. Usar formato DD/MM/YYYY")
            return None

        # Ahora convertir a date (solo si pasó la validación)
        df['fechapago'] = df['fechapago'].dt.date

        # 2. Cruce por código cliente
        query_cod = """
            SELECT DISTINCT ON (identificador_infraccion)
                identificador_infraccion AS codcliente,
                fecha_gestion_sencilla AS fecha_gestion_cod,
                id_gestion AS id_gestion_cod,
                resultado AS resultado_cod,
                archivo_origen AS archivo_cod
            FROM gestiones
            WHERE identificador_infraccion = %s
            AND fecha_gestion_sencilla <= %s
            ORDER BY identificador_infraccion, fecha_gestion_sencilla DESC
        """
        
        # 3. Cruce por NIT
        query_nit = """
            SELECT DISTINCT ON (documento)
                documento AS nitcliente,
                fecha_gestion_sencilla AS fecha_gestion_nit,
                id_gestion AS id_gestion_nit,
                resultado AS resultado_nit,
                archivo_origen AS archivo_nit
            FROM gestiones
            WHERE documento = %s
            AND fecha_gestion_sencilla<= %s
            ORDER BY documento, fecha_gestion_sencilla DESC
        """

        # 4. Realizar cruces manteniendo índice original
        resultados = []
        with conn.cursor() as cursor:
            for idx, row in df.iterrows():
                # Cruce por código cliente
                cursor.execute(query_cod, (str(row['codcliente']), row['fechapago']))
                gestion_cod = cursor.fetchone()
                
                # Si no hay coincidencia, buscar por NIT
                if not gestion_cod:
                    cursor.execute(query_nit, (str(row['nitcliente']), row['fechapago']))
                    gestion_nit = cursor.fetchone()
                else:
                    gestion_nit = None
                
                # Construir registro resultante
                registro = row.to_dict()
                if gestion_cod:
                    registro.update({
                        'fecha_gestion_cod': gestion_cod[1],
                        'id_gestion_cod': gestion_cod[2],
                        'resultado_cod': gestion_cod[3],
                        'archivo_cod': gestion_cod[4]
                    })
                if gestion_nit:
                    registro.update({
                        'fecha_gestion_nit': gestion_nit[1],
                        'id_gestion_nit': gestion_nit[2],
                        'resultado_nit': gestion_nit[3],
                        'archivo_nit': gestion_nit[4]
                    })
                
                resultados.append(registro)

        # 5. Crear DataFrame final
        df_final = pd.DataFrame(resultados)
        
        # Ordenar columnas: originales + nuevas
        original_cols = df.columns.tolist()
        nuevas_cols = [
            'fecha_gestion_cod', 'id_gestion_cod', 'resultado_cod', 'archivo_cod',
            'fecha_gestion_nit', 'id_gestion_nit', 'resultado_nit', 'archivo_nit'
        ]
        
        return df_final[original_cols + nuevas_cols]

    except Exception as e:
        st.error(f"Error en el cruce: {str(e)}")
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

# ==============================================
# COMPONENTES REUTILIZABLES
# ==============================================
def mostrar_metricas(col, titulo, valor, ayuda=None):
    """Componente personalizado para métricas"""
    with col:
        st.markdown(
            f"<div class='metric-box'>"
            f"<h3 style='margin:0; font-size:1.1rem;'>{titulo}</h3>"
            f"<p style='margin:0; font-size:1.8rem; font-weight:bold;'>{valor}</p>"
            f"</div>",
            unsafe_allow_html=True
        )
        if ayuda:
            st.caption(ayuda)

def crear_seccion(titulo, nivel=3):
    """Componente para secciones estilizadas"""
    st.markdown(
        f"<div class='header-accent'><h{nivel}>{titulo}</h{nivel}></div>", 
        unsafe_allow_html=True
    )

# ==============================================
# VISTAS PRINCIPALES
# ==============================================
def mostrar_vista_resumen():
    st.header("📊 Resumen Base de Datos Gestiones")
    total_registros = get_total_records()
    
    # Sección de KPIs
    cols = st.columns(3)
    with cols[0]:
        st.markdown(f'<div class="metric-card">📈 Total Registros<br><h2>{total_registros:,}</h2></div>', unsafe_allow_html=True)
    
    with get_connection() as conn:
        avg_valor = pd.read_sql("SELECT AVG(valor) FROM gestiones", conn).iloc[0,0]
        with cols[1]:
            st.markdown(f'<div class="metric-card">💰 Valor Promedio<br><h2>${avg_valor:,.2f}</h2></div>', unsafe_allow_html=True)
        
        unique_comparendos = pd.read_sql(
            "SELECT COUNT(DISTINCT identificador_infraccion) FROM gestiones", 
            conn
        ).iloc[0,0]
        with cols[2]:
            st.markdown(f'<div class="metric-card">🆔 Comparendos Únicos<br><h2>{unique_comparendos:,}</h2></div>', unsafe_allow_html=True)
    
    # Gráficos
    with st.expander("📊 Distribución por Tipo de Llamada", expanded=True):
        with get_connection() as conn:
            df_tipos = pd.read_sql(
                "SELECT tipo_llamada, COUNT(*) as cantidad FROM gestiones GROUP BY tipo_llamada",
                conn
            )
            st.bar_chart(df_tipos.set_index('tipo_llamada'))

def mostrar_vista_datos_crudos():
    st.header("📋 Datos Crudos de la tabla gestiones")
    total_registros = get_total_records()
    
    page_size = 100
    page_number = st.number_input(
        "Número de Página", 
        min_value=1, 
        value=1,
        help="Cada página muestra 100 registros",
        key="pagination_raw"
    )
    
    offset = (page_number - 1) * page_size
    with get_connection() as conn:
        df_chunk = pd.read_sql(
            f"SELECT * FROM gestiones ORDER BY fecha_gestion DESC LIMIT {page_size} OFFSET {offset}",
            conn
        )
    
    st.dataframe(
        df_chunk,
        use_container_width=True,
        height=600,
        column_config={
            "fecha_gestion": st.column_config.DatetimeColumn(
                "Fecha Gestión",
                format="DD/MM/YYYY HH:mm"
            )
        }
    )
    st.caption(f"Mostrando registros {offset + 1} - {offset + len(df_chunk)} de {total_registros:,}")

def mostrar_vista_campanas():
    st.header("📌 Análisis por Archivo")
    
    with get_connection() as conn:
        page_size = 50
        page_number = st.number_input(
            "Página de Archivos",
            min_value=1,
            value=1,
            help="Navega entre grupos de 50 archivos",
            key="pagination_archivos"
        )
        
        offset = (page_number - 1) * page_size
        archivos = pd.read_sql(
            f"SELECT DISTINCT archivo_origen FROM gestiones ORDER BY archivo_origen DESC LIMIT {page_size} OFFSET {offset}",
            conn
        )['archivo_origen'].tolist()
        
        archivo_seleccionado = st.selectbox(
            "Selecciona archivo:",
            archivos,
            index=0,
            help="Selecciona un archivo origen para analizar"
        )
        
        df_archivo = pd.read_sql(
            "SELECT * FROM gestiones WHERE archivo_origen = %s ORDER BY fecha_gestion DESC LIMIT 1000",
            conn,
            params=(archivo_seleccionado,))
        
        st.subheader(f"🔍 Registros del archivo: {archivo_seleccionado}")
        st.dataframe(
            df_archivo,
            use_container_width=True,
            height=600,
            column_config={
                "fecha_gestion": st.column_config.DatetimeColumn(
                    "Fecha Gestión",
                    format="DD/MM/YYYY HH:mm"
                )
            }
        )
        
        # Estadísticas rápidas
        with st.expander("📊 Estadísticas del archivo", expanded=True):
            cols = st.columns(3)
            with cols[0]:
                st.metric("Total registros", len(df_archivo))
            with cols[1]:
                st.metric("Última gestión", df_archivo['fecha_gestion'].max().strftime("%d/%m/%Y"))
            with cols[2]:
                st.metric("Valor promedio", f"${df_archivo['valor'].mean():,.2f}")

        st.caption(f"Mostrando {len(df_archivo)} registros recientes del archivo {archivo_seleccionado}")

def mostrar_vista_cruce():
    st.header("🔀 Cruce de Datos Simplificado")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        uploaded_file = st.file_uploader("Subir Excel con pagos", type=["xlsx"])
        
        if uploaded_file:
            try:
                # Leer archivo SIN dayfirst
                df = pd.read_excel(uploaded_file)
                
                # Convertir fecha después de leer
                df['fechapago'] = pd.to_datetime(
                    df['fechapago'], 
                    dayfirst=True,  # Aquí sí es válido
                    errors='coerce'
                )
                
                required_cols = ['codcliente', 'nitcliente', 'fechapago']
                
                if all(col in df.columns for col in required_cols):
                    if st.button("Ejecutar cruce temporal"):
                        with st.spinner("Buscando gestiones anteriores..."):
                            resultado = ejecutar_cruce(df)
                            
                            if resultado is not None:
                                st.session_state.cruce_resultado = resultado
                                st.rerun()
                else:
                    st.error("Faltan columnas requeridas")

            except Exception as e:
                st.error(f"Error: {str(e)}")

    with col2:
        if "cruce_resultado" in st.session_state:
            df_resultado = st.session_state.cruce_resultado
            
            st.dataframe(
                df_resultado,
                use_container_width=True,
                height=600,
                column_config={
                    "fechapago": st.column_config.DateColumn(format="DD/MM/YYYY"),
                    "fecha_gestion": st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm")
                }
            )
            
            # Botón de descarga
            excel_buffer = BytesIO()
            df_resultado.to_excel(excel_buffer, index=False)
            st.download_button(
                label="Descargar resultados",
                data=excel_buffer.getvalue(),
                file_name="cruce_gestiones.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

# ==============================================
# BARRA LATERAL - NAVEGACIÓN
# ==============================================
def sidebar_navegacion():
    with st.sidebar:
        st.header("Tipos de consultas a la Base de Datos Gestiones")
        st.caption("Selecciona el módulo que deseas consultar")
        opcion = st.radio(
            "Seleccionar módulo:",
            options=[
                "Resumen Base de Datos Gestiones",
                "Datos Crudos de la tabla gestiones", 
                "Análisis por Archivo",
                "Cruce de Datos"
            ],
            label_visibility="collapsed"
        )
        
        st.divider()
        st.markdown("**Configuración:**")
        st.caption("Base de datos: PostgreSQL")
        st.caption(f"Última actualización: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}")

    return opcion

# ==============================================
# ESTRUCTURA PRINCIPAL
# ==============================================
def main():
    opcion_seleccionada = sidebar_navegacion()
    
    with st.container():
        if opcion_seleccionada == "Resumen Base de Datos Gestiones":
            mostrar_vista_resumen()
        elif opcion_seleccionada == "Datos Crudos de la tabla gestiones":
            mostrar_vista_datos_crudos()
        elif opcion_seleccionada == "Análisis por Archivo":
            mostrar_vista_campanas()
        elif opcion_seleccionada == "Cruce de Datos":
            mostrar_vista_cruce()

if __name__ == "__main__":
    main()