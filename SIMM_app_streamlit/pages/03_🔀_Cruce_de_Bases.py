# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
from src.database.postgres import get_connection
from io import BytesIO
import openpyxl
import sys
from pathlib import Path
import streamlit as st
from io import BytesIO
import pandas as pd
from src.database.postgres import get_connection
from src.utils.fondo import set_background


sys.path.append(str(Path(__file__).parent))

# Configuración de la página
st.set_page_config(
    page_title="SIAMM - Cruces de Bases",
    page_icon="src/utils/favicon-114x114.png",
    layout="wide"
)

st.markdown("""
    <style>
    /* Sidebar */
    [data-testid=stSidebar] {
        background-color: #a5d6a7 !important;
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
# Logo y fondo
st.image("src/utils/logo-andesbpo-359x143.png", width=150)
set_background("src/utils/bg-seccion.png")

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
def mostrar_vista_cruce():
    st.header("🔀 Cruce de Datos Pagos Vs Gestiones")
    
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
                    if st.button("Ejecutar cruce de datos"):
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
        st.header("Tipos de Cruces a la Base de Datos")
        st.write("Selecciona el tipo de cruce que deseas realizar")
        opcion = st.radio(
            "Seleccionar módulo:",
            options=[
                "Cruce de Datos Pagos Vs. Gestiones",
            ],
            label_visibility="collapsed"
        )
        
        st.divider()
        st.markdown("**Configuración:**")
        st.write("Base de datos: PostgreSQL")
        st.write(f"Última actualización: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}")

    return opcion
# ==============================================
# ESTRUCTURA PRINCIPAL
# ==============================================
def main():
    opcion_seleccionada = sidebar_navegacion()
    
    with st.container():
        if opcion_seleccionada == "Cruce de Datos Pagos Vs. Gestiones":
            mostrar_vista_cruce()


if __name__ == "__main__":
    main()
