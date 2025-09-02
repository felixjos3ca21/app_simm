# -*- coding: utf-8 -*-
from ast import main
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
from io import BytesIO
from assets.fondo import set_background
from src.database.postgres import DatabaseManager
import pathlib
import os

# Configuración de la página
st.set_page_config(
    page_title="SIAMM - Consultas",
    page_icon="assets/images/favicon-114x114.png",
    layout="wide"
)

# Cargar CSS global si existe
css_path = pathlib.Path("assets/css/global.css")
if css_path.exists():
    with open(css_path, encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# Logo y fondo
st.image("assets/images/logo-andesbpo-359x143.png", width=310)
set_background("assets/images/bg-seccion.png")

#------------------------------------------------------------------
# Logica para consultar a gestiones
#------------------------------------------------------------------

def modulo_gestiones():
    """Módulo para consultar la tabla de gestiones"""
    # Título
    st.markdown("<h1 class='section-title'> Consulta a la tabla Gestiones </h1>", unsafe_allow_html=True)

    # Conexión
    conn = DatabaseManager.get_connection('SIMM')

    # Entradas de fecha
    st.subheader("Conteo de id gestión únicos por rango de fechas")
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        fecha_inicio = st.date_input("Fecha inicio", value=pd.to_datetime("2025-08-01").date())
    with col4:
        fecha_fin = st.date_input("Fecha fin", value=pd.to_datetime("2025-08-30").date())
    
    
    if st.button("Consultar"):
        with conn.cursor() as cur:
            # 1 Conteo exacto de id_gestion únicos por rango
            query_unicos = """
            SELECT COUNT(DISTINCT id_gestion) AS conteo
            FROM gestiones
            WHERE fecha_gestion >= %s
            AND fecha_gestion <= %s;
            """
            cur.execute(query_unicos, (fecha_inicio, fecha_fin))
            total_unicos = cur.fetchone()[0]

            # 2 Conteo aproximado de filas totales en la tabla
            query_filas_aprox = """
            SELECT reltuples::bigint AS filas_aprox
            FROM pg_class
            WHERE relname = 'gestiones';
            """
            cur.execute(query_filas_aprox)
            filas_aprox = cur.fetchone()[0]

            # 3 Conteo exacto de documentos únicos por rango
            query_docs = """
            SELECT COUNT(DISTINCT documento) AS conteo
            FROM gestiones
            WHERE fecha_gestion >= %s
            AND fecha_gestion <= %s;
            """
            cur.execute(query_docs, (fecha_inicio, fecha_fin))
            documentos_unicos = cur.fetchone()[0]

            # 4 Conteo por tipo_llamada con porcentaje
            query_tipos = """
            SELECT 
                tipo_llamada,
                COUNT(*) AS cantidad,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS porcentaje
            FROM gestiones
            WHERE fecha_gestion >= %s
            AND fecha_gestion <= %s
            GROUP BY tipo_llamada
            ORDER BY cantidad DESC;
            """
            cur.execute(query_tipos, (fecha_inicio, fecha_fin))
            tipos_llamada = cur.fetchall()
            tipos_columns = [desc[0] for desc in cur.description]

            query_resultado = """
            SELECT 
                resultado,
                COUNT(*) AS cantidad,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS porcentaje
            FROM gestiones
            WHERE fecha_gestion >= '2025-08-01'
            GROUP BY resultado
            ORDER BY cantidad DESC;
            """
            cur.execute(query_resultado, (fecha_inicio, fecha_fin))
            resultados = cur.fetchall()
            tipos_columns2 = [desc[0] for desc in cur.description]

            query_asesor = """
            SELECT 
                asesor,
                COUNT(*) AS cantidad,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS porcentaje
            FROM gestiones
            WHERE fecha_gestion >= '2025-08-01'
            GROUP BY asesor
            ORDER BY cantidad DESC;
            """
            cur.execute(query_asesor, (fecha_inicio, fecha_fin))
            asesores = cur.fetchall()
            tipos_columns3 = [desc[0] for desc in cur.description]

            df_tipos = pd.DataFrame(tipos_llamada, columns=tipos_columns)
            df_resultado = pd.DataFrame(resultados, columns=tipos_columns2)
            df_asesores = pd.DataFrame(asesores, columns=tipos_columns3)

        # Mostrar métricas con tu CSS
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(f"<div class='metric-box'><div class='metric-title'>Registros aproximados en Gestiones</div><div class='metric-value'>{filas_aprox:,}".replace(",", ".") + "</div></div>", unsafe_allow_html=True)
        with col2:
            st.markdown(f"<div class='metric-box'><div class='metric-title'>ID de gestión únicos</div><div class='metric-value'>{total_unicos:,}".replace(",", ".") + "</div></div>", unsafe_allow_html=True)
        with col3:
            st.markdown(f"<div class='metric-box'><div class='metric-title'>Documentos únicos</div><div class='metric-value'>{documentos_unicos:,}".replace(",", ".") + "</div></div>", unsafe_allow_html=True)

        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.subheader("Distribución de Tipo Llamada")
            st.dataframe(df_tipos, use_container_width=True)
        with col2:
            st.subheader("Distribución de Resultado")
            st.dataframe(df_resultado, use_container_width=True)
        with col3:
            st.subheader("Distribución de Asesores")
            st.dataframe(df_asesores, use_container_width=True)
#------------------------------------------------------------------
# Logica para consultar SMS
#------------------------------------------------------------------
def modulo_sms():
    """Módulo para consultar la tabla de SMS"""
    # Título
    st.markdown("<h1 class='section-title'> Consulta a la tabla SMS </h1>", unsafe_allow_html=True)

    # Conexión
    conn = DatabaseManager.get_connection('SIMM')

    # Entradas de fecha
    st.subheader("Conteo de sms por rango de fechas")
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        fecha_inicio = st.date_input("Fecha inicio", value=pd.to_datetime("2025-08-01").date())
    with col4:
        fecha_fin = st.date_input("Fecha fin", value=pd.to_datetime("2025-08-30").date())


    if st.button("Consultar"):
            with conn.cursor() as cur:
                # 1 Conteo exacto de id_gestion únicos por rango
                query_unicos = """
                SELECT COUNT(id_registro) AS conteo
                FROM sms
                WHERE fecha_sms >= %s
                AND fecha_sms <= %s;
                """
                cur.execute(query_unicos, (fecha_inicio, fecha_fin))
                total_registros = cur.fetchone()[0]

                query_docs = """
                SELECT COUNT(DISTINCT documento) AS conteo
                FROM sms
                WHERE fecha_sms >= %s
                AND fecha_sms <= %s;
                """
                cur.execute(query_docs, (fecha_inicio, fecha_fin))
                documentos_unicos = cur.fetchone()[0]

                query_base = """
                SELECT 
                    base,
                    COUNT(*) AS cantidad,
                    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS porcentaje
                FROM sms
                WHERE fecha_sms >= %s
                AND fecha_sms <= %s
                GROUP BY base
                ORDER BY cantidad DESC;
                """
                cur.execute(query_base, (fecha_inicio, fecha_fin))
                tipos_base = cur.fetchall()
                tipos_columns = [desc[0] for desc in cur.description]

                query_resultado = """
                SELECT 
                    resultado,
                    COUNT(*) AS cantidad,
                    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS porcentaje
                FROM sms
                WHERE fecha_sms >= %s
                AND fecha_sms <= %s
                GROUP BY resultado
                ORDER BY cantidad DESC;
                """
                cur.execute(query_resultado, (fecha_inicio, fecha_fin))
                resultados_sms = cur.fetchall()
                tipos_columns2 = [desc[0] for desc in cur.description]

                

            # Mostrar métricas con tu CSS
            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown(f"<div class='metric-box'><div class='metric-title'>Registros en SMS</div><div class='metric-value'>{total_registros:,}".replace(",", ".") + "</div></div>", unsafe_allow_html=True)
            with col2:
                 st.markdown(f"<div class='metric-box'><div class='metric-title'>Documentos únicos</div><div class='metric-value'>{documentos_unicos:,}".replace(",", ".") + "</div></div>", unsafe_allow_html=True)

            df_bases= pd.DataFrame(tipos_base, columns=tipos_columns)
            df_resultado = pd.DataFrame(resultados_sms, columns=tipos_columns2)
            #df_asesores = pd.DataFrame(asesores, columns=tipos_columns3)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.subheader("Distribución de Base")
                st.dataframe(df_bases, use_container_width=True)
            with col2:
                st.subheader("Distribución de Resultado")
                st.dataframe(df_resultado, use_container_width=True)
            # with col3:
            #     st.subheader("Distribución de Asesores")
                




#------------------------------------------------------------------
# Logica para consultar a PAGOS
#------------------------------------------------------------------
def modulo_pagos():
    # Lógica para Pagos
    st.write("Aquí va la lógica de Pagos")



#------------------------------------------------------------------
# Logica para consultar Bases
#------------------------------------------------------------------
def modulo_bases():

    """Módulo para consultar la tabla de BASES"""
    # Título
    st.markdown("<h1 class='section-title'> Consulta a la tabla BASES </h1>", unsafe_allow_html=True)

    # Conexión
    conn = DatabaseManager.get_connection('SIMM')

    # Entradas de fecha
    st.subheader("Conteo de sms por rango de fechas")
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        fecha_inicio = st.date_input("Fecha inicio", value=pd.to_datetime("2025-08-01").date())
    with col4:
        fecha_fin = st.date_input("Fecha fin", value=pd.to_datetime("2025-08-30").date())


    if st.button("Consultar"):
            with conn.cursor() as cur:
                # 1 Conteo exacto de id_gestion únicos por rango
                query_total = """
                SELECT COUNT(*) AS conteo
                FROM bases
                WHERE fecha_entrega >= %s
                AND fecha_entrega <= %s;
                """
                cur.execute(query_total, (fecha_inicio, fecha_fin))
                total_registros = cur.fetchone()[0]

                query_bases = """
                SELECT COUNT(*) AS conteo
                FROM (
                    SELECT DISTINCT base, fecha_entrega
                    FROM bases
                    WHERE fecha_entrega >= %s
                    AND fecha_entrega <= %s
                ) t;
                """
                cur.execute(query_bases, (fecha_inicio, fecha_fin))
                canti_bases = cur.fetchone()[0]


                query_valor = """
                SELECT SUM(valor_infraccion) AS conteo
                FROM bases
                WHERE fecha_entrega >= %s
                AND fecha_entrega <= %s;
                """
                cur.execute(query_valor, (fecha_inicio, fecha_fin))
                valor_entregado = cur.fetchone()[0]


                # query_base = """
                # SELECT 
                #     base,
                #     COUNT(*) AS cantidad,
                #     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS porcentaje
                # FROM sms
                # WHERE fecha_sms >= %s
                # AND fecha_sms <= %s
                # GROUP BY base
                # ORDER BY cantidad DESC;
                # """
                # cur.execute(query_base, (fecha_inicio, fecha_fin))
                # tipos_base = cur.fetchall()
                # tipos_columns = [desc[0] for desc in cur.description]

                # query_resultado = """
                # SELECT 
                #     resultado,
                #     COUNT(*) AS cantidad,
                #     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS porcentaje
                # FROM sms
                # WHERE fecha_sms >= %s
                # AND fecha_sms <= %s
                # GROUP BY resultado
                # ORDER BY cantidad DESC;
                # """
                # cur.execute(query_resultado, (fecha_inicio, fecha_fin))
                # resultados_sms = cur.fetchall()
                # tipos_columns2 = [desc[0] for desc in cur.description]

                

            # Mostrar métricas con tu CSS
            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown(f"<div class='metric-box'><div class='metric-title'>Registros en Bases</div><div class='metric-value'>{total_registros:,}".replace(",", ".") + "</div></div>", unsafe_allow_html=True)
            with col2:
                 st.markdown(f"<div class='metric-box'><div class='metric-title'>Bases Recibidas</div><div class='metric-value'>{canti_bases:,}".replace(",", ".") + "</div></div>", unsafe_allow_html=True)
            with col3:
                 st.markdown(f"<div class='metric-box'><div class='metric-title'>Valor Entregado</div><div class='metric-value'>{valor_entregado:,}".replace(",", ".") + "</div></div>", unsafe_allow_html=True)    

            # df_bases= pd.DataFrame(tipos_base, columns=tipos_columns)
            # df_resultado = pd.DataFrame(resultados_sms, columns=tipos_columns2)
            # #df_asesores = pd.DataFrame(asesores, columns=tipos_columns3)
            
            # col1, col2, col3 = st.columns(3)
            # with col1:
            #     st.subheader("Distribución de Base")
            #     st.dataframe(df_bases, use_container_width=True)
            # with col2:
            #     st.subheader("Distribución de Resultado")
            #     st.dataframe(df_resultado, use_container_width=True)
            # # with col3:
            # #     st.subheader("Distribución de Asesores")
                


def main():
    # Sidebar
    with st.sidebar:
        st.header("Módulos")
        modulo = st.selectbox(
            "Seleccione módulo:",
            options=["📊 Gestiones", "📱 SMS", "💰 Pagos", "📋 Bases"],
            index=0
        )

    if modulo == "📊 Gestiones":
        modulo_gestiones()
    elif modulo == "📱 SMS":
        modulo_sms()
    elif modulo == "💰 Pagos":
        modulo_pagos()
    elif modulo == "📋 Bases":
        modulo_bases()

if __name__ == "__main__":
    main()
