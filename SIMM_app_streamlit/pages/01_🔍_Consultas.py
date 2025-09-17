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
from datetime import date
import os
import plotly.express as px

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
    st.markdown("---")
    # Conexión
    conn = DatabaseManager.get_connection('SIMM')

    # Entradas de fecha
    st.subheader("Conteo de id gestión únicos por rango de fechas")
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        hoy = date.today()
        default_ini = hoy.replace(day=1)
        default_fin = hoy

        fecha_rango = st.date_input(
            "Selecciona el rango de fechas a consultar",
            value=(default_ini, default_fin),
            min_value=hoy.replace(year=hoy.year-5),
            max_value=hoy,
            format="DD/MM/YYYY"
        )
        if isinstance(fecha_rango, tuple) and len(fecha_rango) == 2:
            fecha_inicio, fecha_fin = fecha_rango
        else:
            fecha_inicio = default_ini
            fecha_fin = default_fin
    
    
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
                COUNT(DISTINCT id_gestion) AS cantidad,
                ROUND(100.0 * COUNT(DISTINCT id_gestion) / SUM(COUNT(DISTINCT id_gestion)) OVER (), 2) AS porcentaje
            FROM gestiones
            WHERE fecha_gestion >= %s
            AND fecha_gestion <= %s
            GROUP BY tipo_llamada
            ORDER BY porcentaje DESC;
            """
            cur.execute(query_tipos, (fecha_inicio, fecha_fin))
            tipos_llamada = cur.fetchall()
            tipos_columns = [desc[0] for desc in cur.description]

            query_resultado = """
            SELECT 
                resultado,
                COUNT(DISTINCT id_gestion) AS cantidad,
                ROUND(100.0 * COUNT(DISTINCT id_gestion) / SUM(COUNT(DISTINCT id_gestion)) OVER (), 2) AS porcentaje
            FROM gestiones
            WHERE fecha_gestion >= %s
            AND fecha_gestion <= %s
            GROUP BY resultado
            ORDER BY porcentaje DESC;
            """
            cur.execute(query_resultado, (fecha_inicio, fecha_fin))
            resultados = cur.fetchall()
            tipos_columns2 = [desc[0] for desc in cur.description]

            query_asesor = """
            SELECT 
                asesor,
                COUNT(DISTINCT id_gestion) AS cantidad,
                ROUND(100.0 * COUNT(DISTINCT id_gestion) / SUM(COUNT(DISTINCT id_gestion)) OVER (), 2) AS porcentaje
            FROM gestiones
            WHERE fecha_gestion >= %s
            AND fecha_gestion <= %s
            GROUP BY asesor
            ORDER BY porcentaje DESC;
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
            df_tipos['cantidad'] = df_tipos['cantidad'].apply(lambda x: f"{x:,.0f}")
            df_tipos['cantidad'] = df_tipos['cantidad'].str.replace(",", ".")
            df_tipos['porcentaje'] = df_tipos['porcentaje'].apply(lambda x: f"{x:.2f}%")
            df_tipos.rename(columns={'tipo_llamada': 'Tipo Llamada', 'cantidad': 'Cantidad', 'porcentaje': 'Porcentaje'}, inplace=True)
            


            st.dataframe(df_tipos, use_container_width=True, hide_index=True)

        with col2:
            st.subheader("Distribución de Resultado")
            df_resultado['cantidad'] = df_resultado['cantidad'].apply(lambda x: f"{x:,.0f}")
            df_resultado['cantidad'] = df_resultado['cantidad'].str.replace(",", ".")
            df_resultado['porcentaje'] = df_resultado['porcentaje'].apply(lambda x: f"{x:.2f}%")
            df_resultado.rename(columns={'resultado': 'Resultado', 'cantidad': 'Cantidad', 'porcentaje': 'Porcentaje'}, inplace=True)

 

            st.dataframe(df_resultado, use_container_width=True, hide_index=True)

        with col3:
            st.subheader("Distribución de Asesores")
            df_asesores['cantidad'] = df_asesores['cantidad'].apply(lambda x: f"{x:,.0f}")
            df_asesores['cantidad'] = df_asesores['cantidad'].str.replace(",", ".")
            df_asesores['porcentaje'] = df_asesores['porcentaje'].apply(lambda x: f"{x:.2f}%")
            df_asesores.rename(columns={'asesor': 'Asesor', 'cantidad': 'Cantidad', 'porcentaje': 'Porcentaje'}, inplace=True)


            st.dataframe(df_asesores, use_container_width=True, hide_index=True)
            
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
    """Módulo para consultar la tabla de Pagos"""
    st.markdown("<h1 class='section-title'> Consulta a la tabla PAGOS </h1>", unsafe_allow_html=True)
    st.markdown("---")
    conn = DatabaseManager.get_connection('SIMM')

    st.subheader("Consulta de Pagos por rango de fechas")
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        hoy = date.today()
        default_ini = hoy.replace(day=1)
        default_fin = hoy

        fecha_rango = st.date_input(
            "Selecciona el rango de fechas a consultar",
            value=(default_ini, default_fin),
            min_value=hoy.replace(year=hoy.year-5),
            max_value=hoy,
            format="DD/MM/YYYY"
        )
        if isinstance(fecha_rango, tuple) and len(fecha_rango) == 2:
            fecha_inicio, fecha_fin = fecha_rango
        else:
            fecha_inicio = default_ini
            fecha_fin = default_fin
        
    if st.button("Consultar"):

        with conn.cursor() as cur:
            # Métricas principales
            query_total = """
            SELECT COUNT(*) AS conteo
            FROM pagos
            WHERE aplicacion_final = 'APLICA'
            AND fecha_sencilla >= %s
            AND fecha_sencilla <= %s;
            """
            cur.execute(query_total, (fecha_inicio, fecha_fin))
            total_registros = cur.fetchone()[0]

            # Métricas principales
            query_valor = """
            SELECT sum(valorpago) AS valor
            FROM pagos
            WHERE aplicacion_final = 'APLICA'
            AND fecha_sencilla >= %s
            AND fecha_sencilla <= %s;
            """
            cur.execute(query_valor, (fecha_inicio, fecha_fin))
            total_valor = cur.fetchone()[0]

            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown(f"<div class='metric-box'><div class='metric-title'>Cantidad de Pagos en el Período </div><div class='metric-value'>{total_registros:,}".replace(",", ".") + "</div></div>", unsafe_allow_html=True)
            with col2:
                st.markdown(f"<div class='metric-box'><div class='metric-title'>Valor de Pago Total</div><div class='metric-value'>{total_valor:,}".replace(",", ".") + "</div></div>", unsafe_allow_html=True)
            #with col3:
                #st.markdown(f"<div class='metric-box'><div class='metric-title'>Valor Entregado</div><div class='metric-value'>{valor_entregado:,}".replace(",", ".") + "</div></div>", unsafe_allow_html=True)

            # Métricas principales
            query_df = """
            WITH resumen AS (
                            SELECT 
                                infraccion as Nucleo,
                                COUNT(id_registro) AS cantidad,
                                ROUND(
                                    (COUNT(id_registro) * 100.0 / SUM(COUNT(id_registro)) OVER ()), 
                                    2
                                ) AS porcentaje
                            FROM pagos
                            WHERE aplicacion_final = 'APLICA'
                            AND fecha_sencilla >= %s
                            AND fecha_sencilla <= %s
                            GROUP BY Nucleo
                        ),
                        resumen_total AS (
                            SELECT 
                                'TOTAL' AS Nucleo,
                                SUM(cantidad) AS cantidad,
                                100.0 AS porcentaje
                            FROM resumen
                        )
                        SELECT * 
                        FROM (
                            SELECT * FROM resumen
                            UNION ALL
                            SELECT * FROM resumen_total
                        ) AS final
                        ORDER BY 
                            (Nucleo = 'TOTAL'),  -- mueve TOTAL al final
                            cantidad DESC;
            """
            cur.execute(query_df, (fecha_inicio, fecha_fin))
            nucleo = cur.fetchall()
            tipos_columns3 = [desc[0] for desc in cur.description]

            df_nucleo = pd.DataFrame(nucleo, columns=tipos_columns3)

            query_df = """
            WITH resumen AS (
                            SELECT 
                                infraccion as Nucleo,
                                sum(valorpago) AS Recaudo,
                                ROUND(
                                    (sum(valorpago) * 100.0 / SUM(sum(valorpago)) OVER ()), 
                                    2
                                ) AS porcentaje
                            FROM pagos
                            WHERE aplicacion_final = 'APLICA'
                            AND fecha_sencilla >= %s
                            AND fecha_sencilla <= %s
                            GROUP BY Nucleo
                        ),
                        resumen_total AS (
                            SELECT 
                                'TOTAL' AS Nucleo,
                                SUM(Recaudo) AS Recaudo,
                                100.0 AS porcentaje
                            FROM resumen
                        )
                        SELECT * 
                        FROM (
                            SELECT * FROM resumen
                            UNION ALL
                            SELECT * FROM resumen_total
                        ) AS final
                        ORDER BY 
                            (Nucleo = 'TOTAL'),  -- mueve TOTAL al final
                            Recaudo DESC;
            """
            cur.execute(query_df, (fecha_inicio, fecha_fin))
            recaudo_nucleo = cur.fetchall()
            tipos_columns4 = [desc[0] for desc in cur.description]

            df_recaudo_nucleo = pd.DataFrame(recaudo_nucleo, columns=tipos_columns4)
            # Formatear solo las columnas específicas con separador de miles
            if 'cantidad' in df_nucleo.columns:
                df_nucleo['cantidad'] = df_nucleo['cantidad'].apply(lambda x: f"{x:,.0f}")
                df_nucleo['cantidad'] = df_nucleo['cantidad'].str.replace(",", ".")
                df_nucleo['porcentaje'] = df_nucleo['porcentaje'].apply(lambda x: f"{x:.2f}%")
                df_nucleo.rename(columns={'nucleo': 'Núcleo', 'cantidad': 'Cantidad', 'porcentaje': 'Porcentaje'}, inplace=True)
            if 'recaudo' in df_recaudo_nucleo.columns:
                df_recaudo_nucleo['recaudo'] = df_recaudo_nucleo['recaudo'].apply(lambda x: f"{x:,.0f}")
                df_recaudo_nucleo['recaudo'] = df_recaudo_nucleo['recaudo'].str.replace(",", ".")
                df_recaudo_nucleo['porcentaje'] = df_recaudo_nucleo['porcentaje'].apply(lambda x: f"{x:.2f}%")
                df_recaudo_nucleo.rename(columns={'nucleo': 'Núcleo', 'recaudo': 'Recaudo', 'porcentaje': 'Porcentaje'}, inplace=True)

            col1, col2, col3 = st.columns(3)
            with col1:
                st.subheader("Cantidad de Pagos por Nucleo")
                st.dataframe(df_nucleo, use_container_width=True, hide_index=True)
            with col2:
                st.subheader("Recaudo por Nucleo")
                st.dataframe(df_recaudo_nucleo, use_container_width=True, hide_index=True)

            st.markdown("----")
            query_grafico1 = """
                SELECT cast(dia as integer), TO_CHAR(SUM(valorpago), 'FM$ 999,999,999,999') as recaudo from pagos
                    where aplicacion_final = 'APLICA'
                        and fecha_sencilla >= %s
                        and fecha_sencilla <= %s
                    group by dia
                    order by dia
                """
            cur.execute(query_grafico1, (fecha_inicio, fecha_fin))
            grafico = cur.fetchall()
            tipos_columns4 = [desc[0] for desc in cur.description]

            df_grafico_recaudo_dia = pd.DataFrame(grafico, columns=tipos_columns4)
            #st.dataframe(df_grafico_recaudo_dia, use_container_width=True, hide_index=True)
            df_grafico_recaudo_dia['recaudo'] = pd.to_numeric(df_grafico_recaudo_dia['recaudo'].str.replace('$','').str.replace('.','').str.replace(',',''), errors='coerce')

            
            fig = px.bar(df_grafico_recaudo_dia, x='dia', y='recaudo', 
                         title="Recaudo por Día", orientation='v', text='recaudo',
                         color='recaudo', color_continuous_scale='greens',
                         width=800, height=600)
            
            fig.update_traces(texttemplate='%{text:,.0f}', textposition='outside' )
            fig.update_layout(uniformtext_minsize=13)
            fig.update_traces(textfont_color='darkgreen')

            st.plotly_chart(fig, use_container_width=True)

#------------------------------------------------------------------
# Logica para consultar Bases
#------------------------------------------------------------------
def modulo_bases():
    """Módulo para consultar la tabla de BASES"""
    st.markdown("<h1 class='section-title'> Consulta a la tabla BASES </h1>", unsafe_allow_html=True)

    conn = DatabaseManager.get_connection('SIMM')

    st.subheader("Consulta de Bases Cartera por rango de fechas")
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        
        hoy = date.today()
        default_ini = hoy.replace(day=1)
        default_fin = hoy

        fecha_rango = st.date_input(
            "Selecciona el rango de fechas a consultar",
            value=(default_ini, default_fin),
            min_value=hoy.replace(year=hoy.year-5),
            max_value=hoy,
            format="DD/MM/YYYY"
        )
        if isinstance(fecha_rango, tuple) and len(fecha_rango) == 2:
            fecha_inicio, fecha_fin = fecha_rango
        else:
            fecha_inicio = default_ini
            fecha_fin = default_fin
    

    if st.button("Consultar"):
        with conn.cursor() as cur:
            # Métricas principales
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

            # --- Consulta de registros únicos por periodo actual ---
            query_actual = '''
            SELECT DISTINCT fecha_entrega, base, documento
            FROM bases
            WHERE fecha_entrega >= %s AND fecha_entrega <= %s
            '''
            cur.execute(query_actual, (fecha_inicio, fecha_fin))
            registros_actual = cur.fetchall()
            df_actual = pd.DataFrame(registros_actual, columns=["fecha_entrega", "base", "documento"])

            # --- Consulta de registros únicos por mes anterior ---
            fecha_inicio_ant = (fecha_inicio.replace(day=1) - pd.DateOffset(months=1)).date()
            fecha_fin_ant = (fecha_inicio.replace(day=1) - pd.DateOffset(days=1)).date()
            query_anterior = '''
            SELECT DISTINCT fecha_entrega, base, documento
            FROM bases
            WHERE fecha_entrega >= %s AND fecha_entrega <= %s
            '''
            cur.execute(query_anterior, (fecha_inicio_ant, fecha_fin_ant))
            registros_anterior = cur.fetchall()
            df_anterior = pd.DataFrame(registros_anterior, columns=["fecha_entrega", "base", "documento"])

            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown(f"<div class='metric-box'><div class='metric-title'>Registros en Bases</div><div class='metric-value'>{total_registros:,}".replace(",", ".") + "</div></div>", unsafe_allow_html=True)
            with col2:
                st.markdown(f"<div class='metric-box'><div class='metric-title'>Bases Recibidas</div><div class='metric-value'>{canti_bases:,}".replace(",", ".") + "</div></div>", unsafe_allow_html=True)
            with col3:
                st.markdown(f"<div class='metric-box'><div class='metric-title'>Valor Entregado</div><div class='metric-value'>{valor_entregado:,}".replace(",", ".") + "</div></div>", unsafe_allow_html=True)

            # --- COMPARACIÓN: Documentos que estaban en el mes anterior y no están en el actual ---
            docs_actual = set(df_actual['documento'])
            docs_anterior = set(df_anterior['documento'])
            docs_solo_anterior = docs_anterior - docs_actual
            df_solo_anterior = df_anterior[df_anterior['documento'].isin(docs_solo_anterior)]

            # Guardar DataFrames y métricas en session_state
            st.session_state['df_actual'] = df_actual
            st.session_state['df_anterior'] = df_anterior
            st.session_state['df_solo_anterior'] = df_solo_anterior
            st.session_state['total_registros'] = total_registros
            st.session_state['canti_bases'] = canti_bases
            st.session_state['valor_entregado'] = valor_entregado

    # Mostrar resultados si existen en session_state
    if 'df_actual' in st.session_state:
        df_actual = st.session_state['df_actual']
        df_anterior = st.session_state['df_anterior']
        df_solo_anterior = st.session_state['df_solo_anterior']
        total_registros = st.session_state['total_registros']
        canti_bases = st.session_state['canti_bases']
        valor_entregado = st.session_state['valor_entregado']

        st.subheader("Registros únicos por periodo")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("<b>Periodo Actual</b>", unsafe_allow_html=True)
            st.dataframe(df_actual, use_container_width=True)
        with col2:
            st.markdown("<b>Mes Anterior</b>", unsafe_allow_html=True)
            st.dataframe(df_anterior, use_container_width=True)

        st.subheader("Registros que estaban en el mes anterior y no están en el actual")
        st.dataframe(df_solo_anterior, use_container_width=True)

        

        # Botón de descarga de Excel
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_actual.to_excel(writer, index=False, sheet_name='Periodo_Actual')
            df_anterior.to_excel(writer, index=False, sheet_name='Mes_Anterior')
            df_solo_anterior.to_excel(writer, index=False, sheet_name='Faltantes_Actual')
        output.seek(0)
        st.download_button(
            label="Descargar informe en Excel",
            data=output,
            file_name="informe_bases.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )


def main():
    # Option menu horizontal en vez de sidebar
    from streamlit_option_menu import option_menu
    MODULOS = {
        "📊 Gestiones": "📊",
        "📱 SMS": "📱",
        "💰 Pagos": "💰",
        "📋 Bases": "📋"
    }
    modulo = option_menu(
        menu_title=None,
        options=list(MODULOS.keys()),
        icons=list(MODULOS.values()),
        orientation="horizontal"
    )
    st.markdown("---")


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
