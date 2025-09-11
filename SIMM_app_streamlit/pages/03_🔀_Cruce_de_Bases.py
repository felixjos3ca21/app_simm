# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
from src.database.postgres import get_connection
from io import BytesIO
import openpyxl
import sys
from streamlit_option_menu import option_menu
from pathlib import Path
import streamlit as st
from io import BytesIO
import pandas as pd
from src.database.postgres import get_connection
from assets.fondo import set_background
import pathlib

sys.path.append(str(Path(__file__).parent))

# ==============================================================================
# CONFIGURACIÓN INICIAL
# ==============================================================================

# Configuración de página
st.set_page_config(
    page_title="SIAMM - Cruce de Datos",
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

# ==============================================
# Cruce de pagos con gestiones y cartera
# ==============================================

def ejecutar_cruce(df_input, fecha_inicio, fecha_fin):
    """
    Cruza los pagos con gestiones y cartera.
    """
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

        from datetime import date
        fecha_minima = date(2025, 8, 1)
        df = df[df['fechapago'] >= fecha_minima]

        # 2. Cruce por código cliente (dentro del rango de fechas)
        query_cod = """
            SELECT DISTINCT ON (identificador_infraccion)
                identificador_infraccion AS codcliente,
                fecha_gestion_sencilla AS fecha_gestion_cod,
                id_gestion AS id_gestion_cod,
                resultado AS resultado_cod,
                archivo_origen AS archivo_cod,
                tipo_llamada AS tipollamada_cod,
                tipo_chat AS tipo_chat_cod
            FROM gestiones
            WHERE identificador_infraccion = %s
            AND fecha_gestion_sencilla BETWEEN %s AND %s
            ORDER BY identificador_infraccion, fecha_gestion_sencilla DESC
        """

        # 3. Cruce por NIT (dentro del rango de fechas)
        query_nit = """
            SELECT DISTINCT ON (documento)
                documento AS nitcliente,
                fecha_gestion_sencilla AS fecha_gestion_nit,
                id_gestion AS id_gestion_nit,
                resultado AS resultado_nit,
                archivo_origen AS archivo_nit,
                tipo_llamada AS tipollamada_nit,
                tipo_chat AS tipo_chat_nit
            FROM gestiones
            WHERE documento = %s
            AND fecha_gestion_sencilla BETWEEN %s AND %s
            ORDER BY documento, fecha_gestion_sencilla DESC
        """


        # 4. Obtener nits únicos y codclientes únicos para consultar en SMS en el rango
        nits_unicos = df['nitcliente'].dropna().astype(str).unique().tolist()
        cods_unicos = df['codcliente'].dropna().astype(str).unique().tolist()

        # Buscar por documento (nitcliente)
        query_sms_nit = """
            SELECT DISTINCT documento FROM sms
            WHERE documento = ANY(%s) AND fecha_sms BETWEEN %s AND %s
        """
        # Buscar por numero_infraccion (codcliente)
        query_sms_cod = """
            SELECT DISTINCT numero_comparendo FROM sms
            WHERE numero_comparendo = ANY(%s) AND fecha_sms BETWEEN %s AND %s
        """
        with conn.cursor() as cursor:
            cursor.execute(query_sms_nit, (nits_unicos, fecha_inicio, fecha_fin))
            nits_sms = set(row[0] for row in cursor.fetchall())
            cursor.execute(query_sms_cod, (cods_unicos, fecha_inicio, fecha_fin))
            cods_sms = set(row[0] for row in cursor.fetchall())

        # 5. Realizar cruces manteniendo índice original
        resultados = []
        with conn.cursor() as cursor:
            for idx, row in df.iterrows():
                # Cruce por código cliente
                cursor.execute(query_cod, (str(row['codcliente']), fecha_inicio, fecha_fin))
                gestion_cod = cursor.fetchone()

                # Si no hay coincidencia, buscar por NIT
                if not gestion_cod:
                    cursor.execute(query_nit, (str(row['nitcliente']), fecha_inicio, fecha_fin))
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
                        'archivo_cod': gestion_cod[4],
                        'tipollamada_cod': gestion_cod[5],
                        'tipo_chat_cod': gestion_cod[6]
                    })
                if gestion_nit:
                    registro.update({
                        'fecha_gestion_nit': gestion_nit[1],
                        'id_gestion_nit': gestion_nit[2],
                        'resultado_nit': gestion_nit[3],
                        'archivo_nit': gestion_nit[4],
                        'tipollamada_nit': gestion_nit[5],
                        'tipo_chat_nit': gestion_nit[6]
                    })

                # Agregar columna booleana si el nitcliente está en SMS
                registro['sms_enviado'] = str(row['nitcliente']) in nits_sms
                # Agregar columna booleana si el codcliente está en SMS como numero_infraccion
                registro['sms_enviado_codcliente'] = str(row['codcliente']) in cods_sms

                resultados.append(registro)

        # 6. Crear DataFrame final
        df_final = pd.DataFrame(resultados)

        # 7. Cruce con tabla bases: para cada pago, buscar el registro más reciente de bases por nitcliente/documento
        query_bases = """
            SELECT documento, base, fecha_entrega
            FROM bases
            WHERE documento = ANY(%s)
            AND fecha_entrega BETWEEN %s AND %s
        """
        with conn.cursor() as cursor:
            cursor.execute(query_bases, (nits_unicos, fecha_inicio, fecha_fin))
            bases_rows = cursor.fetchall()
            bases_cols = [desc[0] for desc in cursor.description]
            df_bases = pd.DataFrame(bases_rows, columns=bases_cols)
        if not df_bases.empty:
            df_bases['fecha_entrega'] = pd.to_datetime(df_bases['fecha_entrega'])
            # Para cada documento, quedarnos con el registro más reciente
            df_bases = df_bases.sort_values('fecha_entrega', ascending=False).drop_duplicates('documento', keep='first')
            df_bases = df_bases.rename(columns={'documento': 'nitcliente', 'base': 'base_cartera', 'fecha_entrega': 'fecha_entrega_cartera'})
            df_final = df_final.merge(df_bases[['nitcliente', 'base_cartera', 'fecha_entrega_cartera']], on='nitcliente', how='left')
        else:
            df_final['base_cartera'] = None
            df_final['fecha_entrega_cartera'] = None

        # Ordenar columnas: originales + nuevas
        original_cols = df.columns.tolist()
        nuevas_cols = [
            'fecha_gestion_cod', 'id_gestion_cod', 'resultado_cod', 'archivo_cod', 'tipollamada_cod', 'tipo_chat_cod',
            'fecha_gestion_nit', 'id_gestion_nit', 'resultado_nit', 'archivo_nit', 'tipollamada_nit', 'tipo_chat_nit',
            'sms_enviado', 'sms_enviado_codcliente',
            'base_cartera', 'fecha_entrega_cartera',
            'base_cartera_cod', 'fecha_entrega_cartera_cod'
        ]


        # 8. Cruce adicional: buscar en bases por codcliente <-> numero_comparendo
        cods_unicos = df['codcliente'].dropna().astype(str).unique().tolist()
        query_bases_cod = """
            SELECT numero_comparendo, base, fecha_entrega
            FROM bases
            WHERE numero_comparendo = ANY(%s)
            AND fecha_entrega BETWEEN %s AND %s
        """
        with conn.cursor() as cursor:
            cursor.execute(query_bases_cod, (cods_unicos, fecha_inicio, fecha_fin))
            bases_cod_rows = cursor.fetchall()
            bases_cod_cols = [desc[0] for desc in cursor.description]
            df_bases_cod = pd.DataFrame(bases_cod_rows, columns=bases_cod_cols)
        if not df_bases_cod.empty:
            df_bases_cod['fecha_entrega'] = pd.to_datetime(df_bases_cod['fecha_entrega'])
            df_bases_cod = df_bases_cod.sort_values('fecha_entrega', ascending=False).drop_duplicates('numero_comparendo', keep='first')
            df_bases_cod = df_bases_cod.rename(columns={'numero_comparendo': 'codcliente', 'base': 'base_cartera_cod', 'fecha_entrega': 'fecha_entrega_cartera_cod'})
            df_final = df_final.merge(df_bases_cod[['codcliente', 'base_cartera_cod', 'fecha_entrega_cartera_cod']], on='codcliente', how='left')
        else:
            df_final['base_cartera_cod'] = None
            df_final['fecha_entrega_cartera_cod'] = None

        # 9. Crear columnas únicas finales por cruce (gestion_final, sms_final, base_final, tipollamada_final, tipo_chat_final)
        # GESTION FINAL: prioriza gestion_cod, si no hay usa gestion_nit
        df_final['fecha_gestion_final'] = df_final['fecha_gestion_cod'].combine_first(df_final['fecha_gestion_nit'])
        df_final['id_gestion_final'] = df_final['id_gestion_cod'].combine_first(df_final['id_gestion_nit'])
        df_final['resultado_final'] = df_final['resultado_cod'].combine_first(df_final['resultado_nit'])
        df_final['archivo_final'] = df_final['archivo_cod'].combine_first(df_final['archivo_nit'])
        # TIPO LLAMADA FINAL: prioriza tipollamada_cod, si no hay usa tipollamada_nit
        df_final['tipollamada_final'] = df_final['tipollamada_cod'].combine_first(df_final['tipollamada_nit'])
        # TIPO CHAT FINAL: prioriza tipo_chat_cod, si no hay usa tipo_chat_nit
        df_final['tipo_chat_final'] = df_final['tipo_chat_cod'].combine_first(df_final['tipo_chat_nit'])

        # SMS FINAL: prioriza sms_enviado_codcliente, si no hay usa sms_enviado
        df_final['sms_final'] = df_final['sms_enviado_codcliente']
        df_final.loc[df_final['sms_final'].isna(), 'sms_final'] = df_final['sms_enviado']

        # BASE FINAL: prioriza base_cartera_cod, si no hay usa base_cartera
        df_final['base_final'] = df_final['base_cartera_cod'].combine_first(df_final['base_cartera'])
        df_final['fecha_entrega_final'] = df_final['fecha_entrega_cartera_cod'].combine_first(df_final['fecha_entrega_cartera'])
        # Si base_final queda vacía, poner 'Sin Base Asociada'
        df_final['base_final'] = df_final['base_final'].fillna('Sin Base Asociada')


        # Llenar vacíos en resultado_final y tipollamada_final con 'Envio de SMS'
        df_final['resultado_final'] = df_final['resultado_final'].fillna('Envio de SMS')
        df_final['tipollamada_final'] = df_final['tipollamada_final'].fillna('Envio de SMS')


        # Si tipo_chat_final es 'Whatsapp' o 'Chat alcaldia', poner ese valor en tipollamada_final
        mask_whatsapp = df_final['tipo_chat_final'].str.lower() == 'whatsapp'
        mask_chat_alcaldia = df_final['tipo_chat_final'].str.lower() == 'chat alcaldia'
        df_final.loc[mask_whatsapp, 'tipollamada_final'] = 'Whatsapp'
        df_final.loc[mask_chat_alcaldia, 'tipollamada_final'] = 'Chat alcaldia'

        # Si base_final es 'Sin Base Asociada' y tipollamada_final es 'Whatsapp', 'Chat alcaldia' o 'Entrante', poner ese valor en base_final
        mask_sin_base = df_final['base_final'] == 'Sin Base Asociada'
        mask_tllamada = df_final['tipollamada_final'].str.lower().isin(['whatsapp', 'chat alcaldia', 'entrante'])
        df_final.loc[mask_sin_base & mask_tllamada, 'base_final'] = df_final.loc[mask_sin_base & mask_tllamada, 'tipollamada_final']

        # Columnas finales únicas
        columnas_finales = [
            'fecha_gestion_final', 'id_gestion_final', 'resultado_final', 'archivo_final',
            'tipollamada_final', 'tipo_chat_final',
            'sms_final', 'base_final', 'fecha_entrega_final'
        ]
        return df_final[original_cols + nuevas_cols + columnas_finales]
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
    st.markdown("<h1 class='section-title'> Cruce de Datos Pagos Vs Gestiones y Cartera</h1>", unsafe_allow_html=True)
    st.markdown("---")

    col1, col2, col3 = st.columns([1, 6, 1])
    with col2:
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
                    # Filtro de rango de fechas para gestiones
                    hoy = datetime.now().date()
                    default_fin = hoy
                    default_ini = hoy - pd.Timedelta(days=6)
                    st.info("Puedes filtrar las gestiones a buscar por un rango de fechas (por defecto últimos 7 días)")
                    fecha_gestiones = st.date_input(
                        "Selecciona el rango de fechas de gestiones a buscar",
                        value=(default_ini, default_fin),
                        min_value=hoy.replace(year=hoy.year-5),
                        max_value=hoy,
                        format="DD/MM/YYYY"
                    )
                    if isinstance(fecha_gestiones, tuple) and len(fecha_gestiones) == 2:
                        fecha_inicio, fecha_fin = fecha_gestiones
                    else:
                        fecha_inicio = default_ini
                        fecha_fin = default_fin
                    if st.button("Ejecutar cruce de datos"):
                        with st.spinner("Buscando gestiones anteriores..."):
                            resultado = ejecutar_cruce(df, fecha_inicio, fecha_fin)
                            if resultado is not None:
                                st.session_state.cruce_resultado = resultado
                                st.rerun()
                else:
                    st.error("Faltan columnas requeridas")
            except Exception as e:
                st.error(f"Error: {str(e)}")

    with col2:
        
        if "cruce_resultado" in st.session_state:
            st.header("  Resultados para el Análisis de Pagos Vs Gestiones y Cartera")
            st.markdown("""---""")
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
                file_name=f"Análisis Pagos Vs Gestiones y cartera {hoy}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )



# ==============================================
# Logica para el status de bases
# ==============================================
def mostrar_status_bases():
    st.markdown("<h1 class='section-title'> Resultados de cada Base por Gestión</h1>", unsafe_allow_html=True)
    st.markdown("---")
    #st.header("📊 Status de Bases vs Gestiones")
    col1, col2, col3 = st.columns([1, 6, 1])
    with col2:
        conn = get_connection()

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
# ==============================================
# BARRA LATERAL - NAVEGACIÓN
# ==============================================
def topbar_navegacion():
    selected = option_menu(
        menu_title=None,
        options=[
            "Cruce de Datos Pagos Vs. Gestiones",
            "Status Bases General",
            "Status de Bases"
        ],
        icons=["table", "bar-chart", "database"],
        orientation="horizontal"
    )
    return selected
# ==============================================
# ESTRUCTURA PRINCIPAL
# ==============================================
def main():
    st.markdown("<h1 class='section-title'> Estado de Bases & Cruces</h1>", unsafe_allow_html=True)
    st.markdown("---")
    opcion_seleccionada = topbar_navegacion()
    
    with st.container():
        if opcion_seleccionada == "Cruce de Datos Pagos Vs. Gestiones":
            mostrar_vista_cruce()
        elif opcion_seleccionada == "Status Bases General":
            mostrar_status_bases_general()
        elif opcion_seleccionada == "Status de Bases":
            mostrar_status_bases()


def mostrar_status_bases_general():
    st.markdown("<h1 class='section-title'>Status Bases General</h1>", unsafe_allow_html=True)
    st.markdown("---")
    st.info("Aquí irá la lógica de Status Bases General.")

if __name__ == "__main__":
    main()
