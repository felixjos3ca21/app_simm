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
# Logica para el status de bases
# ==============================================





def mostrar_status_bases():
    st.header("📊 Status de Bases vs Gestiones")

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

    # 3. Filtrar documentos de la base seleccionada (esto es liviano, lo mantenemos)
    df_base_filtrada = df_bases[
        (df_bases['fecha_entrega'] == fecha_seleccionada) &
        (df_bases['base'] == base_seleccionada)
    ].drop_duplicates(subset='documento')

    # 4. Determinar rango de fechas (también liviano)
    fecha_actual = datetime.now().date()
    fecha_seleccionada_dt = pd.to_datetime(fecha_seleccionada)
    
    if fecha_actual.day <= 7:
        ultimo_dia_mes_anterior = fecha_seleccionada_dt.replace(day=1) - pd.Timedelta(days=1)
        fecha_inicio = ultimo_dia_mes_anterior - pd.Timedelta(days=6)
        fecha_fin = fecha_seleccionada_dt.replace(day=fecha_actual.day)
    else:
        fecha_inicio = fecha_seleccionada_dt.replace(day=1)
        fecha_fin = fecha_seleccionada_dt
    
    st.info(f"🔍 Rango de búsqueda: {fecha_inicio.strftime('%d/%m/%Y')} a {fecha_fin.strftime('%d/%m/%Y')}")

    # ===== NUEVO BOTÓN DE BÚSQUEDA =====
    if st.button("🔍 Ejecutar Búsqueda", type="primary"):
        with st.spinner("Buscando datos..."):
            # 5. Consultas pesadas (solo se ejecutan al hacer clic)
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
            ).fillna({
                'ultimo_resultado': 'Sin gestión',
                'asesor_ultimo': 'N/A',
                'fecha_ultima_gestion': pd.NaT,
                'resultado_positivo': 'Sin gestión',
                'asesor_positivo': 'N/A',
                'fecha_gestion_positiva': pd.NaT,
                'total_gestiones': 0,
                'total_sms': 0
            })

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
            n_con_gestion = len(df_resultado[df_resultado['ultimo_resultado'] != 'Sin gestión'])
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
                render_kpi("📉", "Sin Gestión", docs_sin_gestion, "#e8f5e9")
            
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
def sidebar_navegacion():
    with st.sidebar:
        st.header("Tipos de Cruces a la Base de Datos")
        st.write("Selecciona el tipo de cruce que deseas realizar")
        opcion = st.radio(
            "Seleccionar módulo:",
            options=[
                "Cruce de Datos Pagos Vs. Gestiones",
                "Status de Bases"
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
        elif opcion_seleccionada == "Status de Bases":
            mostrar_status_bases()


if __name__ == "__main__":
    main()
