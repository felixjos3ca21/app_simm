import streamlit as st
from src.database.postgres import get_engine
from assets.fondo import set_background
import pathlib
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
from sqlalchemy import text
from streamlit_option_menu import option_menu
import base64

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

 # Conexión a la base de datos
try:
    db = get_engine()
    st.success("Conexión exitosa a la base de datos SIMM.")
except Exception as e:
    st.error(f"Error al conectar con la base de datos SIMM: {e}")
    db = None


def load_image_base64(path):
    with open(path, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode()

img_base64 = load_image_base64("assets/images/icons8-whatsapp-48.png")
img_base64_up_arrow = load_image_base64("assets/images/icons8-up-16.png")
img_base64_down_arrow = load_image_base64("assets/images/icons8-down-arrow-24.png")
img_base64_line = load_image_base64("assets/images/icons8-horizontal-line-48.png")
# ======================================================================
# FUNCIONES DE ANÁLISIS
# ======================================================================

def mostrar_cartera(fecha_inicio, fecha_fin, db):
    fecha_fin_ajustada = fecha_fin + timedelta(days=1)
    fecha_inicio_com = fecha_inicio - relativedelta(months=1)
    fecha_fin_com = fecha_fin_ajustada - relativedelta(months=1)
    fecha_inicio_com2 = fecha_inicio - relativedelta(months=2)
    fecha_fin_com2 = fecha_fin_ajustada - relativedelta(months=2)    
    
    with st.spinner('🔄 Consultando información de cartera...'):
        try:
            with db.connect() as conn:
                query_kpi_com = text("""
                    SELECT
                        COUNT(*) AS registros_totales,
                        COUNT(DISTINCT documento) AS documentos_unicos,
                        SUM(valor_infraccion) AS monto_total
                    FROM bases
                    WHERE fecha_entrega BETWEEN :fecha_inicio_com AND :fecha_fin_com
                """)
                query_kpi_com2 = text("""
                    SELECT
                        COUNT(*) AS registros_totales,
                        COUNT(DISTINCT documento) AS documentos_unicos,
                        SUM(valor_infraccion) AS monto_total
                    FROM bases
                    WHERE fecha_entrega BETWEEN :fecha_inicio_com2 AND :fecha_fin_com2
                """)
                query_kpi = text("""
                    SELECT
                        COUNT(*) AS registros_totales,
                        COUNT(DISTINCT documento) AS documentos_unicos,
                        SUM(valor_infraccion) AS monto_total
                    FROM bases
                    WHERE fecha_entrega BETWEEN :fecha_inicio AND :fecha_fin
                """)
                query_detalle = text("""
                    SELECT
                        DATE(fecha_entrega) AS fecha,
                        base,
                        COUNT(*) AS registros,
                        COUNT(DISTINCT documento) AS documentos_unicos,
                        SUM(CASE WHEN estado_telefono LIKE 'VALIDO:%' THEN 1 ELSE 0 END) AS validos,
                        SUM(CASE WHEN estado_telefono LIKE 'AUSENTE:%' OR estado_telefono = '' THEN 1 ELSE 0 END) AS ausentes,
                        SUM(CASE WHEN estado_telefono LIKE 'ERRADO:%' THEN 1 ELSE 0 END) AS errados,
                        TO_CHAR(SUM(valor_infraccion),'FM$ 999,999,999,999') AS monto_total
                    FROM bases
                    WHERE fecha_entrega BETWEEN :fecha_inicio AND :fecha_fin
                    GROUP BY DATE(fecha_entrega), base
                    ORDER BY fecha DESC, base
                """)
                # KPI: Cantidad de bases entregadas en el periodo (únicas por fecha)
                query_bases_unicas = text("""
                    SELECT COUNT(*) FROM (
                        SELECT DISTINCT DATE(fecha_entrega), base
                        FROM bases
                        WHERE fecha_entrega BETWEEN :fecha_inicio AND :fecha_fin
                    ) AS sub
                """)
                # KPI: Cantidad de bases entregadas en el periodo anterior (únicas por fecha)
                query_bases_unicas_com = text("""
                    SELECT COUNT(*) FROM (
                        SELECT DISTINCT DATE(fecha_entrega), base
                        FROM bases
                        WHERE fecha_entrega BETWEEN :fecha_inicio_com AND :fecha_fin_com
                    ) AS sub
                """)
                query_bases_unicas_com2 = text("""
                    SELECT COUNT(*) FROM (
                        SELECT DISTINCT DATE(fecha_entrega), base
                        FROM bases
                        WHERE fecha_entrega BETWEEN :fecha_inicio_com2 AND :fecha_fin_com2
                    ) AS sub
                """)
                params = {'fecha_inicio': fecha_inicio, 'fecha_fin': fecha_fin_ajustada}
                result_kpi = conn.execute(query_kpi, params)
                kpi = result_kpi.fetchone()

                params_com = {'fecha_inicio_com': fecha_inicio_com, 'fecha_fin_com': fecha_fin_com}
                result_kpi_com = conn.execute(query_kpi_com, params_com)
                kpi_com = result_kpi_com.fetchone()

                params_com2 = {'fecha_inicio_com2': fecha_inicio_com2, 'fecha_fin_com2': fecha_fin_com2}
                result_kpi_com2 = conn.execute(query_kpi_com2, params_com2)
                kpi_com2 = result_kpi_com2.fetchone()

                result_detalle = conn.execute(query_detalle, params)
                
                df_detalle = pd.DataFrame(result_detalle.fetchall(), columns=result_detalle.keys())
                
                result_bases_unicas = conn.execute(query_bases_unicas, params)
                cantidad_bases_unicas = result_bases_unicas.scalar()
                
                result_bases_unicas_com = conn.execute(query_bases_unicas_com, params_com)
                cantidad_bases_unicas_com = result_bases_unicas_com.scalar()
                result_bases_unicas_com2 = conn.execute(query_bases_unicas_com2, params_com2)
                cantidad_bases_unicas_com2 = result_bases_unicas_com2.scalar()
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                
                if cantidad_bases_unicas_com == 0:
                    delta_pct = 100 if cantidad_bases_unicas > 0 else 0
                else:
                    delta_pct = ((cantidad_bases_unicas - cantidad_bases_unicas_com) / cantidad_bases_unicas_com) * 100
                if cantidad_bases_unicas_com2 == 0:
                    delta_pct2 = 100 if cantidad_bases_unicas_com > 0 else 0
                else:
                    delta_pct2 = ((cantidad_bases_unicas_com - cantidad_bases_unicas_com2) / cantidad_bases_unicas_com2) * 100
            
                if delta_pct > 0:
                    flecha = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                    color = "#32882f"
                elif delta_pct < 0:
                    flecha = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                    color = "#a03838"
                else:
                    flecha = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                    color = "#2A2828"
                st.markdown(f"""
                    <div style='background:#F5F5F5;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color}; border-right: 6px solid {color};'>
                    <div style='color:#053e01ff;font-size:1.7rem'>Cantidad de Bases Asigandas</div>
                    <div style='font-size:3rem'>{cantidad_bases_unicas:,}</div>
                    <div style='color:{color};font-size:1.2rem'>
                        {flecha} {(delta_pct):.2f}%
                    </div>
                    
                    </div>
                    """, unsafe_allow_html=True)
                
                
                if delta_pct2 > 0:
                    flecha = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                    color = "#32882f"
                elif delta_pct2 < 0:
                    flecha = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                    color = "#a03838"
                else:
                    flecha = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                    color = "#2A2828"
                st.markdown(f"""
                    <div style='background:#F5F5F5;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color}; border-right: 6px solid {color};'>
                    <div style='color:#053e01ff;font-size:1.7rem'>Período Anterior</div>
                    <div style='font-size:3rem'>{cantidad_bases_unicas_com:,}</div>
                    <div style='color:{color};font-size:1.2rem'>
                        {flecha} {(delta_pct2):.2f}%
                    </div>
                    
                    </div>
                    """, unsafe_allow_html=True)
                

            with col2:
                
                if kpi_com[0] == 0:
                    delta_kpi_0 = 0
                else:
                    delta_kpi_0 = ((kpi[0] - kpi_com[0]) / kpi_com[0]) * 100

                if kpi_com2[0] == 0:
                    delta_kpi_0_2 = 0
                else:
                    delta_kpi_0_2 = ((kpi_com[0] - kpi_com2[0]) / kpi_com2[0]) * 100
                
                delta = delta_kpi_0
                if delta_kpi_0 > 0:
                    flecha = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                    color = "#32882f"
                elif delta_kpi_0 < 0:
                    flecha = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                    color = "#a03838"
                else:
                    flecha = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                    color = "#2A2828"
                st.markdown(f"""
                    <div style='background:#E0E0E0;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color}; border-right: 6px solid {color};'>
                    <div style='color:#053e01ff;font-size:1.7rem'>Registros Totales</div>
                    <div style='font-size:3rem'>{f"{kpi[0]:,}".replace(",",".")}</div>
                    <div style='color:{color};font-size:1.2rem'>
                        {flecha} {abs(delta):.2f}%
                    </div>
                    
                    </div>
                    """, unsafe_allow_html=True)
                
                delta = delta_kpi_0_2
                if delta_kpi_0_2 > 0:
                    flecha = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                    color = "#32882f"
                elif delta_kpi_0_2 < 0:
                    flecha = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                    color = "#a03838"
                else:
                    flecha = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                    color = "#2A2828"

                st.markdown(f"""
                    <div style='background:#E0E0E0;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color}; border-right: 6px solid {color};'>
                    <div style='color:#053e01ff;font-size:1.7rem'> Período Anterior</div>
                    <div style='font-size:3rem'>{f"{kpi_com[0]:,}".replace(",",".")}</div>
                    <div style='color:{color};font-size:1.2rem'>
                        {flecha} {abs(delta):.2f}%
                    </div>

                    </div>
                    """, unsafe_allow_html=True)
                
                
            with col3:
                # DELTA para documentos únicos
                if kpi_com[1] == 0:
                    delta_kpi_1 = 0
                else:
                    delta_kpi_1 = ((kpi[1] - kpi_com[1]) / kpi_com[1]) * 100
                
                if kpi_com2[1] == 0:
                    delta_kpi_1_2 = 0
                else:
                    delta_kpi_1_2 = ((kpi_com[1] - kpi_com2[1]) / kpi_com2[1]) * 100
                

                delta = delta_kpi_1
                if delta_kpi_1 > 0:
                    flecha = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                    color = "#32882f"
                elif delta_kpi_1 < 0:
                    flecha = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                    color = "#a03838"
                else:
                    flecha = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                    color = "#2A2828"

                st.markdown(f"""
                    <div style='background:#F5F5F5;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color}; border-right: 6px solid {color};'>
                    <div style='color:#053e01ff;font-size:1.7rem'>Documentos Únicos</div>
                    <div style='font-size:3rem'>{f"{kpi[1]:,}".replace(",",".")}</div>
                    <div style='color:{color};font-size:1.2rem'>
                        {flecha} {abs(delta):.2f}%
                    </div>
                    </div>
                """, unsafe_allow_html=True)

                delta = delta_kpi_1_2
                if delta_kpi_1_2 > 0:
                    flecha = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                    color = "#32882f"
                elif delta_kpi_1_2 < 0:
                    flecha = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                    color = "#a03838"
                else:
                    flecha = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                    color = "#2A2828"

                st.markdown(f"""
                    <div style='background:#F5F5F5;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color}; border-right: 6px solid {color};'>
                    <div style='color:#053e01ff;font-size:1.7rem'>Período Anterior</div>
                    <div style='font-size:3rem'>{f"{kpi_com[1]:,}".replace(",",".")}</div>
                    <div style='color:{color};font-size:1.2rem'>
                        {flecha} {abs(delta):.2f}%
                    </div>
                    </div>
                """, unsafe_allow_html=True)
                

            with col4:
                # DELTA para monto total
                if kpi_com[2] == 0:
                    delta_kpi_2 = 100 if kpi[2] > 0 else 0
                else:
                    delta_kpi_2 = ((kpi[2] - kpi_com[2]) / kpi_com[2]) * 100
                if kpi_com2[2] == 0:
                    delta_kpi_2_2 = 100 if kpi_com[2] > 0 else 0
                else:
                    delta_kpi_2_2 = ((kpi_com[2] - kpi_com2[2]) / kpi_com2[2]) * 100
                

                delta = delta_kpi_2
                if delta_kpi_2 > 0:
                    flecha = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                    color = "#32882f"
                elif delta_kpi_2 < 0:
                    flecha = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                    color = "#a03838"
                else:
                    flecha = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                    color = "#2A2828"
                st.markdown(f"""
                    <div style='background:#E0E0E0;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color}; border-right: 6px solid {color};'>
                    <div style='color:#053e01ff;font-size:1.7rem'>Monto Total</div>
                    <div style='font-size:3rem'>{f"${kpi[2]:,.0f}".replace(",",".")}</div>
                    <div style='color:{color};font-size:1.2rem'>
                        {flecha} {abs(delta):.2f}%
                    </div>
                    
                    </div>
                    """, unsafe_allow_html=True)
                delta = delta_kpi_2_2
                if delta_kpi_2_2 > 0:
                    flecha = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                    color = "#32882f"
                elif delta_kpi_2_2 < 0:
                    flecha = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                    color = "#a03838"
                else:
                    flecha = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                    color = "#2A2828"
                st.markdown(f"""
                    <div style='background:#E0E0E0;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color}; border-right: 6px solid {color};'>
                    <div style='color:#053e01ff;font-size:1.7rem'>Período Anterior</div>
                    <div style='font-size:3rem'>{f"${kpi_com[2]:,.0f}".replace(",",".")}</div>
                    <div style='color:{color};font-size:1.2rem'>
                        {flecha} {abs(delta):.2f}%
                    </div>
                    </div>
                    """, unsafe_allow_html=True)

            st.markdown("---")

            st.subheader("Detalle por Día y Base")
            col1, col2 = st.columns(2)
            with col1:
                if not df_detalle.empty:
                    df_detalle['fecha'] = pd.to_datetime(df_detalle['fecha'])
                    st.dataframe(
                        df_detalle,
                        column_config={
                            "fecha": st.column_config.DateColumn("Fecha", format="DD/MM/YYYY"),
                            "base": "Base",
                            "registros": st.column_config.NumberColumn("Registros", format="%d"),
                            "documentos_unicos": st.column_config.NumberColumn("Documentos Únicos", format="%d"),
                            "validos": st.column_config.NumberColumn("Teléfonos Válidos", format="%d"),
                            "errados": st.column_config.NumberColumn("Teléfonos Errados", format="%d"),
                            "ausentes": st.column_config.NumberColumn("Teléfonos Ausentes", format="%d"),
                            "monto_total": st.column_config.TextColumn("Monto Total")
                        },
                        use_container_width=True,
                        hide_index=True,
                        height=400
                    )
                else:
                    st.info("No hay registros en el rango seleccionado.")
        except Exception as e:
            st.error(f"Error al consultar la información: {e}")

        # ===================== NUEVO BLOQUE: FILTRO POR BASE =====================
    with db.connect() as conn:
        # Definir fechas para el filtro de bases
        fecha_inicio_bases = fecha_inicio if 'fecha_inicio' in locals() else date.today().replace(day=1)
        fecha_fin_bases = fecha_fin_ajustada if 'fecha_fin_ajustada' in locals() else date.today()
        bases_query = conn.execute(
            text("SELECT DISTINCT base FROM bases WHERE fecha_entrega BETWEEN :fecha_inicio AND :fecha_fin ORDER BY base"),
            {'fecha_inicio': fecha_inicio_bases, 'fecha_fin': fecha_fin_bases}
        )
        bases_list = [row[0] for row in bases_query.fetchall()]

    st.markdown("---")
    st.markdown(
        "<h2 style='color:#32882f; text-align:center;'>Consulta por Base Específica</h2>",
        unsafe_allow_html=True
    )
    col_base, col_btn = st.columns([3,1])
    with col_base:
        base_seleccionada = st.selectbox("Selecciona la base a consultar:", bases_list)
    with col_btn:
        buscar_base = st.button("Buscar información de la base seleccionada")

    if buscar_base and base_seleccionada:
        with st.spinner(f'🔄 Consultando información para la base: {base_seleccionada} ...'):
            try:
                with db.connect() as conn:
                    params = {'fecha_inicio': fecha_inicio, 'fecha_fin': fecha_fin_ajustada, 'base': base_seleccionada}
                    params_com = {'fecha_inicio_com': fecha_inicio_com, 'fecha_fin_com': fecha_fin_com, 'base': base_seleccionada}
                    params_com2 = {'fecha_inicio_com2': fecha_inicio_com2, 'fecha_fin_com2': fecha_fin_com2, 'base': base_seleccionada}
                    query_kpi = text("""
                        SELECT
                            COUNT(*) AS registros_totales,
                            COUNT(DISTINCT documento) AS documentos_unicos,
                            SUM(valor_infraccion) AS monto_total
                        FROM bases
                        WHERE fecha_entrega BETWEEN :fecha_inicio AND :fecha_fin AND base = :base
                    """)
                    query_kpi_com = text("""
                        SELECT
                            COUNT(*) AS registros_totales,
                            COUNT(DISTINCT documento) AS documentos_unicos,
                            SUM(valor_infraccion) AS monto_total
                        FROM bases
                        WHERE fecha_entrega BETWEEN :fecha_inicio_com AND :fecha_fin_com AND base = :base
                    """)
                    query_kpi_com2 = text("""
                        SELECT
                            COUNT(*) AS registros_totales,
                            COUNT(DISTINCT documento) AS documentos_unicos,
                            SUM(valor_infraccion) AS monto_total
                        FROM bases
                        WHERE fecha_entrega BETWEEN :fecha_inicio_com2 AND :fecha_fin_com2 AND base = :base
                    """)
                    query_detalle = text("""
                        SELECT
                            DATE(fecha_entrega) AS fecha,
                            base,
                            COUNT(*) AS registros,
                            COUNT(DISTINCT documento) AS documentos_unicos,
                            SUM(CASE WHEN estado_telefono LIKE 'VALIDO:%' THEN 1 ELSE 0 END) AS validos,
                            SUM(CASE WHEN estado_telefono LIKE 'AUSENTE:%' OR estado_telefono = '' THEN 1 ELSE 0 END) AS ausentes,
                            SUM(CASE WHEN estado_telefono LIKE 'ERRADO:%' THEN 1 ELSE 0 END) AS errados,
                            TO_CHAR(SUM(valor_infraccion),'FM$ 999,999,999,999') AS monto_total
                        FROM bases
                        WHERE fecha_entrega BETWEEN :fecha_inicio AND :fecha_fin AND base = :base
                        GROUP BY DATE(fecha_entrega), base
                        ORDER BY fecha DESC, base
                    """)
                    query_detalle_ante = text("""
                        SELECT
                            DATE(fecha_entrega) AS fecha,
                            base,
                            COUNT(*) AS registros,
                            COUNT(DISTINCT documento) AS documentos_unicos,
                            SUM(CASE WHEN estado_telefono LIKE 'VALIDO:%' THEN 1 ELSE 0 END) AS validos,
                            SUM(CASE WHEN estado_telefono LIKE 'AUSENTE:%' OR estado_telefono = '' THEN 1 ELSE 0 END) AS ausentes,
                            SUM(CASE WHEN estado_telefono LIKE 'ERRADO:%' THEN 1 ELSE 0 END) AS errados,
                            TO_CHAR(SUM(valor_infraccion),'FM$ 999,999,999,999') AS monto_total
                        FROM bases
                        WHERE fecha_entrega BETWEEN :fecha_inicio AND :fecha_fin AND base = :base
                        GROUP BY DATE(fecha_entrega), base
                        ORDER BY fecha DESC, base
                    """)
                    query_bases_unicas = text("""
                        SELECT COUNT(*) FROM (
                            SELECT DISTINCT DATE(fecha_entrega), base
                            FROM bases
                            WHERE fecha_entrega BETWEEN :fecha_inicio AND :fecha_fin AND base = :base
                        ) AS sub
                    """)
                    query_bases_unicas_com = text("""
                        SELECT COUNT(*) FROM (
                            SELECT DISTINCT DATE(fecha_entrega), base
                            FROM bases
                            WHERE fecha_entrega BETWEEN :fecha_inicio_com AND :fecha_fin_com AND base = :base
                        ) AS sub
                    """)
                    query_bases_unicas_com2 = text("""
                        SELECT COUNT(*) FROM (
                            SELECT DISTINCT DATE(fecha_entrega), base
                            FROM bases
                            WHERE fecha_entrega BETWEEN :fecha_inicio_com2 AND :fecha_fin_com2 AND base = :base
                        ) AS sub
                    """)
                    result_kpi = conn.execute(query_kpi, params)
                    kpi = result_kpi.fetchone()
                    result_kpi_com = conn.execute(query_kpi_com, params_com)
                    kpi_com = result_kpi_com.fetchone()
                    result_kpi_com2 = conn.execute(query_kpi_com2, params_com2)
                    kpi_com2 = result_kpi_com2.fetchone()
                    result_detalle = conn.execute(query_detalle, params)
                    df_detalle = pd.DataFrame(result_detalle.fetchall(), columns=result_detalle.keys())
                    result_bases_unicas = conn.execute(query_bases_unicas, params)
                    cantidad_bases_unicas = result_bases_unicas.scalar()
                    result_bases_unicas_com = conn.execute(query_bases_unicas_com, params_com)
                    result_detalle_ante = conn.execute(query_detalle_ante, {
                                                            'fecha_inicio': fecha_inicio_com,
                                                            'fecha_fin': fecha_fin_com,
                                                            'base': base_seleccionada
                                                        })
                    df_detalle_com = pd.DataFrame(result_detalle_ante.fetchall(), columns=result_detalle_ante.keys())   
                    cantidad_bases_unicas_com = result_bases_unicas_com.scalar()
                    result_bases_unicas_com2 = conn.execute(query_bases_unicas_com2, params_com2)
                    cantidad_bases_unicas_com2 = result_bases_unicas_com2.scalar()

                    cantidad_bases_unicas = cantidad_bases_unicas or 0
                    cantidad_bases_unicas_com = cantidad_bases_unicas_com or 0
                    cantidad_bases_unicas_com2 = cantidad_bases_unicas_com2 or 0

                    kpi = tuple(0 if v is None else v for v in (kpi or (0,0,0)))
                    kpi_com = tuple(0 if v is None else v for v in (kpi_com or (0,0,0)))
                    kpi_com2 = tuple(0 if v is None else v for v in (kpi_com2 or (0,0,0)))
            
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    # DELTA para cantidad de bases asignadas
                    if cantidad_bases_unicas_com == 0:
                        delta_pct = 100 if cantidad_bases_unicas > 0 else 0
                    else:
                        delta_pct = ((cantidad_bases_unicas - cantidad_bases_unicas_com) / cantidad_bases_unicas_com) * 100
                    if cantidad_bases_unicas_com2 == 0:
                        delta_pct2 = 100 if cantidad_bases_unicas_com > 0 else 0
                    else:
                        delta_pct2 = ((cantidad_bases_unicas_com - cantidad_bases_unicas_com2) / cantidad_bases_unicas_com2) * 100
                
                    if delta_pct > 0:
                        flecha = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color = "#32882f"
                    elif delta_pct < 0:
                        flecha = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color = "#a03838"
                    else:
                        flecha = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color = "#2A2828"
                        
                    st.markdown(f"""
                        <div style='background:#F5F5F5;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color}; border-right: 6px solid {color};'>
                        <div style='color:#053e01ff;font-size:1.7rem'>Cantidad de Bases Asignadas</div>
                        <div style='font-size:3rem'>{cantidad_bases_unicas:,}</div>
                        <div style='color:{color};font-size:1.2rem'>
                            {flecha} {(delta_pct):.2f}%
                        </div>
                        
                        </div>
                        """, unsafe_allow_html=True)
                    
                    if delta_pct2 > 0:
                        flecha = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color = "#32882f"
                    elif delta_pct2 < 0:
                        flecha = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color = "#a03838"
                    else:
                        flecha = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color = "#2A2828"
                    st.markdown(f"""
                        <div style='background:#F5F5F5;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color}; border-right: 6px solid {color};'>
                        <div style='color:#053e01ff;font-size:1.7rem'>Período Anterior</div>
                        <div style='font-size:3rem'>{cantidad_bases_unicas_com:,}</div>
                        <div style='color:{color};font-size:1.2rem'>
                            {flecha} {(delta_pct2):.2f}%
                        </div
                        
                        </div>
                        """, unsafe_allow_html=True)

                with col2:
                    # DELTA para registros totales
                    if kpi_com[0] == 0:
                        delta_kpi_0 = 0
                    else:
                        delta_kpi_0 = ((kpi[0] - kpi_com[0]) / kpi_com[0]) * 100
                    if kpi_com2[0] == 0:
                        delta_kpi_0_2 = 0
                    else:
                        delta_kpi_0_2 = ((kpi_com[0] - kpi_com2[0]) / kpi_com2[0]) * 100
                    
                    
                    delta = delta_kpi_0
                    if delta_kpi_0 > 0:
                        flecha = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color = "#32882f"
                    elif delta_kpi_0 < 0:
                        flecha = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color = "#a03838"
                    else:
                        flecha = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color = "#2A2828"
                    st.markdown(f"""
                        <div style='background:#E0E0E0;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color}; border-right: 6px solid {color};'>
                        <div style='color:#053e01ff;font-size:1.7rem'>Registros Totales</div>
                        <div style='font-size:3rem'>{f"{kpi[0]:,}".replace(",",".")}</div>
                        <div style='color:{color};font-size:1.2rem'>
                            {flecha} {abs(delta):.2f}%
                        </div>
                        
                        </div>
                        """, unsafe_allow_html=True)
                    
                    delta = delta_kpi_0_2
                    if delta_kpi_0_2 > 0:
                        flecha = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color = "#32882f"
                    elif delta_kpi_0_2 < 0:
                        flecha = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color = "#a03838"
                    else:
                        flecha = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color = "#2A2828"

                    st.markdown(f"""
                        <div style='background:#E0E0E0;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color}; border-right: 6px solid {color};'>
                        <div style='color:#053e01ff;font-size:1.7rem'> Período Anterior</div>
                        <div style='font-size:3rem'>{f"{kpi_com[0]:,}".replace(",",".")}</div>
                        <div style='color:{color};font-size:1.2rem'>
                            {flecha} {abs(delta):.2f}%
                        </div>

                        </div>
                        """, unsafe_allow_html=True)
                
                with col3:
                    # DELTA para documentos únicos
                    if kpi_com[1] == 0:
                        delta_kpi_1 = 0
                    else:
                        delta_kpi_1 = ((kpi[1] - kpi_com[1]) / kpi_com[1]) * 100
                    
                    if kpi_com2[1] == 0:
                        delta_kpi_1_2 = 0
                    else:
                        delta_kpi_1_2 = ((kpi_com[1] - kpi_com2[1]) / kpi_com2[1]) * 100
                    

                    delta = delta_kpi_1
                    if delta_kpi_1 > 0:
                        flecha = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color = "#32882f"
                    elif delta_kpi_1 < 0:
                        flecha = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color = "#a03838"
                    else:
                        flecha = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color = "#2A2828"
                    st.markdown(f"""
                        <div style='background:#F5F5F5;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color}; border-right: 6px solid {color};'>
                        <div style='color:#053e01ff;font-size:1.7rem'>Documentos Únicos</div>
                        <div style='font-size:3rem'>{f"{kpi[1]:,}".replace(",",".")}</div>
                        <div style='color:{color};font-size:1.2rem'>
                            {flecha} {abs(delta):.2f}%
                        </div>
                        </div>
                    """, unsafe_allow_html=True)

                    delta = delta_kpi_1_2
                    if delta_kpi_1_2 > 0:
                        flecha = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color = "#32882f"
                    elif delta_kpi_1_2 < 0:
                        flecha = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color = "#a03838"
                    else:
                        flecha = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color = "#2A2828"
                    st.markdown(f"""
                        <div style='background:#F5F5F5;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color}; border-right: 6px solid {color};'>
                        <div style='color:#053e01ff;font-size:1.7rem'>Período Anterior</div>
                        <div style='font-size:3rem'>{f"{kpi_com[1]:,}".replace(",",".")}</div>
                        <div style='color:{color};font-size:1.2rem'>
                            {flecha} {abs(delta):.2f}%
                        </div>
                        </div>
                    """, unsafe_allow_html=True)
                with col4:
                    # DELTA para monto total
                    if kpi_com[2] == 0:
                        delta_kpi_2 = 0
                    else:
                        delta_kpi_2 = ((kpi[2] - kpi_com[2]) / kpi_com[2]) * 100
                    if kpi_com2[2] == 0:
                        delta_kpi_2_2 = 0
                    else:
                        delta_kpi_2_2 = ((kpi_com[2] - kpi_com2[2]) / kpi_com2[2]) * 100
                    

                    delta = delta_kpi_2
                    if delta_kpi_2 > 0:
                        flecha = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color = "#32882f"
                    elif delta_kpi_2 < 0:
                        flecha = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color = "#a03838"
                    else:
                        flecha = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color = "#2A2828"
                    st.markdown(f"""
                        <div style='background:#E0E0E0;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color}; border-right: 6px solid {color};'>
                        <div style='color:#053e01ff;font-size:1.7rem'>Monto Total</div>
                        <div style='font-size:3rem'>{f"${kpi[2]:,.0f}".replace(",",".")}</div>
                        <div style='color:{color};font-size:1.2rem'>
                            {flecha} {abs(delta):.2f}%
                        </div>
                        
                        </div>
                        """, unsafe_allow_html=True)
                    delta = delta_kpi_2_2
                    if delta_kpi_2_2 > 0:
                        flecha = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color = "#32882f"
                    elif delta_kpi_2_2 < 0:
                        flecha = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color = "#a03838"
                    else:
                        flecha = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color = "#2A2828"
                    st.markdown(f"""
                        <div style='background:#E0E0E0;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color}; border-right: 6px solid {color};'>
                        <div style='color:#053e01ff;font-size:1.7rem'>Período Anterior</div>
                        <div style='font-size:3rem'>{f"${kpi_com[2]:,.0f}".replace(",",".")}</div>
                        <div style='color:{color};font-size:1.2rem'>
                            {flecha} {abs(delta):.2f}%
                        </div>
                        
                        </div>
                        """, unsafe_allow_html=True)

                st.markdown("---")

                st.subheader("Detalle por Día y Base")
                col1, col2 = st.columns(2)
                with col1:
                    required_cols = {'fecha', 'base'}
                    # Asegurar que ambos DataFrames tengan las mismas columnas
                    metricas = ['registros', 'documentos_unicos', 'validos', 'errados', 'ausentes', 'monto_total']
                    for col in metricas:
                        if col not in df_detalle.columns:
                            df_detalle[col] = 0
                        if col not in df_detalle_com.columns:
                            df_detalle_com[col] = 0
                    if required_cols.issubset(df_detalle.columns) and required_cols.issubset(df_detalle_com.columns):
                        # Concatenar ambos DataFrames y mostrar juntos
                        df_detalle['fecha'] = pd.to_datetime(df_detalle['fecha'])
                        df_detalle_com['fecha'] = pd.to_datetime(df_detalle_com['fecha'])
                        df_combinado = pd.concat([df_detalle, df_detalle_com], ignore_index=True)
                        st.dataframe(
                            df_combinado[[
                                'fecha', 'base', 'registros', 'documentos_unicos', 'validos', 'errados', 'ausentes', 'monto_total'
                            ]],
                            column_config={
                                "fecha": st.column_config.DateColumn("Fecha", format="DD/MM/YYYY"),
                                "base": "Base",
                                "registros": st.column_config.NumberColumn("Registros", format="%d"),
                                "documentos_unicos": st.column_config.NumberColumn("Documentos Únicos", format="%d"),
                                "validos": st.column_config.NumberColumn("Teléfonos Válidos", format="%d"),
                                "errados": st.column_config.NumberColumn("Teléfonos Errados", format="%d"),
                                "ausentes": st.column_config.NumberColumn("Teléfonos Ausentes", format="%d"),
                                "monto_total": st.column_config.TextColumn("Monto Total")
                            },
                            use_container_width=True,
                            hide_index=True,
                            height=400
                        )
                    else:
                        st.info("No hay registros suficientes para comparar por día y base.")
            except Exception as e:
                st.error(f"Error al consultar la información: {e}")
# ===================== FIN BLOQUE: FILTRO POR BASE =====================



# =======================================================================
# ===================== NUEVO BLOQUE: GESTIONES =====================
# =======================================================================


def mostrar_gestiones(fecha_inicio, fecha_fin, db):
    fecha_fin_ajustada = fecha_fin + timedelta(days=1)

    fecha_inicio_com = fecha_inicio - relativedelta(months=1)
    fecha_fin_com = fecha_fin_ajustada - relativedelta(months=1)
    fecha_inicio_com2 = fecha_inicio - relativedelta(months=2)
    fecha_fin_com2 = fecha_fin_ajustada - relativedelta(months=2)

    with st.spinner('🔄 Consultando información de gestiones...'):    
        try:
            with db.connect() as conn:
                query_cuenta_gestiones_id_act = text("""
                    SELECT
                        count(distinct id_gestion) AS total_gestiones
                    FROM gestiones
                    where fecha_gestion_sencilla between :fecha_inicio and :fecha_fin
                """)
                query_cuenta_gestiones_id_act_com = text("""
                    SELECT
                        count(distinct id_gestion) AS total_gestiones
                    FROM gestiones
                    where fecha_gestion_sencilla between :fecha_inicio_com and :fecha_fin_com
                """)
                query_cuenta_gestiones_id_act_com2 = text("""
                    SELECT
                        count(distinct id_gestion) AS total_gestiones
                    FROM gestiones
                    where fecha_gestion_sencilla between :fecha_inicio_com2 and :fecha_fin_com2
                """)
                query_cuenta_gestiones_doc_act = text("""
                    SELECT
                        count(distinct documento) AS total_gestiones_doc
                    FROM gestiones
                    where fecha_gestion_sencilla between :fecha_inicio and :fecha_fin
                """)
                query_cuenta_gestiones_doc_act_com = text("""
                    SELECT
                        count(distinct documento) AS total_gestiones_doc
                    FROM gestiones
                    where fecha_gestion_sencilla between :fecha_inicio_com and :fecha_fin_com
                """)
                query_cuenta_gestiones_doc_com2 = text("""
                    SELECT
                        count(distinct documento) AS total_gestiones_doc
                    FROM gestiones
                    where fecha_gestion_sencilla between :fecha_inicio_com2 and :fecha_fin_com2
                """)
                query_cuenta_sms_act = text("""
                    SELECT
                        count(telefono) AS total_sms
                    FROM sms
                    where fecha_sms between :fecha_inicio and :fecha_fin
                """)
                query_cuenta_sms_act_com = text("""
                    SELECT
                        count(telefono) AS total_sms
                    FROM sms
                    where fecha_sms between :fecha_inicio_com and :fecha_fin_com
                """)
                query_cuenta_sms_act_com2 = text("""
                    SELECT
                        count(telefono) AS total_sms
                    FROM sms
                    where fecha_sms between :fecha_inicio_com2 and :fecha_fin_com2
                """)
                query_Whatsapp = text("""
                    SELECT 
                        COUNT(DISTINCT id_gestion) AS gestion_whatsapp
                    FROM gestiones
                    WHERE fecha_gestion_sencilla BETWEEN :fecha_inicio AND :fecha_fin
                    AND tipo_chat ILIKE '%whatsapp%'
                """)
                query_Whatsapp_com = text("""
                    SELECT
                        COUNT(DISTINCT id_gestion) AS gestion_whatsapp
                    FROM gestiones
                    WHERE fecha_gestion_sencilla BETWEEN :fecha_inicio_com AND :fecha_fin_com
                    AND tipo_chat ILIKE '%whatsapp%'
                """)
                query_Whatsapp_com2 = text("""
                    SELECT
                        COUNT(DISTINCT id_gestion) AS gestion_whatsapp
                    FROM gestiones
                    WHERE fecha_gestion_sencilla BETWEEN :fecha_inicio_com2 AND :fecha_fin_com2
                    AND tipo_chat ILIKE '%whatsapp%'
                """)
                query_chat_act = text("""
                    SELECT
                        COUNT(DISTINCT id_gestion) AS gestion_chat
                    FROM gestiones
                    WHERE fecha_gestion_sencilla BETWEEN :fecha_inicio AND :fecha_fin
                    AND tipo_chat ILIKE '%Chat alcaldia%'
                """)
                query_chat_act_com = text("""
                    SELECT
                        COUNT(DISTINCT id_gestion) AS gestion_chat
                    FROM gestiones
                    WHERE fecha_gestion_sencilla BETWEEN :fecha_inicio_com AND :fecha_fin_com
                    AND tipo_chat ILIKE '%Chat alcaldia%'
                """)
                query_chat_act_com2 = text("""
                    SELECT
                        COUNT(DISTINCT id_gestion) AS gestion_chat
                    FROM gestiones
                    WHERE fecha_gestion_sencilla BETWEEN :fecha_inicio_com2 AND :fecha_fin_com2
                    AND tipo_chat ILIKE '%Chat alcaldia%'
                """)
                query_campanas = text("""
                    SELECT
                        COUNT(telephone) AS contactos_predictivos
                    FROM tipificaciones_3
                    where type_interaction = 'out_pre'
                        AND date BETWEEN :fecha_inicio AND :fecha_fin
                """)
                query_campanas_com = text("""
                    SELECT
                        COUNT(telephone) AS contactos_predictivos
                    FROM tipificaciones_3
                    where type_interaction = 'out_pre'
                        AND date BETWEEN :fecha_inicio_com AND :fecha_fin_com
                """)
                query_campanas_com2 = text("""
                    SELECT
                        COUNT(telephone) AS contactos_predictivos
                    FROM tipificaciones_3
                    where type_interaction = 'out_pre'
                        AND date BETWEEN :fecha_inicio_com2 AND :fecha_fin_com2
                """)
                query_manual = text("""
                    SELECT 
                        COUNT(telephone) AS contactos_manual
                    FROM tipificaciones_3
                    WHERE type_interaction = 'outbound_ma'               
                        AND date BETWEEN :fecha_inicio AND :fecha_fin
                """)
                query_manual_com = text("""
                    SELECT 
                        COUNT(telephone) AS contactos_manual
                    FROM tipificaciones_3
                    WHERE type_interaction = 'outbound_ma'               
                        AND date BETWEEN :fecha_inicio_com AND :fecha_fin_com
                """)
                query_manual_com2 = text("""
                    SELECT 
                        COUNT(telephone) AS contactos_manual
                    FROM tipificaciones_3
                    WHERE type_interaction = 'outbound_ma'               
                        AND date BETWEEN :fecha_inicio_com2 AND :fecha_fin_com2
                """)
                query_no_conectadas = text("""
                    SELECT
                        COUNT(telephone) AS contactos_no_conectadas
                    FROM cdr_5
                    WHERE date BETWEEN :fecha_inicio AND :fecha_fin
                """)
                query_no_conectadas_com = text("""
                    SELECT
                        COUNT(telephone) AS contactos_no_conectadas
                    FROM cdr_5
                    WHERE date BETWEEN :fecha_inicio_com AND :fecha_fin_com
                """)
                query_no_conectadas_com2 = text("""
                    SELECT
                        COUNT(telephone) AS contactos_no_conectadas
                    FROM cdr_5
                    WHERE date BETWEEN :fecha_inicio_com2 AND :fecha_fin_com2
                """)

                result = conn.execute(query_cuenta_gestiones_id_act, {"fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin})
                total_gestiones = result.scalar() or 0
                result_com = conn.execute(query_cuenta_gestiones_id_act_com, {"fecha_inicio_com": fecha_inicio_com, "fecha_fin_com": fecha_fin_com})
                total_gestiones_com = result_com.scalar() or 0
                result_com_ant = conn.execute(query_cuenta_gestiones_id_act_com2, {"fecha_inicio_com2": fecha_inicio_com2, "fecha_fin_com2": fecha_fin_com2})
                total_gestiones_com2 = result_com_ant.scalar() or 0
                result_doc = conn.execute(query_cuenta_gestiones_doc_act, {"fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin})
                total_gestiones_doc = result_doc.scalar() or 0
                result_doc_com = conn.execute(query_cuenta_gestiones_doc_act_com, {"fecha_inicio_com": fecha_inicio_com, "fecha_fin_com": fecha_fin_com})
                total_gestiones_doc_com = result_doc_com.scalar() or 0  
                result_doc_com2 = conn.execute(query_cuenta_gestiones_doc_com2, {"fecha_inicio_com2": fecha_inicio_com2, "fecha_fin_com2": fecha_fin_com2})
                total_gestiones_doc_com2 = result_doc_com2.scalar() or 0
                result_sms = conn.execute(query_cuenta_sms_act, {"fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin})
                total_sms = result_sms.scalar() or 0
                result_sms_com = conn.execute(query_cuenta_sms_act_com, {"fecha_inicio_com": fecha_inicio_com, "fecha_fin_com": fecha_fin_com})
                total_sms_com = result_sms_com.scalar() or 0    
                result_sms_com2 = conn.execute(query_cuenta_sms_act_com2, {"fecha_inicio_com2": fecha_inicio_com2, "fecha_fin_com2": fecha_fin_com2})
                total_sms_com2 = result_sms_com2.scalar() or 0
                result_whatsapp = conn.execute(query_Whatsapp, {"fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin})
                total_whatsapp = result_whatsapp.scalar() or 0
                result_whatsapp_com = conn.execute(query_Whatsapp_com, {"fecha_inicio_com": fecha_inicio_com, "fecha_fin_com": fecha_fin_com})
                total_whatsapp_com = result_whatsapp_com.scalar() or 0
                result_whatsapp_com2 = conn.execute(query_Whatsapp_com2, {"fecha_inicio_com2": fecha_inicio_com2, "fecha_fin_com2": fecha_fin_com2})
                total_whatsapp_com2 = result_whatsapp_com2.scalar() or 0
                result_chat = conn.execute(query_chat_act, {"fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin})
                total_chat = result_chat.scalar() or 0
                result_chat_com = conn.execute(query_chat_act_com, {"fecha_inicio_com": fecha_inicio_com, "fecha_fin_com": fecha_fin_com})
                total_chat_com = result_chat_com.scalar() or 0
                result_chat_com2 = conn.execute(query_chat_act_com2, {"fecha_inicio_com2": fecha_inicio_com2, "fecha_fin_com2": fecha_fin_com2})
                total_chat_com2 = result_chat_com2.scalar() or 0
                result_campanas = conn.execute(query_campanas, {"fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin})
                total_campanas = result_campanas.scalar() or 0
                result_campanas_com = conn.execute(query_campanas_com, {"fecha_inicio_com": fecha_inicio_com, "fecha_fin_com": fecha_fin_com})
                total_campanas_com = result_campanas_com.scalar() or 0
                result_campanas_com2 = conn.execute(query_campanas_com2, {"fecha_inicio_com2": fecha_inicio_com2, "fecha_fin_com2": fecha_fin_com2})
                total_campanas_com2 = result_campanas_com2.scalar() or 0
                result_manual = conn.execute(query_manual, {"fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin})
                total_manual = result_manual.scalar() or 0 
                result_manual_com = conn.execute(query_manual_com, {"fecha_inicio_com": fecha_inicio_com, "fecha_fin_com": fecha_fin_com})
                total_manual_com = result_manual_com.scalar() or 0
                result_manual_com2 = conn.execute(query_manual_com2, {"fecha_inicio_com2": fecha_inicio_com2, "fecha_fin_com2": fecha_fin_com2})
                total_manual_com2 = result_manual_com2.scalar() or 0
                result_no_conectadas = conn.execute(query_no_conectadas, {"fecha_inicio": fecha_inicio, "fecha_fin": fecha_fin})
                total_no_conectadas = result_no_conectadas.scalar() or 0
                result_no_conectadas_com = conn.execute(query_no_conectadas_com, {"fecha_inicio_com": fecha_inicio_com, "fecha_fin_com": fecha_fin_com})
                total_no_conectadas_com = result_no_conectadas_com.scalar() or 0
                result_no_conectadas_com2 = conn.execute(query_no_conectadas_com2, {"fecha_inicio_com2": fecha_inicio_com2, "fecha_fin_com2": fecha_fin_com2})
                total_no_conectadas_com2 = result_no_conectadas_com2.scalar() or 0


                
                col1, col2, col3 = st.columns(3)
                with col1:
                    delta = ((total_gestiones - total_gestiones_com) / total_gestiones_com * 100) if total_gestiones_com != 0 else (100 if total_gestiones > 0 else 0)
                    if delta > 0:
                        flecha = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color = "#32882f"
                    elif delta < 0:
                        flecha = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color = "#a03838"
                    else: 
                        flecha = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color = "#2A2828"  
                    
                    st.markdown(f"""
                    <div style='background:#F5F5F5;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color}; border-right: 6px solid {color};'>
                    <div style='color:#053e01ff;font-size:1.7rem'>📚 Total de Gestiones</div>
                    <div style='font-size:3rem'>{total_gestiones:,}</div>
                    <div style='color:{color};font-size:1.2rem'>
                            {flecha} {abs(delta):.2f}%
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    delta2 = ((total_gestiones_com - total_gestiones_com2) / total_gestiones_com2 * 100) if total_gestiones_com2 != 0 else (100 if total_gestiones_com > 0 else 0)
                    if delta2 > 0:
                        flecha2 = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color2 = "#32882f"
                    elif delta2 < 0:
                        flecha2 = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color2 = "#a03838"
                    else: 
                        flecha2 = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color2 = "#2A2828"
                    st.markdown(f"""
                    <div style='background:#F5F5F5;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color2}; border-right: 6px solid {color2};'>
                    <div style='color:#053e01ff;font-size:1.7rem'>📚 Período Anterior</div>
                    <div style='font-size:3rem'>{total_gestiones_com:,}</div>
                    <div style='color:{color2};font-size:1.2rem'>
                            {flecha2} {abs(delta2):.2f}%
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    st.metric("Gestiones - Hace 2 Períodos", f"{total_gestiones_com2:,}")
                
                with col2:
                    delta_doc = ((total_gestiones_doc - total_gestiones_doc_com) / total_gestiones_doc_com * 100) if total_gestiones_doc_com != 0 else (100 if total_gestiones_doc > 0 else 0)
                    if delta_doc > 0:
                        flecha_doc = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color_doc = "#32882f"
                    elif delta_doc < 0:
                        flecha_doc = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color_doc = "#a03838"
                    else: 
                        flecha_doc = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color_doc = "#2A2828"
                    st.markdown(f"""
                    <div style='background:#E0E0E0;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color_doc}; border-right: 6px solid {color_doc};'>
                    <div style='color:#053e01ff;font-size:1.7rem'>📋 Total de Documentos con Gestión</div>
                    <div style='font-size:3rem'>{total_gestiones_doc:,}</div>
                    <div style='color:{color_doc};font-size:1.2rem'>
                            {flecha_doc} {abs(delta_doc):.2f}%
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    delta2_doc = ((total_gestiones_doc_com - total_gestiones_doc_com2) / total_gestiones_doc_com2 * 100) if total_gestiones_doc_com2 != 0 else (100 if total_gestiones_doc_com > 0 else 0)
                    if delta2_doc > 0:
                        flecha2_doc = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color2_doc = "#32882f"
                    elif delta2_doc < 0:
                        flecha2_doc = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color2_doc = "#a03838"
                    else: 
                        flecha2_doc = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color2_doc = "#2A2828"
                    st.markdown(f"""
                    <div style='background:#E0E0E0;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color2_doc}; border-right: 6px solid {color2_doc};'>
                    <div style='color:#053e01ff;font-size:1.7rem'>📋 Período Anterior</div>
                    <div style='font-size:3rem'>{total_gestiones_doc_com:,}</div>
                    <div style='color:{color2_doc};font-size:1.2rem'>
                            {flecha2_doc} {abs(delta2_doc):.2f}%
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    st.metric("Documentos con Gestión - Hace 2 Períodos", f"{total_gestiones_doc_com2:,}")

                st.markdown("---")

                col1, col2, col3, col4, col5, col6 = st.columns(6)

                with col1:
                    delta_sms = ((total_sms - total_sms_com) / total_sms_com * 100) if total_sms_com != 0 else (100 if total_sms > 0 else 0)
                    if delta_sms > 0:
                        flecha_sms = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color_sms = "#32882f"
                    elif delta_sms < 0:
                        flecha_sms = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color_sms = "#a03838"
                    else:
                        flecha_sms = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color_sms = "#2A2828"
                    st.markdown(f"""
                    <div style='background:#F5F5F5;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color_sms}; border-right: 6px solid {color_sms};'>
                    <div style='color:#053e01ff;font-size:1.5rem'>📲 SMS Enviados</div>
                    <div style='font-size:3rem'>{total_sms:,}</div>
                    <div style='color:{color_sms};font-size:1.2rem'>
                            {flecha_sms} {abs(delta_sms):.2f}%
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    delta2_sms = ((total_sms_com - total_sms_com2) / total_sms_com2 * 100) if total_sms_com2 != 0 else (100 if total_sms_com > 0 else 0)
                    if delta2_sms > 0:
                        flecha2_sms = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color2_sms = "#32882f"
                    elif delta2_sms < 0:
                        flecha2_sms = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color2_sms = "#a03838"
                    else:
                        flecha2_sms = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color2_sms = "#2A2828"
                    st.markdown(f"""
                    <div style='background:#F5F5F5;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color2_sms}; border-right: 6px solid {color2_sms};'>
                    <div style='color:#053e01ff;font-size:1.5rem'>📲 Período Anterior</div>
                    <div style='font-size:3rem'>{total_sms_com:,}</div>
                    <div style='color:{color2_sms};font-size:1.2rem'>
                            {flecha2_sms} {abs(delta2_sms):.2f}%
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    st.metric("SMS Enviados - Hace 2 Períodos", f"{total_sms_com2:,}")
                with col2:
                    delta_whatsapp = ((total_whatsapp - total_whatsapp_com) / total_whatsapp_com * 100) if total_whatsapp_com != 0 else (100 if total_whatsapp > 0 else 0)
                    if delta_whatsapp > 0:
                        flecha_whatsapp = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color_whatsapp = "#32882f"
                    elif delta_whatsapp < 0:
                        flecha_whatsapp = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color_whatsapp = "#a03838"
                    else:
                        flecha_whatsapp = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color_whatsapp = "#2A2828"
                    st.markdown(f"""
                    <div style='background:#E0E0E0;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color_whatsapp}; border-right: 6px solid {color_whatsapp};'>
                    <div style='color:#053e01ff;font-size:1.5rem'><img src="data:image/png;base64,{img_base64}" style="width:35px"> Gestión WhatsApp</div>
                    <div style='font-size:3rem'>{total_whatsapp:,}</div>
                    <div style='color:{color_whatsapp};font-size:1.2rem'>
                            {flecha_whatsapp} {abs(delta_whatsapp):.2f}%
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    delta2_whatsapp = ((total_whatsapp_com - total_whatsapp_com2) / total_whatsapp_com2 * 100) if total_whatsapp_com2 != 0 else (100 if total_whatsapp_com > 0 else 0)
                    if delta2_whatsapp > 0:
                        flecha2_whatsapp = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color2_whatsapp = "#32882f"
                    elif delta2_whatsapp < 0:
                        flecha2_whatsapp = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color2_whatsapp = "#a03838"
                    else:
                        flecha2_whatsapp = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color2_whatsapp = "#2A2828"
                    st.markdown(f"""
                    <div style='background:#E0E0E0;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color2_whatsapp}; border-right: 6px solid {color2_whatsapp};'>
                    <div style='color:#053e01ff;font-size:1.5rem'><img src="data:image/png;base64,{img_base64}" style="width:35px"> Período Anterior</div>
                    <div style='font-size:3rem'>{total_whatsapp_com:,}</div>
                    <div style='color:{color2_whatsapp};font-size:1.2rem'>
                            {flecha2_whatsapp} {abs(delta2_whatsapp):.2f}%
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    st.metric("Gestiones por WhatsApp - Hace 2 Períodos", f"{total_whatsapp_com2:,}")
                with col3:
                    delta_chat = ((total_chat - total_chat_com) / total_chat_com * 100) if total_chat_com != 0 else (100 if total_chat > 0 else 0)
                    if delta_chat > 0:
                        flecha_chat = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color_chat = "#32882f"
                    elif delta_chat < 0:
                        flecha_chat = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color_chat = "#a03838"
                    else:
                        flecha_chat = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color_chat = "#2A2828"
                    st.markdown(f"""
                    <div style='background:#F5F5F5;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color_chat}; border-right: 6px solid {color_chat};'>
                    <div style='color:#053e01ff;font-size:1.5rem'>📱 Gestiones por Chat</div>
                    <div style='font-size:3rem'>{total_chat:,}</div>
                    <div style='color:{color_chat};font-size:1.2rem'>
                            {flecha_chat} {abs(delta_chat):.2f}%
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    delta2_chat = ((total_chat_com - total_chat_com2) / total_chat_com2 * 100) if total_chat_com2 != 0 else (100 if total_chat_com > 0 else 0)
                    if delta2_chat > 0:
                        flecha2_chat = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color2_chat = "#32882f"
                    elif delta2_chat < 0:
                        flecha2_chat = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color2_chat = "#a03838"
                    else:
                        flecha2_chat = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color2_chat = "#2A2828"
                    st.markdown(f"""
                    <div style='background:#F5F5F5;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color2_chat}; border-right: 6px solid {color2_chat};'>
                    <div style='color:#053e01ff;font-size:1.5rem'>📱 Período Anterior</div>
                    <div style='font-size:3rem'>{total_chat_com:,}</div>
                    <div style='color:{color2_chat};font-size:1.2rem'>
                            {flecha2_chat} {abs(delta2_chat):.2f}%
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    st.metric("Gestiones por Chat - Hace 2 Períodos", f"{total_chat_com2:,}")
                with col4:
                    delta_campanas = ((total_campanas - total_campanas_com) / total_campanas_com * 100) if total_campanas_com != 0 else (100 if total_campanas > 0 else 0)
                    if delta_campanas > 0:
                        flecha_campanas = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color_campanas = "#32882f"
                    elif delta_campanas < 0:
                        flecha_campanas = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color_campanas = "#a03838"
                    else:
                        flecha_campanas = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color_campanas = "#2A2828"
                    st.markdown(f"""
                    <div style='background:#E0E0E0;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color_campanas}; border-right: 6px solid {color_campanas};'>
                    <div style='color:#053e01ff;font-size:1.5rem'>🤖 Contactos Predictivos</div>
                    <div style='font-size:3rem'>{total_campanas:,}</div>
                    <div style='color:{color_campanas};font-size:1.2rem'>
                            {flecha_campanas} {abs(delta_campanas):.2f}%
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    delta2_campanas = ((total_campanas_com - total_campanas_com2) / total_campanas_com2 * 100) if total_campanas_com2 != 0 else (100 if total_campanas_com > 0 else 0)
                    if delta2_campanas > 0:
                        flecha2_campanas = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color2_campanas = "#32882f"
                    elif delta2_campanas < 0:
                        flecha2_campanas = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color2_campanas = "#a03838"
                    else:
                        flecha2_campanas = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color2_campanas = "#2A2828"
                    st.markdown(f"""
                    <div style='background:#E0E0E0;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color2_campanas}; border-right: 6px solid {color2_campanas};'>
                    <div style='color:#053e01ff;font-size:1.5rem'>🤖 Período Anterior</div>
                    <div style='font-size:3rem'>{total_campanas_com:,}</div>
                    <div style='color:{color2_campanas};font-size:1.2rem'>
                            {flecha2_campanas} {abs(delta2_campanas):.2f}%
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    st.metric("Contactos Predictivos - Hace 2 Períodos", f"{total_campanas_com2:,}")
                with col5:
                    delta_manual = ((total_manual - total_manual_com) / total_manual_com * 100) if total_manual_com != 0 else (100 if total_manual > 0 else 0)
                    if delta_manual > 0:
                        flecha_manual = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color_manual = "#32882f"
                    elif delta_manual < 0:
                        flecha_manual = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color_manual = "#a03838"
                    else:
                        flecha_manual = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color_manual = "#2A2828"
                    st.markdown(f"""
                    <div style='background:#F5F5F5;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color_manual}; border-right: 6px solid {color_manual};'>
                    <div style='color:#053e01ff;font-size:1.5rem'>📞 Contactos Manuales</div>
                    <div style='font-size:3rem'>{total_manual:,}</div>
                    <div style='color:{color_manual};font-size:1.2rem'>
                            {flecha_manual} {abs(delta_manual):.2f}%
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    delta2_manual = ((total_manual_com - total_manual_com2) / total_manual_com2 * 100) if total_manual_com2 != 0 else (100 if total_manual_com > 0 else 0)
                    if delta2_manual > 0:
                        flecha2_manual = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color2_manual = "#32882f"
                    elif delta2_manual < 0:
                        flecha2_manual = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color2_manual = "#a03838"
                    else:
                        flecha2_manual = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color2_manual = "#2A2828"
                    st.markdown(f"""
                    <div style='background:#F5F5F5;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color2_manual}; border-right: 6px solid {color2_manual};'>
                    <div style='color:#053e01ff;font-size:1.5rem'>📞 Período Anterior</div>
                    <div style='font-size:3rem'>{total_manual_com:,}</div>
                    <div style='color:{color2_manual};font-size:1.2rem'>
                            {flecha2_manual} {abs(delta2_manual):.2f}%
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    st.metric("Contactos Manuales - Hace 2 Períodos", f"{total_manual_com2:,}")
                with col6:
                    delta_no_conectadas = ((total_no_conectadas - total_no_conectadas_com) / total_no_conectadas_com * 100) if total_no_conectadas_com != 0 else (100 if total_no_conectadas > 0 else 0)
                    if delta_no_conectadas > 0:
                        flecha_no_conectadas = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color_no_conectadas = "#a03838"
                    elif delta_no_conectadas < 0:
                        flecha_no_conectadas = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color_no_conectadas = "#32882f"
                    else:
                        flecha_no_conectadas = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color_no_conectadas = "#2A2828"
                    st.markdown(f"""
                    <div style='background:#E0E0E0;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color_no_conectadas}; border-right: 6px solid {color_no_conectadas};'>
                    <div style='color:#053e01ff;font-size:1.5rem'>🚫 Llamadas No Conectados</div>
                    <div style='font-size:3rem'>{total_no_conectadas:,}</div>
                    <div style='color:{color_no_conectadas};font-size:1.2rem'>
                            {flecha_no_conectadas} {abs(delta_no_conectadas):.2f}%
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    delta2_no_conectadas = ((total_no_conectadas_com - total_no_conectadas_com2) / total_no_conectadas_com2 * 100) if total_no_conectadas_com2 != 0 else (100 if total_no_conectadas_com > 0 else 0)
                    if delta2_no_conectadas > 0:
                        flecha2_no_conectadas = f"<img src='data:image/png;base64,{img_base64_down_arrow}' style='width:24px'>"
                        color2_no_conectadas = "#a03838"
                    elif delta2_no_conectadas < 0:
                        flecha2_no_conectadas = f"<img src='data:image/png;base64,{img_base64_up_arrow}' style='width:24px'>"
                        color2_no_conectadas = "#32882f"
                    else:
                        flecha2_no_conectadas = f"<img src='data:image/png;base64,{img_base64_line}' style='width:24px'>"
                        color2_no_conectadas = "#2A2828"
                    st.markdown(f"""
                    <div style='background:#E0E0E0;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color2_no_conectadas}; border-right: 6px solid {color2_no_conectadas};'>
                    <div style='color:#053e01ff;font-size:1.5rem'>🚫 Período Anterior</div>
                    <div style='font-size:3rem'>{total_no_conectadas_com:,}</div>
                    <div style='color:{color2_no_conectadas};font-size:1.2rem'>
                            {flecha2_no_conectadas} {abs(delta2_no_conectadas):.2f}%
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    st.metric("Contactos No Conectados - Hace 2 Períodos", f"{total_no_conectadas_com2:,}")
            st.markdown("---")

        except Exception as e:
            st.error(f"Error al consultar la información de gestiones: {e}")
            return

# ======================================================================
# FLUJO PRINCIPAL CENTRALIZADO
# ======================================================================

def main():
    st.markdown("<h1 class='section-title'>Análisis de Productividad</h1>", unsafe_allow_html=True)
    st.markdown("---")
    
    if "modulo_seleccionado" not in st.session_state:
        st.session_state["modulo_seleccionado"] = " 📊 Gestiones"
    
    MODULOS = {
        " 📋 Cartera": "📋",
        " 📊 Gestiones": "📊"
    }
    modulo = option_menu(
        menu_title=None,
        options=list(MODULOS.keys()),
        icons=list(MODULOS.values()),
        orientation="horizontal",
        default_index= list(MODULOS.keys()).index(st.session_state["modulo_seleccionado"])
    )

    st.session_state["modulo_seleccionado"] = modulo

    st.markdown("---")
    # Filtros de fecha
    st.subheader("Filtros de Fecha")
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
    st.info(f"Rango seleccionado: {fecha_inicio} a {fecha_fin}")
    # Calcular rango de comparación del mes anterior
    fecha_inicio_comparacion = fecha_inicio - relativedelta(months=1)
    fecha_fin_comparacion = fecha_fin - relativedelta(months=1)
    st.info(f"Rango de comparación: {fecha_inicio_comparacion} a {fecha_fin_comparacion}")

    # Ejecución según selección
    if modulo == " 📋 Cartera":
        mostrar_cartera(fecha_inicio, fecha_fin, db)
    elif modulo == " 📊 Gestiones":
        mostrar_gestiones(fecha_inicio, fecha_fin, db)



if __name__ == "__main__":
    main()

