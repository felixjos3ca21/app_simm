import streamlit as st
from src.database.postgres import get_engine
from assets.fondo import set_background
import pathlib
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
from sqlalchemy import text
from streamlit_option_menu import option_menu

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
                if not cantidad_bases_unicas_com == 0:
                    delta_pct = ((cantidad_bases_unicas - cantidad_bases_unicas_com) / cantidad_bases_unicas_com) * 100
                if not cantidad_bases_unicas_com2 == 0:
                    delta_pct2 = ((cantidad_bases_unicas_com - cantidad_bases_unicas_com2) / cantidad_bases_unicas_com2) * 100
            
                flecha = "🟢" if delta_pct > 0 else "🔴"
                color = "#32882f" if delta_pct > 0 else "#a03838"
                st.markdown(f"""
                    <div style='background:#F5F5F5;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color}; border-right: 6px solid {color};'>
                    <div style='color:#053e01ff;font-size:1.7rem'>Cantidad de Bases Asigandas</div>
                    <div style='font-size:3rem'>{cantidad_bases_unicas:,}</div>
                    <div style='color:{color};font-size:1.2rem'>
                        {flecha} {(delta_pct):.2f}%
                    </div>
                    
                    </div>
                    """, unsafe_allow_html=True)
                
                flecha = "🟢" if delta_pct2 > 0 else "🔴"
                color = "#32882f" if delta_pct2 > 0 else "#a03838"
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
                if kpi_com[0] != 0:
                    delta_pct = ((kpi[0] - kpi_com[0]) / kpi_com[0]) * 100
                else:
                    delta_pct = 0
                if kpi_com2[0] != 0:
                    delta_pct2 = ((kpi_com[0] - kpi_com2[0]) / kpi_com2[0]) * 100
                else:
                    delta_pct2 = 0
                
                
                delta = delta_pct
                flecha = "🟢" if delta_pct > 0 else "🔴"
                color = "#32882f" if delta_pct > 0 else "#a03838"
                st.markdown(f"""
                    <div style='background:#E0E0E0;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color}; border-right: 6px solid {color};'>
                    <div style='color:#053e01ff;font-size:1.7rem'>Registros Totales</div>
                    <div style='font-size:3rem'>{f"{kpi[0]:,}".replace(",",".")}</div>
                    <div style='color:{color};font-size:1.2rem'>
                        {flecha} {abs(delta):.2f}%
                    </div>
                    
                    </div>
                    """, unsafe_allow_html=True)
                
                delta = delta_pct2
                flecha = "🟢" if delta_pct2 > 0 else "🔴"
                color = "#32882f" if delta_pct2 > 0 else "#a03838"

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
                if kpi_com[1] != 0:
                    delta_pct = ((kpi[1] - kpi_com[1]) / kpi_com[1]) * 100
                else:
                    delta_pct = 0
                if kpi_com2[1] != 0:
                    delta_pct2 = ((kpi_com[1] - kpi_com2[1]) / kpi_com2[1]) * 100
                else:
                    delta_pct2 = 0
                

                delta = delta_pct
                flecha = "🟢" if delta_pct > 0 else "🔴"
                color = "#32882f" if delta_pct > 0 else "#a03838"
                st.markdown(f"""
                    <div style='background:#F5F5F5;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color}; border-right: 6px solid {color};'>
                    <div style='color:#053e01ff;font-size:1.7rem'>Documentos Únicos</div>
                    <div style='font-size:3rem'>{f"{kpi[1]:,}".replace(",",".")}</div>
                    <div style='color:{color};font-size:1.2rem'>
                        {flecha} {abs(delta):.2f}%
                    </div>
                    </div>
                """, unsafe_allow_html=True)

                delta = delta_pct2
                flecha = "🟢" if delta_pct2 > 0 else "🔴"    
                color = "#32882f" if delta_pct2 > 0 else "#a03838"
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
                if kpi_com[2] != 0:
                    delta_pct = ((kpi[2] - kpi_com[2]) / kpi_com[2]) * 100
                else:
                    delta_pct = 0
                if kpi_com2[2] != 0:
                    delta_pct2 = ((kpi_com[2] - kpi_com2[2]) / kpi_com2[2]) * 100
                

                delta = delta_pct
                flecha = "🟢" if delta_pct > 0 else "🔴"
                color = "#32882f" if delta_pct > 0 else "#a03838"
                st.markdown(f"""
                    <div style='background:#E0E0E0;padding:1rem;border-radius:10px;text-align:left; border-bottom: 6px solid {color}; border-right: 6px solid {color};'>
                    <div style='color:#053e01ff;font-size:1.7rem'>Monto Total</div>
                    <div style='font-size:3rem'>{f"${kpi[2]:,.0f}".replace(",",".")}</div>
                    <div style='color:{color};font-size:1.2rem'>
                        {flecha} {abs(delta):.2f}%
                    </div>
                    
                    </div>
                    """, unsafe_allow_html=True)
                delta = delta_pct2
                flecha = "🟢" if delta_pct2 > 0 else "🔴"
                color = "#32882f" if delta_pct2 > 0 else "#a03838"
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

def mostrar_gestiones():
    st.info("Funcionalidad de gestiones próximamente disponible.")

# ======================================================================
# FLUJO PRINCIPAL CENTRALIZADO
# ======================================================================

def main():
    st.markdown("<h1 class='section-title'>Análisis de Productividad</h1>", unsafe_allow_html=True)
    st.markdown("---")

    MODULOS = {
        " 📋 Cartera": "📋",
        " 📊 Gestiones": "📊"
    }
    modulo = option_menu(
        menu_title=None,
        options=list(MODULOS.keys()),
        icons=list(MODULOS.values()),
        orientation="horizontal"
    )
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
        mostrar_gestiones()

if __name__ == "__main__":
    main()

