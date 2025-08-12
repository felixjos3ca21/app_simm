import streamlit as st
from assets.fondo import set_background
from src.database.postgres import DatabaseManager
import pathlib

# ==============================================
# CONFIGURACIÓN GLOBAL 
# ==============================================
st.set_page_config(
    page_title="SIAMM",
    page_icon="assets/images/favicon-114x114.png",
    layout="wide"
)

st.image("assets/images/logo-andesbpo-359x143.png", width=450)
set_background("assets/images/bg-seccion.png")


css_path = pathlib.Path("assets/css/global.css")
if css_path.exists():
    with open(css_path, encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# ==============================================
# FUNCIONES AUXILIARES
# ==============================================
@st.cache_resource
def get_engines():
    """Obtiene y cachea los engines de conexión para ambas bases"""
    engines = {
        'SIMM': DatabaseManager.get_engine('SIMM'),
    }
    return engines

def test_db_connections():
    """Prueba las conexiones a ambas bases de datos"""
    col1, col2 = st.columns(2)
    with col1:
        with st.expander("🔍 Probar conexiones a bases de datos"):
            if st.button("Probar conexión a SIMM"):
                try:
                    with DatabaseManager.get_connection('SIMM') as conn:
                        conn.cursor().execute("SELECT 1")
                    st.success("✅ Conexión a SIMM exitosa")
                except Exception as e:
                    st.error(f"❌ Error conectando a SIMM: {str(e)}")

# ==============================================
# CONTENIDO PRINCIPAL DE LA PÁGINA
# ==============================================
def main():
    st.markdown("<h1 class='section-title'> Bienvenido al Sistema de Información de ANDES - SIMM </h1>", unsafe_allow_html=True)
    st.markdown("""
        ### Funcionalidades principales:
        - 🕵️‍♂️ **Consultas**: Análisis detallado de gestiones existentes
        - ✏️ **Actualización**: Modificación y carga de nuevos registros
        - 🔀 **Cruce de Bases**: Comparación de datos entre SIMM y Andes-Wolkvo
        - 📈 **Productividad**: Analisis temporal de las bases gestionadas
        - 🗃️ **Cartera**: visualización de cartera de clientes y gestiones
    """)
    
    # Mostrar estado de conexiones
    test_db_connections()

if __name__ == "__main__":
    main()
