import streamlit as st
from src.utils.fondo import set_background
from src.database.postgres import DatabaseManager

# ==============================================
# CONFIGURACIÓN GLOBAL 
# ==============================================
st.set_page_config(
    page_title="SIAMM",
    page_icon="src/utils/favicon-114x114.png",
    layout="wide"
)

# Estilos CSS (se mantienen igual)
st.markdown("""
    <style>
    /* Sidebar */
    [data-testid=stSidebar] {
        background-color: #A6DEB7 !important;
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

st.image("src/utils/logo-andesbpo-359x143.png", width=250)
set_background("src/utils/bg-seccion.png")

# ==============================================
# FUNCIONES AUXILIARES
# ==============================================
@st.cache_resource
def get_engines():
    """Obtiene y cachea los engines de conexión para ambas bases"""
    engines = {
        'SIMM': DatabaseManager.get_engine('SIMM'),
        'ANDES': DatabaseManager.get_engine('ANDES')
    }
    return engines

def test_db_connections():
    """Prueba las conexiones a ambas bases de datos"""
    with st.expander("🔍 Probar conexiones a bases de datos"):
        cols = st.columns(2)
        
        with cols[0]:
            if st.button("Probar conexión a SIMM"):
                try:
                    with DatabaseManager.get_connection('SIMM') as conn:
                        conn.cursor().execute("SELECT 1")
                    st.success("✅ Conexión a SIMM exitosa")
                except Exception as e:
                    st.error(f"❌ Error conectando a SIMM: {str(e)}")
        
        with cols[1]:
            if st.button("Probar conexión a Andes-Wolkvox"):
                try:
                    with DatabaseManager.get_connection('ANDES') as conn:
                        conn.cursor().execute("SELECT 1")
                    st.success("✅ Conexión a Andes-Wolkvox exitosa")
                except Exception as e:
                    st.error(f"❌ Error conectando a Andes-Wolkvox: {str(e)}")

# ==============================================
# CONTENIDO PRINCIPAL DE LA PÁGINA
# ==============================================
def main():
    st.title("Bienvenido al Sistema de Información de ANDES - SIMM")
    st.markdown("""
        ### Funcionalidades principales:
        - 🕵️‍♂️ **Consultas**: Análisis detallado de gestiones existentes
        - ✏️ **Actualización**: Modificación y carga de nuevos registros
        - 📊 **Dashboards**: Reportes ejecutivos con indicadores clave
    """)
    
    # Obtener conexiones (se cachean automáticamente)
    engines = get_engines()
    
    # Mostrar estado de conexiones
    test_db_connections()

if __name__ == "__main__":
    main()