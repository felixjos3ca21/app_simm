import streamlit as st
from src.utils.fondo import set_background


# ==============================================
# CONFIGURACIÓN GLOBAL 
# ==============================================
st.set_page_config(
    page_title="SIAMM",
    page_icon="src/utils/favicon-114x114.png",
    layout="wide"
)

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
    
# Verificación de conexión (opcional)
    if st.checkbox("🔌 Mostrar estado de conexión a PostgreSQL"):
        from src.database.postgres import get_connection
        try:
            conn = get_connection()
            st.success("✅ Conexión exitosa a PostgreSQL")
            conn.close()
        except Exception as e:
            st.error(f"❌ Error de conexión: {e}")

if __name__ == "__main__":
    main()