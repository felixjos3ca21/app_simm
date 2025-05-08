import streamlit as st

# ==============================================
# CONFIGURACIÓN GLOBAL 
# ==============================================
st.set_page_config(
    page_title="SIAMM",
    page_icon="src/utils/favicon-114x114.png",
    layout="wide"
)
st.image("src/utils/logo-andesbpo-359x143.png", width=250)
# Configuración global (compartida para todas las páginas).png", width=100)
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