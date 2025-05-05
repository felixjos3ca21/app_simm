import streamlit as st

# Configuración global (compartida para todas las páginas)
st.set_page_config(
    page_title="SIMM Dashboard",
    page_icon="📊",
    layout="wide"
)

# Página de inicio
def main():
    st.title("Bienvenido al Sistema de Información de ANDES - SIMM")
    st.markdown("""
        **Selecciona una página en el menú lateral:**
        - 🕵️‍♂️ `Consultas`: Análisis de gestiones existentes.
        - ✏️ `Actualización`: Modificar o añadir registros.
    """)
    
    # Info de conexión (opcional)
    if st.checkbox("Mostrar estado de conexión a PostgreSQL"):
        from src.database.postgres import get_connection
        try:
            conn = get_connection()
            st.success("✅ Conexión exitosa a PostgreSQL")
            conn.close()
        except Exception as e:
            st.error(f"❌ Error de conexión: {e}")

if __name__ == "__main__":
    main()