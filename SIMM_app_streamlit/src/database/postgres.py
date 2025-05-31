# -*- coding: utf-8 -*-
import os
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import streamlit as st

load_dotenv()

class DatabaseManager:
    """Clase para manejar conexiones a múltiples bases de datos"""
    
    @staticmethod
    def get_connection(db_name='SIMM'):
        """Obtiene conexión directa con psycopg2"""
        try:
            conn = psycopg2.connect(
                host=os.getenv(f"DB_{db_name}_HOST"),
                port=os.getenv(f"DB_{db_name}_PORT"),
                user=os.getenv(f"DB_{db_name}_USER"),
                password=os.getenv(f"DB_{db_name}_PASSWORD"),
                database=os.getenv(f"DB_{db_name}_NAME")
            )
            return conn
        except Exception as e:
            st.error(f"❌ Error conectando a {db_name}: {str(e)}")
            raise

    @staticmethod
    def get_engine(db_name='SIMM'):
        """Obtiene engine SQLAlchemy"""
        try:
            engine = create_engine(
                f"postgresql+psycopg2://{os.getenv(f'DB_{db_name}_USER')}:{os.getenv(f'DB_{db_name}_PASSWORD')}@"
                f"{os.getenv(f'DB_{db_name}_HOST')}:{os.getenv(f'DB_{db_name}_PORT')}/{os.getenv(f'DB_{db_name}_NAME')}"
            )
            # Prueba simple de conexión usando text()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return engine
        except Exception as e:
            st.error(f"❌ Error creando engine para {db_name}: {str(e)}")
            raise

# Funciones legacy para compatibilidad (pueden eliminarse luego)
def get_connection():
    """Función legacy para compatibilidad con código existente"""
    return DatabaseManager.get_connection('SIMM')

def get_engine():
    """Función legacy para compatibilidad con código existente"""
    return DatabaseManager.get_engine('SIMM')

def refresh_materialized_views():
    """Función legacy para compatibilidad con código existente"""
    conn = DatabaseManager.get_connection('SIMM')
    try:
        cursor = conn.cursor()
        cursor.execute("REFRESH MATERIALIZED VIEW mv_id_gestiones")
        conn.commit()
    finally:
        conn.close()

# Test de conexión
if __name__ == "__main__":
    for db in ['SIMM', 'ANDES']:
        try:
            conn = DatabaseManager.get_connection(db)
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            print(f"{db} PostgreSQL version:", cursor.fetchone())
            conn.close()
        except Exception as e:
            print(f"Error conectando a {db}: {str(e)}")