# -*- coding: utf-8 -*-
import os
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import streamlit as st
from typing import Optional, Dict, Any

load_dotenv()

class DatabaseManager:
    """Clase para manejar conexiones a múltiples bases de datos"""
    
    @staticmethod
    def _validar_configuracion(db_name: str) -> bool:
        """Valida que todas las variables de entorno necesarias estén configuradas"""
        required_vars = [
            f'DB_{db_name}_HOST',
            f'DB_{db_name}_PORT',
            f'DB_{db_name}_USER',
            f'DB_{db_name}_PASSWORD',
            f'DB_{db_name}_NAME'
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            st.error(f"❌ Variables de entorno faltantes para {db_name}: {', '.join(missing_vars)}")
            return False
        return True
    
    @staticmethod
    def get_connection(db_name: str = 'SIMM') -> Optional[psycopg2.extensions.connection]:
        """Obtiene conexión directa con psycopg2 con manejo robusto de errores"""
        if not DatabaseManager._validar_configuracion(db_name):
            return None
            
        try:
            conn = psycopg2.connect(
                host=os.getenv(f"DB_{db_name}_HOST"),
                port=os.getenv(f"DB_{db_name}_PORT"),
                user=os.getenv(f"DB_{db_name}_USER"),
                password=os.getenv(f"DB_{db_name}_PASSWORD"),
                database=os.getenv(f"DB_{db_name}_NAME"),
                connect_timeout=10
            )
            conn.autocommit = False
            return conn
        except psycopg2.OperationalError as e:
            st.error(f"❌ Error de conexión a {db_name}: {str(e)}")
            return None
        except Exception as e:
            st.error(f"❌ Error inesperado conectando a {db_name}: {str(e)}")
            return None

    @staticmethod
    def get_engine(db_name: str = 'SIMM') -> Optional[Any]:
        """Obtiene engine SQLAlchemy con manejo robusto de errores"""
        if not DatabaseManager._validar_configuracion(db_name):
            return None
            
        try:
            connection_string = (
                f"postgresql+psycopg2://{os.getenv(f'DB_{db_name}_USER')}:"
                f"{os.getenv(f'DB_{db_name}_PASSWORD')}@"
                f"{os.getenv(f'DB_{db_name}_HOST')}:"
                f"{os.getenv(f'DB_{db_name}_PORT')}/"
                f"{os.getenv(f'DB_{db_name}_NAME')}"
            )
            
            engine = create_engine(connection_string, pool_pre_ping=True)
            
            # Prueba simple de conexión - Versión corregida
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).fetchone()
                if result[0] != 1:
                    raise ValueError("La prueba de conexión no devolvió el resultado esperado")
                    
            return engine
        except Exception as e:
            st.error(f"❌ Error creando engine para {db_name}: {str(e)}")
            return None
    @staticmethod
    def test_connection(db_name: str = 'SIMM') -> Dict[str, Any]:
        """Prueba la conexión y devuelve un reporte detallado"""
        result = {
            'db_name': db_name,
            'connection_success': False,
            'engine_success': False,
            'error': None,
            'version': None
        }
        
        # Probar conexión directa
        try:
            conn = DatabaseManager.get_connection(db_name)
            if conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT version();")
                    result['version'] = cursor.fetchone()[0]
                    result['connection_success'] = True
                conn.close()
        except Exception as e:
            result['error'] = str(e)
        
        # Probar engine SQLAlchemy
        try:
            engine = DatabaseManager.get_engine(db_name)
            if engine:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                result['engine_success'] = True
        except Exception as e:
            if not result['error']:
                result['error'] = str(e)
        
        return result

# Funciones legacy para compatibilidad
def get_connection():
    """Función legacy para compatibilidad con código existente"""
    return DatabaseManager.get_connection('SIMM')

def get_engine():
    """Función legacy para compatibilidad con código existente"""
    return DatabaseManager.get_engine('SIMM')

def refresh_materialized_views():
    """Función legacy para compatibilidad con código existente"""
    conn = DatabaseManager.get_connection('SIMM')
    if conn is None:
        st.error("No se pudo conectar a la base de datos para refrescar vistas materializadas")
        return False
        
    try:
        cursor = conn.cursor()
        cursor.execute("REFRESH MATERIALIZED VIEW mv_id_gestiones")
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        st.error(f"Error refrescando vistas materializadas: {str(e)}")
        return False
    finally:
        conn.close()