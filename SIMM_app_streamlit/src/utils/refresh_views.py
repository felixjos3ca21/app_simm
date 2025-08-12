# -*- coding: utf-8 -*-
from datetime import datetime
import logging
import time
from argparse import ArgumentParser
from src.database.postgres import get_connection, refresh_materialized_views

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('refresh_mv.log'),
        logging.StreamHandler()
    ]
)

def main(full_refresh=False):
    """
    Función principal para refrescar vistas materializadas
    
    Args:
        full_refresh (bool): Si True, hace un refresco completo (REFRESH MATERIALIZED VIEW)
                            Si False, hace un refresco incremental (REFRESH MATERIALIZED VIEW CONCURRENTLY)
    """
    start_time = time.time()
    conn = None
    
    try:
        logging.info(f"Iniciando refresco de vistas a las {datetime.now()}")
        logging.info(f"Modo: {'COMPLETO' if full_refresh else 'INCREMENTAL'}")
        
        conn = get_connection()
        refresh_materialized_views(conn, full_refresh=full_refresh)
        
        elapsed_time = time.time() - start_time
        logging.info(f"Vistas actualizadas correctamente. Tiempo: {elapsed_time:.2f} segundos")
        return True
        
    except Exception as e:
        logging.error(f"Error al refrescar vistas: {str(e)}")
        return False
        
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Configuración de argumentos de línea de comandos
    parser = ArgumentParser(description='Script para refrescar vistas materializadas')
    parser.add_argument(
        '--full',
        action='store_true',
        help='Ejecuta un refresco completo (REFRESH MATERIALIZED VIEW) en lugar de incremental'
    )
    args = parser.parse_args()
    
    # Ejecutar refresco
    success = main(full_refresh=args.full)
    
    # Terminar con código de salida apropiado
    exit(0 if success else 1)