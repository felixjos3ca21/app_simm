# -*- coding: utf-8 -*-
"""
Módulo para carga de DataFrames a PostgreSQL
Utiliza el módulo postgres.py para conexiones
"""

import pandas as pd
import logging
from datetime import datetime
from typing import Optional, Dict, Any
import json
from src.database.postgres import DatabaseManager

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class CargadorPostgres:
    """Clase para cargar DataFrames procesados a PostgreSQL"""
    
    def __init__(self, db_name: str = 'SIMM'):
        self.db_name = db_name
        self.logger = logging.getLogger(__name__)
        self.stats = {
            'cargas_exitosas': 0,
            'cargas_fallidas': 0,
            'registros_ap': 0,
            'registros_comp': 0,
            'total_registros': 0
        }
        
        # Configuración de tablas
        self.config_tablas = {
            'AP': 'pagos_ap',
            'COMP': 'pagos_comparendos'
        }
    
    def verificar_conexion(self) -> bool:
        """Verifica si la conexión a PostgreSQL está disponible"""
        try:
            engine = DatabaseManager.get_engine(self.db_name)
            with engine.connect() as conn:
                conn.execute("SELECT 1")
            self.logger.info(f"✅ Conexión exitosa a {self.db_name}")
            return True
        except Exception as e:
            self.logger.error(f"❌ Error conectando a {self.db_name}: {e}")
            return False
    
    def obtener_estado_tablas(self) -> Optional[Dict[str, Any]]:
        """Obtiene el estado actual de las tablas en PostgreSQL"""
        try:
            engine = DatabaseManager.get_engine(self.db_name)
            estado = {}
            
            for tipo, tabla in self.config_tablas.items():
                try:
                    # Verificar si la tabla existe
                    query_exists = f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = '{tabla}'
                    );
                    """
                    existe = pd.read_sql(query_exists, engine).iloc[0, 0]
                    
                    if existe:
                        # Contar registros
                        query_count = f"SELECT COUNT(*) as total FROM {tabla}"
                        result = pd.read_sql(query_count, engine)
                        
                        # Obtener rango de fechas
                        query_fechas = f"""
                        SELECT 
                            MIN(fecha_liquida) as min_fecha, 
                            MAX(fecha_liquida) as max_fecha,
                            COUNT(DISTINCT documento) as usuarios_unicos
                        FROM {tabla}
                        WHERE fecha_liquida IS NOT NULL
                        """
                        result_fechas = pd.read_sql(query_fechas, engine)
                        
                        estado[tipo] = {
                            'tabla': tabla,
                            'existe': True,
                            'registros': int(result['total'].iloc[0]),
                            'min_fecha': result_fechas['min_fecha'].iloc[0],
                            'max_fecha': result_fechas['max_fecha'].iloc[0],
                            'usuarios_unicos': int(result_fechas['usuarios_unicos'].iloc[0]),
                            'estado': 'OK'
                        }
                    else:
                        estado[tipo] = {
                            'tabla': tabla,
                            'existe': False,
                            'registros': 0,
                            'estado': 'TABLA_NO_EXISTE'
                        }
                        
                except Exception as e:
                    estado[tipo] = {
                        'tabla': tabla,
                        'existe': False,
                        'registros': 0,
                        'estado': f'ERROR: {str(e)}'
                    }
            
            return estado
            
        except Exception as e:
            self.logger.error(f"Error verificando estado de tablas: {e}")
            return None
    
    def cargar_dataframe(self, df: pd.DataFrame, tipo: str, 
                        metodo_carga: str = 'append') -> bool:
        """
        Carga un DataFrame a PostgreSQL
        
        Args:
            df: DataFrame a cargar
            tipo: Tipo de datos ('AP' o 'COMP')
            metodo_carga: 'append', 'replace', 'fail'
        """
        if df is None or df.empty:
            self.logger.warning(f"DataFrame {tipo} vacío, no se cargará")
            return False
        
        tabla = self.config_tablas.get(tipo)
        if not tabla:
            self.logger.error(f"Tipo de tabla no válido: {tipo}")
            return False
        
        try:
            engine = DatabaseManager.get_engine(self.db_name)
            
            # Preparar DataFrame para carga
            df_carga = df.copy()
            
            # Convertir fechas a formato PostgreSQL
            if 'fecha_liquida' in df_carga.columns:
                df_carga['fecha_liquida'] = pd.to_datetime(df_carga['fecha_liquida'])
            
            if 'fecha_carga' in df_carga.columns:
                df_carga['fecha_carga'] = pd.to_datetime(df_carga['fecha_carga'])
            
            # Cargar a PostgreSQL
            df_carga.to_sql(
                name=tabla,
                con=engine,
                if_exists=metodo_carga,
                index=False,
                chunksize=1000
            )
            
            # Actualizar estadísticas
            self.stats['cargas_exitosas'] += 1
            if tipo == 'AP':
                self.stats['registros_ap'] = len(df_carga)
            else:
                self.stats['registros_comp'] = len(df_carga)
            
            self.stats['total_registros'] += len(df_carga)
            
            self.logger.info(f"✅ Carga exitosa {tipo}: {len(df_carga)} registros → {tabla}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error cargando {tipo} a PostgreSQL: {e}")
            self.stats['cargas_fallidas'] += 1
            return False
    
    def cargar_multiples_dataframes(self, df_ap: Optional[pd.DataFrame], 
                                   df_comp: Optional[pd.DataFrame],
                                   metodo_carga: str = 'append') -> Dict[str, bool]:
        """
        Carga múltiples DataFrames a PostgreSQL
        
        Args:
            df_ap: DataFrame de Acuerdos de Pago
            df_comp: DataFrame de Comparendos
            metodo_carga: Método de carga para ambos DataFrames
        
        Returns:
            Dict con resultado de cada carga
        """
        resultados = {}
        
        self.logger.info("🔄 Iniciando carga múltiple a PostgreSQL")
        
        # Verificar conexión
        if not self.verificar_conexion():
            self.logger.error("❌ No se puede conectar a PostgreSQL")
            return {'AP': False, 'COMP': False}
        
        # Cargar AP
        if df_ap is not None and not df_ap.empty:
            resultados['AP'] = self.cargar_dataframe(df_ap, 'AP', metodo_carga)
        else:
            resultados['AP'] = True  # No hay datos que cargar, no es error
            self.logger.info("ℹ️ No hay datos AP para cargar")
        
        # Cargar COMP
        if df_comp is not None and not df_comp.empty:
            resultados['COMP'] = self.cargar_dataframe(df_comp, 'COMP', metodo_carga)
        else:
            resultados['COMP'] = True  # No hay datos que cargar, no es error
            self.logger.info("ℹ️ No hay datos COMP para cargar")
        
        return resultados
    
    def generar_reporte_carga(self) -> Dict[str, Any]:
        """Genera reporte detallado de la carga"""
        estado_tablas = self.obtener_estado_tablas()
        
        reporte = {
            'fecha_reporte': datetime.now().isoformat(),
            'estadisticas_carga': self.stats,
            'estado_tablas': estado_tablas,
            'resumen': {
                'total_cargas': self.stats['cargas_exitosas'] + self.stats['cargas_fallidas'],
                'tasa_exito': (self.stats['cargas_exitosas'] / 
                              max(self.stats['cargas_exitosas'] + self.stats['cargas_fallidas'], 1)) * 100,
                'total_registros_cargados': self.stats['total_registros']
            }
        }
        
        return reporte
    
    def guardar_reporte_carga(self, archivo: str = None) -> str:
        """Guarda reporte de carga en archivo JSON"""
        if archivo is None:
            archivo = f"reporte_carga_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        reporte = self.generar_reporte_carga()
        
        with open(archivo, 'w', encoding='utf-8') as f:
            json.dump(reporte, f, indent=2, ensure_ascii=False, default=str)
        
        self.logger.info(f"📋 Reporte guardado: {archivo}")
        return archivo
    
    def imprimir_resumen(self):
        """Imprime resumen de la carga en consola"""
        self.logger.info("="*60)
        self.logger.info("RESUMEN DE CARGA A POSTGRESQL")
        self.logger.info("="*60)
        self.logger.info(f"🎯 Base de datos: {self.db_name}")
        self.logger.info(f"✅ Cargas exitosas: {self.stats['cargas_exitosas']}")
        self.logger.info(f"❌ Cargas fallidas: {self.stats['cargas_fallidas']}")
        self.logger.info(f"💰 Registros AP: {self.stats['registros_ap']}")
        self.logger.info(f"🚗 Registros COMP: {self.stats['registros_comp']}")
        self.logger.info(f"📊 Total registros: {self.stats['total_registros']}")
        
        # Mostrar estado de tablas
        estado_tablas = self.obtener_estado_tablas()
        if estado_tablas:
            self.logger.info(f"📋 Estado actual de tablas:")
            for tipo, info in estado_tablas.items():
                self.logger.info(f"  • {tipo}: {info['registros']} registros - {info['estado']}")

# Función principal para usar desde otros módulos
def cargar_dataframes_a_postgres(df_ap: Optional[pd.DataFrame], 
                                df_comp: Optional[pd.DataFrame],
                                db_name: str = 'SIMM',
                                metodo_carga: str = 'append') -> Dict[str, Any]:
    """
    Función principal para cargar DataFrames a PostgreSQL
    
    Args:
        df_ap: DataFrame de Acuerdos de Pago
        df_comp: DataFrame de Comparendos
        db_name: Nombre de la base de datos
        metodo_carga: Método de carga ('append', 'replace', 'fail')
    
    Returns:
        Dict con resultados y estadísticas
    """
    cargador = CargadorPostgres(db_name)
    
    # Realizar cargas
    resultados = cargador.cargar_multiples_dataframes(df_ap, df_comp, metodo_carga)
    
    # Generar reporte
    reporte = cargador.generar_reporte_carga()
    
    # Imprimir resumen
    cargador.imprimir_resumen()
    
    return {
        'resultados_carga': resultados,
        'reporte_completo': reporte,
        'archivo_reporte': cargador.guardar_reporte_carga()
    }

# Test del módulo
if __name__ == "__main__":
    # Crear datos de prueba
    df_test = pd.DataFrame({
        'documento': ['123456789'],
        'nombre_usuario': ['USUARIO PRUEBA'],
        'valor': [100000],
        'fecha_liquida': [datetime.now()],
        'archivo_origen': ['test.txt'],
        'fecha_carga': [datetime.now()],
        'tipo_pago': ['AP'],
        'id_registro': ['test_id']
    })
    
    print("🧪 Probando módulo CargadorPostgres...")
    resultado = cargar_dataframes_a_postgres(df_test, None, 'SIMM', 'append')
    print("✅ Prueba completada")