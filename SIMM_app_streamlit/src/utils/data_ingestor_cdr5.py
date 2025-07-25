import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import chardet
import logging
from typing import List, Dict, Optional
import traceback
from dotenv import load_dotenv
import sys

# Cargar variables de entorno
load_dotenv()
def setup_logging():
    """Configura logging detallado"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Formato completo
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s() - %(message)s'
    )
    
    # Handler para archivo
    file_handler = logging.FileHandler('wolkvox_processing.log')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    
    # Handler para console
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)
    
    # Limpiar handlers existentes
    if logger.hasHandlers():
        logger.handlers.clear()
    
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    
    # Capturar warnings
    logging.captureWarnings(True)
    

    # Log de excepciones no capturadas
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        
        logger.critical(
            "Excepción no capturada",
            exc_info=(exc_type, exc_value, exc_traceback)
        )
    
    sys.excepthook = handle_exception
setup_logging()
logger = logging.getLogger()

class DataIngestorCDR5:
    """Clase para procesar archivos CSV de CDR5"""
    
    def __init__(self, db_config: Dict):
        """Inicializa el procesador de datos"""
        self.engine = None
        self.connection_status = False
        self.error_message = ""
        self.db_config = db_config
        self.target_table = 'cdr_5'
        self.control_table = 'archivos_procesados_cdr5'
        self._initialize_connection()

    def _initialize_connection(self):
        """Inicializa la conexión a la base de datos"""
        try:
            connection_string = (
                f"postgresql+psycopg2://{self.db_config['user']}:{self.db_config['password']}"
                f"@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            )
            self.engine = create_engine(
                connection_string,
                isolation_level="AUTOCOMMIT",
                pool_pre_ping=True
            )
            
            # Probar conexión
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                self.connection_status = True
                self._setup_database()
            
        except Exception as e:
            self.connection_status = False
            self.error_message = str(e)
            logging.error(f"Error de conexión a la base de datos: {str(e)}")

    def _setup_database(self):
        """Verifica/actualiza la estructura de la tabla de control con manejo explícito de transacciones"""
        try:
            with self.engine.begin() as conn:  # Usamos begin() para manejo explícito de transacción
                # Verificar existencia de la tabla
                table_exists = conn.execute(text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = '{self.control_table}'
                    );
                """)).scalar()
                
                if not table_exists:
                    # Crear tabla con todos los campos
                    conn.execute(text(f"""
                        CREATE TABLE {self.control_table} (
                            id SERIAL PRIMARY KEY,
                            nombre_archivo VARCHAR(255) UNIQUE NOT NULL,
                            fecha_procesado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            registros_insertados INTEGER,
                            estado VARCHAR(20) DEFAULT 'completado',
                            intentos INTEGER DEFAULT 1,
                            error_type VARCHAR(100),
                            error_message TEXT,
                            error_details TEXT,
                            stack_trace TEXT,
                            fecha_error TIMESTAMP,
                            no_valid_records BOOLEAN DEFAULT FALSE
                        );
                    """))
                    logger.info(f"Tabla {self.control_table} creada exitosamente")
                else:
                    # Verificar y añadir columnas faltantes
                    existing_columns = [row[0] for row in 
                        conn.execute(text(f"""
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_name = '{self.control_table}'
                        """)).fetchall()]
                    
                    columns_to_add = [
                        ('error_type', 'VARCHAR(100)'),
                        ('error_details', 'TEXT'),
                        ('stack_trace', 'TEXT'),
                        ('fecha_error', 'TIMESTAMP'),
                        ('no_valid_records', 'BOOLEAN DEFAULT FALSE')
                    ]
                    
                    for col_name, col_type in columns_to_add:
                        if col_name not in existing_columns:
                            try:
                                conn.execute(text(f"""
                                    ALTER TABLE {self.control_table} 
                                    ADD COLUMN {col_name} {col_type}
                                """))
                                logger.info(f"Columna {col_name} añadida a {self.control_table}")
                            except Exception as e:
                                logger.error(f"Error añadiendo columna {col_name}: {str(e)}")
                                raise  # Opcional: puedes continuar sin raise si prefieres
                    
        except Exception as e:
            logger.critical(f"Fallo en _setup_database: {str(e)}")
            raise Exception(f"Error configurando estructura de BD: {str(e)}")

    def _detect_file_encoding(self, file_path: str) -> str:
        """Detecta la codificación del archivo"""
        encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    f.read(10000)
                    return encoding
            except:
                continue
        
        # Si falla, usar chardet
        try:
            with open(file_path, 'rb') as f:
                result = chardet.detect(f.read(100000))
            return result['encoding'] if result['confidence'] > 0.7 else 'latin-1'
        except:
            return 'latin-1'

    def _parse_datetime(self, date_str) -> Optional[datetime]:
        """Convierte string a datetime"""
        if pd.isna(date_str) or str(date_str).strip() in ['', 'nan', 'None', 'NaT']:
            return None
        
        date_str = str(date_str).strip()
        
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%d/%m/%Y %H:%M:%S',
            '%m/%d/%Y %H:%M:%S',
            '%d-%m-%Y %H:%M:%S',
            '%Y/%m/%d %H:%M:%S'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        logging.warning(f"No se pudo parsear fecha: {date_str}")
        return None

    def _clean_column(self, col: str) -> str:
        """Normaliza nombres de columnas para PostgreSQL"""
        col = str(col).strip().lower()
        col = col.replace(' ', '_').replace('-', '_')
        return ''.join(c for c in col if c.isalnum() or c == '_')

    def _transform_data(self, df: pd.DataFrame, file_name: str) -> pd.DataFrame:
        """Transforma y limpia los datos para CDR5 con filtrado por módulo"""
        # Limpieza inicial de columnas
        df.columns = [self._clean_column(col) for col in df.columns]
        
        # Eliminar columna de índices si existe
        if 'index' in df.columns:
            df = df.drop(columns=['index'])
        
        # Verificar existencia de columna module
        if 'module' not in df.columns:
            raise ValueError(f"El archivo {file_name} no contiene la columna 'module' - requerida para filtrado")
        
        # Convertir module a minúsculas y limpiar
        df['module'] = df['module'].str.lower().str.strip()
        
        # Filtrar solo registros con module='andes-movilidadtigo'
        filtered_df = df[df['module'] == 'andes-movilidadtigo'].copy()
        
        if len(filtered_df) == 0:
            logging.warning(f"Archivo {file_name} no contiene registros con module='andes-movilidadtigo'")
            # Retornar DataFrame vacío con las columnas esperadas
            return pd.DataFrame(columns=df.columns.tolist() + ['archivo_origen'])
        
        # Mapeo de posibles nombres de columnas
        column_mapping = {
            'agente': 'agent_name',
            'agent': 'agent_name',
            'nombre_agente': 'agent_name',
            'destino': 'destiny',
            'destination': 'destiny',
            'telefono': 'telephone',
            'tel': 'telephone',
            'phone': 'telephone',
            'tiempo_ring': 'ring_time',
            'ring': 'ring_time',
            'resultado': 'result',
            'tipo_interaccion': 'type_interaction',
            'interaction_type': 'type_interaction',
            'tipo': 'type_interaction',
            'id_cliente': 'customer_id',
            'cliente_id': 'customer_id',
            'id_campana': 'campaign_id',
            'campana_id': 'campaign_id',
            'campaign': 'campaign_id',
            'id_agente': 'agent_id',
            'agente_id': 'agent_id',
            'conexion_id': 'conn_id',
            'connection_id': 'conn_id',
            'modulo': 'module',
            'fecha': 'date',
            'fecha_hora': 'date',
            'datetime': 'date',
            'timestamp': 'date'
        }
        
        # Aplicar mapeo de columnas
        df = df.rename(columns=column_mapping)
        
        # Lista de columnas esperadas
        expected_columns = {
            'agent_name', 'destiny', 'telephone', 'ring_time',
            'result', 'type_interaction', 'customer_id', 'campaign_id',
            'agent_id', 'conn_id', 'module', 'date'
        }
        
        # Convertir todas las columnas a texto y limpiar (excepto date)
        for col in expected_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).replace(['nan', 'None', 'NaT'], '').str.strip()
            else:
                # Si falta alguna columna, la creamos vacía
                if col != 'date':  # date se maneja aparte
                    df[col] = ''
        
        # Manejo especial de fechas
        if 'date' in df.columns:
            df['date'] = df['date'].apply(self._parse_datetime)
            if df['date'].isna().any():
                logging.warning(f"Archivo {file_name} contiene fechas inválidas")
        else:
            # Si no hay columna de fecha, usar fecha actual
            df['date'] = datetime.now()
            logging.warning(f"Archivo {file_name} no contiene columna de fecha, usando fecha actual")
        
        # Asegurar que module esté en minúsculas y filtrar
        if 'module' in df.columns:
            df['module'] = df['module'].str.lower().str.strip()
            
            # Filtrar solo registros con module='andes-movilidadtigo'
            mask = df['module'] == 'andes-movilidadtigo'
            filtered_df = df[mask].copy()
            
            if len(filtered_df) == 0:
                logging.info(f"Archivo {file_name} no contiene registros con module='andes-movilidadtigo' - 0 registros válidos")
                # Retornar DataFrame vacío pero con las columnas correctas
                return pd.DataFrame(columns=df.columns)
        else:
            logging.warning(f"Archivo {file_name} no tiene columna 'module' - se procesarán todos los registros")
            filtered_df = df.copy()
        
        # Añadir origen
        filtered_df['archivo_origen'] = file_name
        
        return filtered_df

    def _validate_data(self, df: pd.DataFrame) -> List[str]:
        """Valida la calidad de los datos para CDR5"""
        errors = []
        
        # Validación de columnas críticas
        important_columns = {'agent_name', 'date', 'campaign_id'}
        missing_cols = important_columns - set(df.columns)
        if missing_cols:
            errors.append(f"Columnas importantes faltantes: {missing_cols}")
        
        # Validación de fechas
        if 'date' in df.columns:
            null_dates = df['date'].isna().sum()
            if null_dates > 0:
                errors.append(f"{null_dates} registros con fecha inválida")
        
        return errors

    def _process_single_file(self, file_path: str, is_retry: bool = False) -> Dict:
        """Procesa un archivo individual con manejo explícito de casos sin registros válidos"""
        file_name = os.path.basename(file_path)
        result = {
            'success': False,
            'filename': file_name,
            'records': 0,
            'error': None,
            'error_type': None,
            'is_retry': is_retry,
            'no_valid_records': False  
        }
        
        try:
            # Lectura y transformación del archivo
            encoding = self._detect_file_encoding(file_path)
            df = pd.read_csv(file_path, encoding=encoding, engine='python', on_bad_lines='warn')
            df = self._transform_data(df, file_name)
            
            # Caso especial: archivo sin registros válidos
            if len(df) == 0:
                result.update({
                    'success': True,  # Considerado éxito técnico
                    'no_valid_records': True,
                    'error': "Archivo no contiene registros con module='andes-movilidadtigo'"
                })
                return result
            
            # 6. Conexión y procesamiento en BD
            try:
                with self.engine.begin() as conn:
                    # Eliminar registros previos si es reintento
                    if is_retry:
                        try:
                            conn.execute(
                                text(f"DELETE FROM {self.target_table} WHERE archivo_origen = :filename"),
                                {'filename': file_name}
                            )
                        except Exception as e:
                            raise Exception(f"Error eliminando registros previos: {str(e)}") from e
                    
                    # Insertar nuevos registros
                    try:
                        df.to_sql(
                            self.target_table,
                            conn,
                            if_exists='append',
                            index=False,
                            method='multi',
                            chunksize=10000
                        )
                    except Exception as e:
                        raise Exception(f"Error insertando registros: {str(e)}") from e
                    
                    # Actualizar tabla de control
                    try:
                        conn.execute(
                            text(f"""
                                INSERT INTO {self.control_table} 
                                (nombre_archivo, registros_insertados, estado) 
                                VALUES (:filename, :registros, 'completado')
                                ON CONFLICT (nombre_archivo) DO UPDATE SET
                                    registros_insertados = EXCLUDED.registros_insertados,
                                    estado = 'completado',
                                    fecha_procesado = CURRENT_TIMESTAMP,
                                    intentos = {self.control_table}.intentos + 1,
                                    error_message = NULL
                            """),
                            {'filename': file_name, 'registros': len(df)}
                        )
                    except Exception as e:
                        raise Exception(f"Error actualizando tabla de control: {str(e)}") from e
                
                result.update({
                    'success': True,
                    'records': len(df)
                })
                
            except Exception as e:
                raise e  # Re-lanzar excepciones de la conexión/BD
            
        except Exception as e:
            # Capturar TODOS los errores posibles
            error_type = type(e).__name__
            error_details = traceback.format_exc()
            
            result.update({
                'error': str(e),
                'error_type': error_type,
                'error_details': error_details
            })
            
            # Registrar en logs
            logger.error(f"ERROR PROCESANDO {file_name} - Tipo: {error_type}")
            logger.error(f"Mensaje: {str(e)}")
            logger.error(f"Detalles:\n{error_details}")
            
            # Registrar en BD (con más detalles)
            try:
                if result.get('no_valid_records', False):
                    conn.execute(text(f"""
                        INSERT INTO {self.control_table} 
                        (nombre_archivo, registros_insertados, estado, error_message, no_valid_records) 
                        VALUES (:filename, 0, 'completado', :error, TRUE)
                        ON CONFLICT (nombre_archivo) DO UPDATE SET
                            registros_insertados = 0,
                            estado = 'completado',
                            fecha_procesado = CURRENT_TIMESTAMP,
                            intentos = {self.control_table}.intentos + 1,
                            error_message = :error,
                            no_valid_records = TRUE
                    """), {'filename': file_name, 'error': result['error']})
            except Exception as db_error:
                logger.critical(f"NO SE PUDO REGISTRAR EL ERROR EN BD: {str(db_error)}")
                logger.critical(f"Error original: {error_type} - {str(e)}")
        
        return result

    def get_files_to_process(self, folder_path: str) -> Dict[str, List[str]]:
        """Identifica archivos nuevos y fallidos para procesar"""
        try:
            # Obtener todos los archivos CSV
            all_files = [
                f for f in os.listdir(folder_path) 
                if f.lower().endswith('.csv') and os.path.isfile(os.path.join(folder_path, f))
            ]
            
            if not all_files:
                return {'new': [], 'failed': [], 'processed': []}
            
            # Consultar estado en base de datos
            with self.engine.connect() as conn:
                processed = pd.read_sql(
                    f"SELECT nombre_archivo FROM {self.control_table} WHERE estado = 'completado'", 
                    conn
                )['nombre_archivo'].tolist()
                
                failed = pd.read_sql(
                    f"SELECT nombre_archivo FROM {self.control_table} WHERE estado = 'fallido'", 
                    conn
                )['nombre_archivo'].tolist()
            
            new_files = [f for f in all_files if f not in processed and f not in failed]
            failed_files = [f for f in failed if f in all_files]
            processed_files = [f for f in processed if f in all_files]
            
            return {
                'new': new_files, 
                'failed': failed_files,
                'processed': processed_files
            }
            
        except Exception as e:
            logging.error(f"Error identificando archivos: {str(e)}")
            return {'new': [], 'failed': [], 'processed': []}