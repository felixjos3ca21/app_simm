import pandas as pd
import os
from sqlalchemy import text, create_engine
from datetime import datetime
import chardet
import logging
from typing import List, Dict, Optional
import traceback
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configurar logging
logger = logging.getLogger(__name__)

def setup_logging():
    """Configura el sistema de logging"""
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        file_handler = logging.FileHandler('wolkvox_ingestion.log', encoding='utf-8')
        file_handler.setFormatter(formatter)
        
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

setup_logging()

class DataIngestorTip3:
    """Clase para procesar archivos CSV de Wolkvox Tipificación 3"""
    
    def __init__(self, db_config: Dict):
        """Inicializa el procesador de datos"""
        self.engine = None
        self.connection_status = False
        self.error_message = ""
        self.db_config = db_config
        self.target_table = 'tipificaciones_3'
        self.control_table = 'archivos_procesados_tip3'
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
            logger.error(f"Error de conexión a la base de datos: {str(e)}")

    def _setup_database(self):
        """Verifica que las tablas necesarias existan"""
        try:
            with self.engine.connect() as conn:
                # Verificar que la tabla de control existe
                result = conn.execute(text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = '{self.control_table}'
                    );
                """))
                
                if not result.scalar():
                    conn.execute(text(f"""
                        CREATE TABLE {self.control_table} (
                            id SERIAL PRIMARY KEY,
                            nombre_archivo VARCHAR(255) UNIQUE NOT NULL,
                            fecha_procesado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            registros_insertados INTEGER,
                            estado VARCHAR(20) DEFAULT 'completado',
                            intentos INTEGER DEFAULT 1,
                            error_message TEXT
                        )
                    """))
                
                # Verificar tabla objetivo
                result = conn.execute(text(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = '{self.target_table}'
                    );
                """))
                
                if not result.scalar():
                    raise Exception(f"Tabla objetivo {self.target_table} no existe en la base de datos")
                
                # Asegurar columna archivo_origen
                try:
                    conn.execute(text(f"""
                        ALTER TABLE {self.target_table} 
                        ADD COLUMN IF NOT EXISTS archivo_origen VARCHAR(255)
                    """))
                except:
                    pass  # La columna ya existe
                    
        except Exception as e:
            logger.error(f"Error configurando base de datos: {str(e)}")
            raise Exception(f"Error configurando base de datos: {str(e)}")

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
        """Versión mejorada para manejar formatos dd/mm/yyyy HH:MM"""
        if pd.isna(date_str) or str(date_str).strip() in ['', 'nan', 'None', 'NaT']:
            return None
        
        date_str = str(date_str).strip()
        
        # Formatos específicos para tus archivos
        formats = [
            '%d/%m/%Y %H:%M',      # 26/03/2025 18:25 (tu formato actual)
            '%d/%m/%Y %H:%M:%S',   # 26/03/2025 18:25:00 (por si acaso)
            '%Y-%m-%d %H:%M:%S',   # 2025-03-26 18:25:00 (formato deseado)
            '%Y-%m-%d %H:%M',      # 2025-03-26 18:25
            '%d-%m-%Y %H:%M',      # 26-03-2025 18:25
            '%m/%d/%Y %H:%M',      # 03/26/2025 18:25 (formato americano)
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt
            except ValueError:
                continue
        
        # Si falla, intentar con parser flexible de pandas
        try:
            return pd.to_datetime(date_str, dayfirst=True)
        except:
            logger.warning(f"Formato de fecha no reconocido: {date_str}")
            return None

    def _clean_column(self, col: str) -> str:
        """Normaliza nombres de columnas para PostgreSQL"""
        col = str(col).strip().lower()
        col = col.replace(' ', '_').replace('-', '_')
        return ''.join(c for c in col if c.isalnum() or c == '_')

    def _transform_data(self, df: pd.DataFrame, file_name: str) -> pd.DataFrame:
        """Transforma y limpia los datos filtrando por module='andes-movilidadtigo'"""
        # Limpieza inicial de columnas
        df.columns = [self._clean_column(col) for col in df.columns]
        
        # Verificar existencia de columna module
        if 'module' not in df.columns:
            logger.error(f"Columna 'module' no encontrada en {file_name}")
            return pd.DataFrame()
        
        # Filtrar solo registros con module='andes-movilidadtigo'
        filtered_df = df[df['module'].str.strip().str.lower() == 'andes-movilidadtigo'].copy()
        
        if len(filtered_df) == 0:
            logger.warning(f"No se encontraron registros con module='andes-movilidadtigo' en {file_name}")
            return pd.DataFrame()
        
        # Limpieza de columnas (manteniendo solo las necesarias)
        if 'indices' in filtered_df.columns:
            filtered_df = filtered_df.drop(columns=['indices'])
        
        # Transformación de fechas
        if 'date' in filtered_df.columns:
            filtered_df['date'] = filtered_df['date'].apply(self._parse_datetime)
        
        # Limpieza de texto en columnas específicas
        text_columns = [
            'conn_id', 'agent_id', 'agent_name', 'agent_dni', 'skill_id',
            'cod_act', 'description_cod_act', 'cod_act_2', 'description_cod_act_2',
            'type_interaction', 'telephone', 'destiny', 'hang_up', 'customer_id',
            'campaign_id', 'comments'
        ]
        
        for col in text_columns:
            if col in filtered_df.columns:
                filtered_df[col] = filtered_df[col].astype(str).replace(['nan', 'None', 'NaT'], '').str.strip()
        
        # Manejo de tiempo
        if 'time' in filtered_df.columns:
            filtered_df['time'] = pd.to_datetime(
                filtered_df['time'], 
                format='%H:%M:%S', 
                errors='coerce'
            ).dt.time
        
        # Añadir origen del archivo
        filtered_df['archivo_origen'] = file_name
        
        logger.info(f"Filtrado completado: {len(filtered_df)} registros válidos de {len(df)} totales")
        return filtered_df

    def _validate_data(self, df: pd.DataFrame) -> List[str]:
        """Valida la calidad de los datos"""
        errors = []
        
        # Validar fechas
        if 'date' in df.columns:
            null_dates = df['date'].isna().sum()
            if null_dates > 0:
                errors.append(f"{null_dates} registros con fecha inválida")
        
        # Validar columnas requeridas
        required_columns = {'conn_id', 'date', 'agent_id'}
        missing_cols = required_columns - set(df.columns)
        if missing_cols:
            errors.append(f"Columnas requeridas faltantes: {missing_cols}")
        
        return errors

    def _process_single_file(self, file_path: str, is_retry: bool = False) -> Dict:
        """Procesa un archivo individual y retorna resultado"""
        file_name = os.path.basename(file_path)
        result = {
            'success': False,
            'filename': file_name,
            'records': 0,
            'error': None,
            'is_retry': is_retry
        }
        
        try:
            # Leer archivo
            encoding = self._detect_file_encoding(file_path)
            df = pd.read_csv(file_path, encoding=encoding, engine='python', on_bad_lines='warn')
            
            # Transformar datos
            df = self._transform_data(df, file_name)
            
            # Caso especial: archivo sin registros válidos después del filtro
            if len(df) == 0:
                # Registrar en tabla de control como procesado con 0 registros
                try:
                    with self.engine.begin() as conn:
                        conn.execute(
                            text(f"""
                                INSERT INTO {self.control_table} 
                                (nombre_archivo, registros_insertados, estado, error_message) 
                                VALUES (:filename, 0, 'completado', 'Archivo sin registros con module=andes-movilidadtigo')
                                ON CONFLICT (nombre_archivo) DO UPDATE SET
                                    registros_insertados = 0,
                                    estado = 'completado',
                                    error_message = 'Archivo sin registros con module=andes-movilidadtigo',
                                    fecha_procesado = CURRENT_TIMESTAMP,
                                    intentos = CAST({self.control_table}.intentos AS INTEGER) + 1
                            """),
                            {'filename': file_name}
                        )
                except Exception as e:
                    logging.error(f"Error registrando archivo vacío en tabla de control: {str(e)}")
                
                result.update({
                    'success': True,  # Considerado éxito técnico
                    'no_valid_records': True,
                    'error': "Archivo no contiene registros con module='andes-movilidadtigo'"
                })
                return result
            
            # Validar datos
            validation_errors = self._validate_data(df)
            if validation_errors:
                raise ValueError(f"Errores de validación: {', '.join(validation_errors)}")
            
            # Guardar en base de datos
            with self.engine.begin() as conn:
                # Si es reintento, eliminar registros previos
                if is_retry:
                    conn.execute(
                        text(f"DELETE FROM {self.target_table} WHERE archivo_origen = :filename"),
                        {'filename': file_name}
                    )
                
                # Insertar nuevos registros
                df.to_sql(
                    self.target_table,
                    conn,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=10000
                )
                
                # Actualizar tabla de control
                conn.execute(
                    text(f"""
                        INSERT INTO {self.control_table} 
                        (nombre_archivo, registros_insertados, estado) 
                        VALUES (:filename, :registros, 'completado')
                        ON CONFLICT (nombre_archivo) DO UPDATE SET
                            registros_insertados = EXCLUDED.registros_insertados,
                            estado = 'completado',
                            fecha_procesado = CURRENT_TIMESTAMP,
                            intentos = CAST({self.control_table}.intentos AS INTEGER) + 1,
                            error_message = NULL
                    """),
                    {'filename': file_name, 'registros': len(df)}
                )
            
            result['success'] = True
            result['records'] = len(df)
            
        except Exception as e:
            result['error'] = str(e)
            
            # Registrar fallo en BD
            try:
                with self.engine.connect() as conn:
                    conn.execute(
                        text(f"""
                            INSERT INTO {self.control_table} 
                            (nombre_archivo, registros_insertados, estado, error_message) 
                            VALUES (:filename, 0, 'fallido', :error)
                            ON CONFLICT (nombre_archivo) DO UPDATE SET
                                registros_insertados = 0,
                                estado = 'fallido',
                                fecha_procesado = CURRENT_TIMESTAMP,
                                intentos = CAST({self.control_table}.intentos AS INTEGER) + 1,
                                error_message = EXCLUDED.error_message
                        """),
                        {'filename': file_name, 'error': str(e)[:500]}
                    )
            except Exception as db_error:
                logger.error(f"Error al registrar fallo en BD: {str(db_error)}")
        
        return result

    def get_files_to_process(self, folder_path: str) -> Dict[str, List[str]]:
        """Identifica archivos nuevos y fallidos para procesar"""
        try:
            logger.info(f"Analizando archivos en carpeta: {folder_path}")
            
            # Obtener todos los archivos CSV
            all_files = [
                f for f in os.listdir(folder_path) 
                if f.lower().endswith('.csv') and os.path.isfile(os.path.join(folder_path, f))
            ]
            
            logger.info(f"Archivos CSV encontrados: {len(all_files)}")
            if len(all_files) > 0:
                logger.info(f"Primeros archivos: {all_files[:5]}")
            
            if not all_files:
                return {'new': [], 'failed': [], 'processed': []}
            
            # Consultar estado en base de datos
            processed = []
            failed = []
            
            try:
                with self.engine.connect() as conn:
                    # Verificar que la tabla de control existe
                    table_exists = conn.execute(text(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = '{self.control_table}'
                        );
                    """)).scalar()
                    
                    if not table_exists:
                        logger.warning(f"Tabla de control {self.control_table} no existe. Todos los archivos serán considerados nuevos.")
                        return {'new': all_files, 'failed': [], 'processed': []}
                    
                    # Consultar archivos procesados
                    processed_result = conn.execute(text(f"""
                        SELECT nombre_archivo 
                        FROM {self.control_table} 
                        WHERE estado = 'completado'
                    """))
                    processed = [row[0] for row in processed_result]
                    
                    # Consultar archivos fallidos
                    failed_result = conn.execute(text(f"""
                        SELECT nombre_archivo 
                        FROM {self.control_table} 
                        WHERE estado = 'fallido'
                    """))
                    failed = [row[0] for row in failed_result]
                    
                    logger.info(f"Archivos procesados en BD: {len(processed)}")
                    logger.info(f"Archivos fallidos en BD: {len(failed)}")
                    
                    # Corrección automática de estados para archivos fallidos sin registros válidos
                    if failed:
                        logger.info("Verificando archivos fallidos para corrección automática de estados...")
                        corrected_files = []
                        
                        for filename in failed:
                            file_path = os.path.join(folder_path, filename)
                            
                            # Verificar si el archivo existe en la carpeta
                            if not os.path.exists(file_path):
                                continue
                                
                            try:
                                # Leer y transformar el archivo para verificar registros válidos
                                encoding = self._detect_file_encoding(file_path)
                                df = pd.read_csv(file_path, encoding=encoding, engine='python', on_bad_lines='warn')
                                df_transformed = self._transform_data(df, filename)
                                
                                # Si no tiene registros válidos, corregir el estado automáticamente
                                if len(df_transformed) == 0:
                                    conn.execute(
                                        text(f"""
                                            UPDATE {self.control_table} 
                                            SET estado = 'completado',
                                                error_message = 'Archivo sin registros con module=andes-movilidadtigo - Estado corregido automáticamente',
                                                fecha_procesado = CURRENT_TIMESTAMP,
                                                registros_insertados = 0
                                            WHERE nombre_archivo = :filename
                                        """),
                                        {'filename': filename}
                                    )
                                    corrected_files.append(filename)
                                    logger.info(f"Estado corregido automáticamente para: {filename}")
                                    
                            except Exception as e:
                                logger.warning(f"No se pudo verificar archivo {filename} para corrección automática: {str(e)}")
                                continue
                        
                        # Actualizar listas después de la corrección
                        if corrected_files:
                            logger.info(f"Se corrigieron automáticamente {len(corrected_files)} archivos")
                            # Mover archivos corregidos de failed a processed
                            for filename in corrected_files:
                                if filename in failed:
                                    failed.remove(filename)
                                    processed.append(filename)
                    
            except Exception as db_error:
                logger.error(f"Error consultando base de datos: {str(db_error)}")
                # En caso de error de BD, consideramos todos como nuevos por seguridad
                return {'new': all_files, 'failed': [], 'processed': []}
            
            # Clasificar archivos
            new_files = [f for f in all_files if f not in processed and f not in failed]
            failed_files = [f for f in failed if f in all_files]
            processed_files = [f for f in processed if f in all_files]
            
            logger.info(f"Clasificación final - Nuevos: {len(new_files)}, Fallidos: {len(failed_files)}, Procesados: {len(processed_files)}")
            
            return {
                'new': new_files, 
                'failed': failed_files,
                'processed': processed_files
            }
            
        except Exception as e:
            logger.error(f"Error identificando archivos: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            # En caso de error, devolver todos como nuevos por seguridad
            return {'new': all_files if 'all_files' in locals() else [], 'failed': [], 'processed': []}