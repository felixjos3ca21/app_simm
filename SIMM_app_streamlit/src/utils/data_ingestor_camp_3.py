import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import chardet
import logging
from typing import List, Dict, Optional
import traceback
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

class DataIngestorCamp3:
    """Clase para procesar archivos CSV de Campañas 3"""
    
    def __init__(self, db_config: Dict):
        """Inicializa el procesador de datos"""
        self.engine = None
        self.connection_status = False
        self.error_message = ""
        self.db_config = db_config
        self.target_table = 'campanas_3'
        self.control_table = 'archivos_procesados_camp_3'
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
        """Convierte string a datetime"""
        if pd.isna(date_str) or str(date_str).strip() in ['', 'nan', 'None', 'NaT']:
            return None
        
        date_str = str(date_str).strip()
        
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%d/%m/%Y %H:%M:%S',
            '%m/%d/%Y %H:%M:%S'
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
        """Transforma y limpia los datos para campanas_3"""
        # Limpieza de columnas
        df.columns = [self._clean_column(col) for col in df.columns]
        
        # Eliminar columna de índices si existe
        if 'indices' in df.columns:
            df = df.drop(columns=['indices'])
        
        # Lista completa de columnas esperadas
        expected_columns = {
            'campaign_id', 'customer_name', 'customer_last_name',
            'id_type', 'customer_id', 'telephone', 'result',
            'opt1', 'opt2', 'opt3', 'opt4', 'opt5', 'opt6',
            'opt7', 'opt8', 'opt9', 'opt10', 'opt11', 'opt12',
            'conn_id', 'module'
        }
        
        # Convertir todas las columnas a texto y limpiar
        for col in expected_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).replace(['nan', 'None', 'NaT'], '').str.strip()
            else:
                # Si falta alguna columna opcional, la creamos vacía
                if col not in ['date', 'conn_id']:  # Estas son requeridas
                    df[col] = ''
        
        # Manejo especial de fechas
        if 'date' in df.columns:
            df['date'] = df['date'].apply(self._parse_datetime)
            if df['date'].isna().any():
                logging.warning(f"Archivo {file_name} contiene fechas inválidas")
        
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
        """Valida la calidad de los datos para campanas_3"""
        errors = []
        
        # Validación de columnas requeridas
        required_columns = {'campaign_id', 'customer_id', 'date', 'conn_id'}
        missing_cols = required_columns - set(df.columns)
        if missing_cols:
            errors.append(f"Columnas requeridas faltantes: {missing_cols}")
        
        # Validación de fechas
        if 'date' in df.columns:
            null_dates = df['date'].isna().sum()
            if null_dates > 0:
                errors.append(f"{null_dates} registros con fecha inválida")
        
        return errors

    def _process_single_file(self, file_path: str, is_retry: bool = False) -> Dict:
        """Procesa un archivo individual y retorna resultado"""
        file_name = os.path.basename(file_path)
        result = {
            'success': False,
            'filename': file_name,
            'records': 0,
            'error': None,
            'is_retry': is_retry,
            'empty_module': False  # Nuevo flag para indicar que no había registros con el módulo
        }
        
        try:
            # Leer archivo
            encoding = self._detect_file_encoding(file_path)
            df = pd.read_csv(file_path, encoding=encoding, engine='python', on_bad_lines='warn')
            
            # Transformar datos
            df = self._transform_data(df, file_name)
            
            # Si el DataFrame está vacío porque no había registros con el módulo
            if len(df) == 0:
                result['success'] = True
                result['records'] = 0
                result['empty_module'] = True
                return result
                
            # Validar datos (solo si hay registros)
            validation_errors = self._validate_data(df)
            if validation_errors:
                raise ValueError(f"Errores de validación: {', '.join(validation_errors)}")
            
            # Guardar en base de datos
            with self.engine.begin() as conn:
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
                            intentos = {self.control_table}.intentos + 1,
                            error_message = NULL
                    """),
                    {'filename': file_name, 'registros': len(df)}
                )
            
            result['success'] = True
            result['records'] = len(df)
            
        except Exception as e:
            result['error'] = str(e)
            logging.error(f"Error procesando {file_name}: {str(e)}")
            logging.error(traceback.format_exc())
            
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
                                intentos = {self.control_table}.intentos + 1,
                                error_message = EXCLUDED.error_message
                        """),
                        {'filename': file_name, 'error': str(e)[:500]}
                    )
            except Exception as db_error:
                logging.error(f"Error al registrar fallo en BD: {str(db_error)}")
        
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