# import streamlit as st
# import pandas as pd
# import os
# from pathlib import Path
# from tkinter import Tk, filedialog
# from sqlalchemy import text
# from src.database.postgres import DatabaseManager
# from src.utils.fondo import set_background
# from src.utils.data_ingestor_tip3 import DataIngestorTip3
# import time
# from sqlalchemy import create_engine
# import streamlit as st
# import pandas as pd
# import os
# from pathlib import Path
# from tkinter import Tk, filedialog
# from sqlalchemy import create_engine, text
# from datetime import datetime
# import chardet
# import logging
# from typing import List, Dict, Optional
# import traceback
# from dotenv import load_dotenv
# import time
# import io
# import sys
# # Configuración de página
# st.set_page_config(
#     page_title="SIAMM - Carga Wolkvox Tipificación 3",
#     page_icon="src/utils/favicon-114x114.png",
#     layout="wide",
#     initial_sidebar_state="expanded"
# )

# # Estilos (usando los mismos que proporcionaste)
# st.markdown("""
#     <style>
#     [data-testid=stSidebar] { background: #a5d6a7 !important; padding: 20px 10px; }
#     .main-container { padding: 2rem; background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%); min-height: 100vh; }
#     .metric-card { background: white; padding: 20px; border-radius: 15px; box-shadow: 0 8px 32px rgba(0,0,0,0.1); margin: 10px 0; transition: transform 0.3s ease; }
#     .metric-card:hover { transform: translateY(-5px); }
#     .section-title { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-size: 2rem; margin: 30px 0 20px 0; text-align: center; }
#     .section-header { color: #2e7d32; border-bottom: 2px solid #a5d6a7; padding-bottom: 0.5rem; margin-bottom: 1rem; }
#     .stButton > button { background: linear-gradient(120deg, #a5d6a7 0%, #2ecc71 100%); color: white; border: none; border-radius: 25px; padding: 12px 30px; transition: all 0.3s ease; }
#     .stButton > button:hover { transform: translateY(-2px); box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6); }
#     .stProgress > div > div > div > div { background-color: #2e7d32; }
#     .sidebar-title { color: #2c3e50; font-size: 1.2rem; margin-bottom: 1rem; font-weight: 600; }
#     .success-box { background: #d4edda; border: 1px solid #c3e6cb; border-radius: 8px; padding: 15px; margin: 10px 0; }
#     .error-box { background: #f8d7da; border: 1px solid #f5c6cb; border-radius: 8px; padding: 15px; margin: 10px 0; }
#     .info-box { background: #d1ecf1; border: 1px solid #b3d4d8; border-radius: 8px; padding: 15px; margin: 10px 0; }
#     .file-status-new { color: #28a745; font-weight: bold; }
#     .file-status-modified { color: #ffc107; font-weight: bold; }
#     .file-status-existing { color: #6c757d; }
#     .directory-input { background: white; padding: 15px; border-radius: 10px; margin-bottom: 20px; }
#     </style>
# """, unsafe_allow_html=True)

# # Logo y fondo
# st.image("src/utils/logo-andesbpo-359x143.png", width=150)
# set_background("src/utils/bg-seccion.png")

# # Título principal
# st.markdown("<h1 class='section-title'>Carga de Datos Wolkvox - Tipificación 3</h1>", unsafe_allow_html=True)

# # Cargar variables de entorno
# load_dotenv()


# # Configurar logging al inicio del módulo
# logger = logging.getLogger(__name__)

# def setup_logging():
#     """Configura el sistema de logging una sola vez"""
#     if not logger.handlers:  # Evitar múltiples configuraciones
#         logger.setLevel(logging.INFO)
        
#         # Formato del log
#         formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
#         # Manejador para archivo
#         file_handler = logging.FileHandler('wolkvox_ingestion.log', encoding='utf-8')
#         file_handler.setFormatter(formatter)
        
#         # Manejador para consola
#         stream_handler = logging.StreamHandler(sys.stdout)
#         stream_handler.setFormatter(formatter)
        
#         # Añadir manejadores
#         logger.addHandler(file_handler)
#         logger.addHandler(stream_handler)

# # Llamar a la configuración al inicio
# setup_logging()

# # Configuración de base de datos desde .env
# DB_CONFIG = {
#     'host': os.getenv('DB_SIMM_HOST', 'localhost'),
#     'database': os.getenv('DB_SIMM_NAME', 'SIMM'),
#     'user': os.getenv('DB_SIMM_USER', 'postgres'),
#     'password': os.getenv('DB_SIMM_PASSWORD', 'admin'),
#     'port': os.getenv('DB_SIMM_PORT', '5432')
# }

# # Configuración de tablas
# TARGET_TABLE = 'tipificaciones_3'
# CONTROL_TABLE = 'archivos_procesados_tip3'

# class WolkvoxDataIngestor:
#     """Clase principal para procesar archivos CSV de Wolkvox"""
    
#     def __init__(self):
#         """Inicializa el procesador de datos"""
#         self.engine = None
#         self.connection_status = False
#         self.error_message = ""
#         self._initialize_connection()

#     def _initialize_connection(self):
#         """Inicializa la conexión a la base de datos"""
#         try:
#             connection_string = (
#                 f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
#                 f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
#             )
#             self.engine = create_engine(
#                 connection_string,
#                 isolation_level="AUTOCOMMIT",
#                 pool_pre_ping=True
#             )
            
#             # Probar conexión
#             with self.engine.connect() as conn:
#                 conn.execute(text("SELECT 1"))
#                 self.connection_status = True
#                 self._setup_database()
            
#         except Exception as e:
#             self.connection_status = False
#             self.error_message = str(e)

#     def _setup_database(self):
#         """Verifica que las tablas necesarias existan"""
#         try:
#             with self.engine.connect() as conn:
#                 # Verificar que la tabla de control existe
#                 result = conn.execute(text(f"""
#                     SELECT EXISTS (
#                         SELECT FROM information_schema.tables 
#                         WHERE table_name = '{CONTROL_TABLE}'
#                     );
#                 """))
                
#                 if not result.scalar():
#                     conn.execute(text(f"""
#                         CREATE TABLE {CONTROL_TABLE} (
#                             id SERIAL PRIMARY KEY,
#                             nombre_archivo VARCHAR(255) UNIQUE NOT NULL,
#                             fecha_procesado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#                             registros_insertados INTEGER,
#                             estado VARCHAR(20) DEFAULT 'completado',
#                             intentos INTEGER DEFAULT 1,
#                             error_message TEXT
#                         )
#                     """))
                
#                 # Verificar tabla objetivo
#                 result = conn.execute(text(f"""
#                     SELECT EXISTS (
#                         SELECT FROM information_schema.tables 
#                         WHERE table_name = '{TARGET_TABLE}'
#                     );
#                 """))
                
#                 if not result.scalar():
#                     raise Exception(f"Tabla objetivo {TARGET_TABLE} no existe en la base de datos")
                
#                 # Asegurar columna archivo_origen
#                 try:
#                     conn.execute(text(f"""
#                         ALTER TABLE {TARGET_TABLE} 
#                         ADD COLUMN IF NOT EXISTS archivo_origen VARCHAR(255)
#                     """))
#                 except:
#                     pass  # La columna ya existe
                    
#         except Exception as e:
#             raise Exception(f"Error configurando base de datos: {str(e)}")

#     def _detect_file_encoding(self, file_path: str) -> str:
#         """Detecta la codificación del archivo"""
#         encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
        
#         for encoding in encodings:
#             try:
#                 with open(file_path, 'r', encoding=encoding) as f:
#                     f.read(10000)
#                     return encoding
#             except:
#                 continue
        
#         # Si falla, usar chardet
#         try:
#             with open(file_path, 'rb') as f:
#                 result = chardet.detect(f.read(100000))
#             return result['encoding'] if result['confidence'] > 0.7 else 'latin-1'
#         except:
#             return 'latin-1'

#     def _parse_datetime(self, date_str) -> Optional[datetime]:
#         """Versión mejorada para manejar formatos dd/mm/yyyy HH:MM"""
#         if pd.isna(date_str) or str(date_str).strip() in ['', 'nan', 'None', 'NaT']:
#             return None
        
#         date_str = str(date_str).strip()
        
#         # Formatos específicos para tus archivos
#         formats = [
#             '%d/%m/%Y %H:%M',      # 26/03/2025 18:25 (tu formato actual)
#             '%d/%m/%Y %H:%M:%S',   # 26/03/2025 18:25:00 (por si acaso)
#             '%Y-%m-%d %H:%M:%S',   # 2025-03-26 18:25:00 (formato deseado)
#             '%Y-%m-%d %H:%M',      # 2025-03-26 18:25
#             '%d-%m-%Y %H:%M',      # 26-03-2025 18:25
#             '%m/%d/%Y %H:%M',      # 03/26/2025 18:25 (formato americano)
#         ]
        
#         for fmt in formats:
#             try:
#                 dt = datetime.strptime(date_str, fmt)
#                 # Convertir a formato estándar para la base de datos
#                 return dt
#             except ValueError:
#                 continue
        
#         # Si falla, intentar con parser flexible de pandas
#         try:
#             return pd.to_datetime(date_str, dayfirst=True)  # dayfirst es clave para dd/mm
#         except:
#             logger.warning(f"Formato de fecha no reconocido: {date_str}")
#             return None

#     def _clean_column(self, col: str) -> str:
#         """Normaliza nombres de columnas para PostgreSQL"""
#         col = str(col).strip().lower()
#         col = col.replace(' ', '_').replace('-', '_')
#         return ''.join(c for c in col if c.isalnum() or c == '_')

#     def _transform_data(self, df: pd.DataFrame, file_name: str) -> pd.DataFrame:
#         """Transforma y limpia los datos filtrando por module='andes-movilidadtigo'"""
#         # Limpieza inicial de columnas
#         df.columns = [self._clean_column(col) for col in df.columns]
        
#         # Verificar existencia de columna module
#         if 'module' not in df.columns:
#             logger.error(f"Columna 'module' no encontrada en {file_name}")
#             return pd.DataFrame()  # Retorna DataFrame vacío
        
#         # Filtrar solo registros con module='andes-movilidadtigo'
#         filtered_df = df[df['module'].str.strip().str.lower() == 'andes-movilidadtigo'].copy()
        
#         if len(filtered_df) == 0:
#             logger.warning(f"No se encontraron registros con module='andes-movilidadtigo' en {file_name}")
#             return pd.DataFrame()
        
#         # Limpieza de columnas (manteniendo solo las necesarias)
#         if 'indices' in filtered_df.columns:
#             filtered_df = filtered_df.drop(columns=['indices'])
        
#         # Transformación de fechas (usando tu implementación mejorada)
#         if 'date' in filtered_df.columns:
#             filtered_df['date'] = filtered_df['date'].apply(self._parse_datetime)
        
#         # Limpieza de texto en columnas específicas
#         text_columns = [
#             'conn_id', 'agent_id', 'agent_name', 'agent_dni', 'skill_id',
#             'cod_act', 'description_cod_act', 'cod_act_2', 'description_cod_act_2',
#             'type_interaction', 'telephone', 'destiny', 'hang_up', 'customer_id',
#             'campaign_id', 'comments'
#         ]
        
#         for col in text_columns:
#             if col in filtered_df.columns:
#                 filtered_df[col] = filtered_df[col].astype(str).replace(['nan', 'None', 'NaT'], '').str.strip()
        
#         # Manejo de tiempo
#         if 'time' in filtered_df.columns:
#             filtered_df['time'] = pd.to_datetime(
#                 filtered_df['time'], 
#                 format='%H:%M:%S', 
#                 errors='coerce'
#             ).dt.time
        
#         # Añadir origen del archivo
#         filtered_df['archivo_origen'] = file_name
        
#         logger.info(f"Filtrado completado: {len(filtered_df)} registros válidos de {len(df)} totales")
#         return filtered_df

#     def _validate_data(self, df: pd.DataFrame) -> List[str]:
#         """Valida la calidad de los datos"""
#         errors = []
        
#         # Validar fechas
#         if 'date' in df.columns:
#             null_dates = df['date'].isna().sum()
#             if null_dates > 0:
#                 errors.append(f"{null_dates} registros con fecha inválida")
        
#         # Validar columnas requeridas
#         required_columns = {'conn_id', 'date', 'agent_id'}
#         missing_cols = required_columns - set(df.columns)
#         if missing_cols:
#             errors.append(f"Columnas requeridas faltantes: {missing_cols}")
        
#         return errors

#     def _process_single_file(self, file_path: str, is_retry: bool = False) -> Dict:
#         """Procesa un archivo individual y retorna resultado"""
#         file_name = os.path.basename(file_path)
#         result = {
#             'success': False,
#             'filename': file_name,
#             'records': 0,
#             'error': None,
#             'is_retry': is_retry
#         }
        
#         try:
#             # Leer archivo
#             encoding = self._detect_file_encoding(file_path)
#             df = pd.read_csv(file_path, encoding=encoding, engine='python', on_bad_lines='warn')
            
#             # Transformar datos
#             df = self._transform_data(df, file_name)
            
#             # Validar datos
#             validation_errors = self._validate_data(df)
#             if validation_errors:
#                 raise ValueError(f"Errores de validación: {', '.join(validation_errors)}")
            
#             # Guardar en base de datos
#             with self.engine.begin() as conn:
#                 # Si es reintento, eliminar registros previos
#                 if is_retry:
#                     conn.execute(
#                         text(f"DELETE FROM {TARGET_TABLE} WHERE archivo_origen = :filename"),
#                         {'filename': file_name}
#                     )
                
#                 # Insertar nuevos registros
#                 df.to_sql(
#                     TARGET_TABLE,
#                     conn,
#                     if_exists='append',
#                     index=False,
#                     method='multi',
#                     chunksize=10000
#                 )
                
#                 # Actualizar tabla de control
#                 conn.execute(
#                     text(f"""
#                         INSERT INTO {CONTROL_TABLE} 
#                         (nombre_archivo, registros_insertados, estado) 
#                         VALUES (:filename, :registros, 'completado')
#                         ON CONFLICT (nombre_archivo) DO UPDATE SET
#                             registros_insertados = EXCLUDED.registros_insertados,
#                             estado = 'completado',
#                             fecha_procesado = CURRENT_TIMESTAMP,
#                             intentos = CAST({CONTROL_TABLE}.intentos AS INTEGER) + 1,
#                             error_message = NULL
#                     """),
#                     {'filename': file_name, 'registros': len(df)}
#                 )
            
#             result['success'] = True
#             result['records'] = len(df)
            
#         except Exception as e:
#             result['error'] = str(e)
            
#             # Registrar fallo en BD
#             try:
#                 with self.engine.connect() as conn:
#                     conn.execute(
#                         text(f"""
#                             INSERT INTO {CONTROL_TABLE} 
#                             (nombre_archivo, registros_insertados, estado, error_message) 
#                             VALUES (:filename, 0, 'fallido', :error)
#                             ON CONFLICT (nombre_archivo) DO UPDATE SET
#                                 registros_insertados = 0,
#                                 estado = 'fallido',
#                                 fecha_procesado = CURRENT_TIMESTAMP,
#                                 intentos = CAST({CONTROL_TABLE}.intentos AS INTEGER) + 1,
#                                 error_message = EXCLUDED.error_message
#                         """),
#                         {'filename': file_name, 'error': str(e)[:500]}
#                     )
#             except:
#                 pass
        
#         return result

#     def get_files_to_process(self, folder_path: str) -> Dict[str, List[str]]:
#         """Identifica archivos nuevos y fallidos para procesar"""
#         try:
#             # Obtener todos los archivos CSV
#             all_files = [
#                 f for f in os.listdir(folder_path) 
#                 if f.lower().endswith('.csv') and os.path.isfile(os.path.join(folder_path, f))
#             ]
            
#             if not all_files:
#                 return {'new': [], 'failed': [], 'processed': []}
            
#             # Consultar estado en base de datos
#             with self.engine.connect() as conn:
#                 processed = pd.read_sql(
#                     f"SELECT nombre_archivo FROM {CONTROL_TABLE} WHERE estado = 'completado'", 
#                     conn
#                 )['nombre_archivo'].tolist()
                
#                 failed = pd.read_sql(
#                     f"SELECT nombre_archivo FROM {CONTROL_TABLE} WHERE estado = 'fallido'", 
#                     conn
#                 )['nombre_archivo'].tolist()
            
#             new_files = [f for f in all_files if f not in processed and f not in failed]
#             failed_files = [f for f in failed if f in all_files]
#             processed_files = [f for f in processed if f in all_files]
            
#             return {
#                 'new': new_files, 
#                 'failed': failed_files,
#                 'processed': processed_files
#             }
            
#         except Exception as e:
#             st.error(f"Error identificando archivos: {str(e)}")
#             return {'new': [], 'failed': [], 'processed': []}

# def select_folder():
#     """Abre explorador para seleccionar carpeta"""
#     root = Tk()
#     root.withdraw()
#     root.wm_attributes('-topmost', 1)
    
#     folder_path = filedialog.askdirectory(
#         title="Seleccionar carpeta con archivos CSV de Wolkvox"
#     )
    
#     root.destroy()
#     return folder_path if folder_path else None



# def main():
#     """Función principal de la aplicación Streamlit"""
    
#     # Header con logo (si existe)
#     col1, col2, col3 = st.columns([1, 2, 1])
#     with col2:
#         st.markdown("<h1 class='section-title'>🗂️ SIAMM - Carga Wolkvox</h1>", unsafe_allow_html=True)
#         st.markdown("<p style='text-align: center; color: #666; font-size: 1.1rem;'>Sistema de Ingestión de Datos - Tipificación 3</p>", unsafe_allow_html=True)

#     # Sidebar
#     with st.sidebar:
#         st.markdown("<h2 class='sidebar-title'>🔧 Configuración</h2>", unsafe_allow_html=True)
        
#         # Mostrar configuración de BD
#         st.markdown("**📊 Base de Datos:**")
#         st.code(f"""
# Host: {DB_CONFIG['host']}
# Puerto: {DB_CONFIG['port']}
# Base de Datos: {DB_CONFIG['database']}
# Usuario: {DB_CONFIG['user']}
#         """)
        
#         st.markdown("---")
#         st.markdown("**📁 Tablas:**")
#         st.code(f"""
# Destino: {TARGET_TABLE}
# Control: {CONTROL_TABLE}
#         """)

#     # Inicializar procesador
#     if 'processor' not in st.session_state:
#         with st.spinner("Inicializando conexión a base de datos..."):
#             st.session_state.processor = WolkvoxDataIngestor()

#     processor = st.session_state.processor

#     # Verificar conexión
#     if not processor.connection_status:
#         st.markdown(f"""
#         <div class='error-box'>
#             <h3>❌ Error de Conexión</h3>
#             <p>No se pudo conectar a la base de datos:</p>
#             <code>{processor.error_message}</code>
#             <p><strong>Verifica:</strong></p>
#             <ul>
#                 <li>Que PostgreSQL esté ejecutándose</li>
#                 <li>Las credenciales en el archivo .env</li>
#                 <li>La conectividad de red</li>
#             </ul>
#         </div>
#         """, unsafe_allow_html=True)
#         return

#     # Conexión exitosa
#     st.markdown("""
#     <div class='success-box'>
#         <h3>✅ Conexión Establecida</h3>
#         <p>Sistema listo para procesar archivos CSV de Wolkvox</p>
#     </div>
#     """, unsafe_allow_html=True)

#     # Selección de carpeta
#     st.markdown("<h2 class='section-header'>📂 Selección de Carpeta</h2>", unsafe_allow_html=True)
    
#     col1, col2 = st.columns([3, 1])
#     with col1:
#         if 'selected_folder' not in st.session_state:
#             st.session_state.selected_folder = None
        
#         if st.session_state.selected_folder:
#             st.info(f"📁 Carpeta seleccionada: `{st.session_state.selected_folder}`")
#         else:
#             st.warning("⚠️ No se ha seleccionado ninguna carpeta")
    
#     with col2:
#         if st.button("🗂️ Seleccionar Carpeta", type="primary"):
#             folder_path = select_folder()
#             if folder_path:
#                 st.session_state.selected_folder = folder_path
#                 st.rerun()

#     # Análisis de archivos
#     if st.session_state.selected_folder:
#         st.markdown("<h2 class='section-header'>📋 Análisis de Archivos</h2>", unsafe_allow_html=True)
        
#         with st.spinner("Analizando archivos..."):
#             files = processor.get_files_to_process(st.session_state.selected_folder)
        
#         # Estadísticas
#         col1, col2, col3, col4 = st.columns(4)
        
#         with col1:
#             st.metric("📄 Archivos Nuevos", len(files['new']), delta=None)
        
#         with col2:
#             st.metric("❌ Archivos Fallidos", len(files['failed']), delta=None)
        
#         with col3:
#             st.metric("✅ Ya Procesados", len(files['processed']), delta=None)
        
#         with col4:
#             total = len(files['new']) + len(files['failed'])
#             st.metric("🎯 Total a Procesar", total, delta=None)

#         # Detalles de archivos
#         if files['new'] or files['failed'] or files['processed']:
            
#             # Pestañas para organizar información
#             tab1, tab2, tab3 = st.tabs(["📄 Nuevos", "❌ Fallidos", "✅ Procesados"])
            
#             with tab1:
#                 if files['new']:
#                     st.markdown("**Archivos nuevos encontrados:**")
#                     for i, file in enumerate(files['new'], 1):
#                         st.markdown(f"<span class='file-status-new'>{i}. {file}</span>", unsafe_allow_html=True)
#                 else:
#                     st.info("No hay archivos nuevos para procesar")
            
#             with tab2:
#                 if files['failed']:
#                     st.markdown("**Archivos que fallaron previamente:**")
#                     for i, file in enumerate(files['failed'], 1):
#                         st.markdown(f"<span class='file-status-failed'>{i}. {file}</span>", unsafe_allow_html=True)
#                 else:
#                     st.info("No hay archivos fallidos")
            
#             with tab3:
#                 if files['processed']:
#                     st.markdown("**Archivos ya procesados exitosamente:**")
#                     for i, file in enumerate(files['processed'], 1):
#                         st.markdown(f"<span class='file-status-processed'>{i}. {file}</span>", unsafe_allow_html=True)
#                 else:
#                     st.info("No hay archivos procesados previamente")

#         # Botón de procesamiento
#         total_to_process = len(files['new']) + len(files['failed'])
        
#         if total_to_process > 0:
#             st.markdown("<h2 class='section-header'>🚀 Procesamiento</h2>", unsafe_allow_html=True)
            
#             if st.button(f"🎯 Procesar {total_to_process} Archivos", type="primary", use_container_width=True):
                
#                 # Contenedor para el progreso
#                 progress_container = st.container()
#                 log_container = st.container()
                
#                 with progress_container:
#                     # Barra de progreso general
#                     progress_bar = st.progress(0)
#                     status_text = st.empty()
                    
#                     # Métricas en tiempo real
#                     col1, col2, col3 = st.columns(3)
#                     with col1:
#                         success_metric = st.empty()
#                     with col2:
#                         error_metric = st.empty()
#                     with col3:
#                         current_file = st.empty()

#                 with log_container:
#                     st.markdown("### 📝 Log de Procesamiento")
#                     log_placeholder = st.empty()
#                     log_messages = []

#                 # Procesar archivos
#                 successful = 0
#                 failed = 0
#                 total_processed = 0
                
#                 # Procesar archivos nuevos
#                 for i, filename in enumerate(files['new']):
#                     total_processed += 1
#                     progress = total_processed / total_to_process
#                     progress_bar.progress(progress)
#                     status_text.text(f"Procesando archivo nuevo {i+1}/{len(files['new'])}: {filename}")
#                     current_file.metric("📁 Archivo Actual", filename)
                    
#                     log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] Procesando: {filename}")
#                     log_placeholder.markdown(
#                         f"<div class='log-container'>{'<br>'.join(log_messages[-10:])}</div>", 
#                         unsafe_allow_html=True
#                     )
                    
#                     result = processor._process_single_file(
#                         os.path.join(st.session_state.selected_folder, filename)
#                     )
                    
#                     if result['success']:
#                         successful += 1
#                         log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Éxito: {result['records']} registros")
#                     else:
#                         failed += 1
#                         log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Error: {result['error'][:100]}...")
                    
#                     success_metric.metric("✅ Exitosos", successful)
#                     error_metric.metric("❌ Fallidos", failed)
                
#                 # Procesar archivos fallidos (reintentos)
#                 for i, filename in enumerate(files['failed']):
#                     total_processed += 1
#                     progress = total_processed / total_to_process
#                     progress_bar.progress(progress)
#                     status_text.text(f"Reintentando archivo {i+1}/{len(files['failed'])}: {filename}")
#                     current_file.metric("🔄 Reintentando", filename)
                    
#                     log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] Reintentando: {filename}")
#                     log_placeholder.markdown(
#                         f"<div class='log-container'>{'<br>'.join(log_messages[-10:])}</div>", 
#                         unsafe_allow_html=True
#                     )
                    
#                     result = processor._process_single_file(
#                         os.path.join(st.session_state.selected_folder, filename),
#                         is_retry=True
#                     )
                    
#                     if result['success']:
#                         successful += 1
#                         log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Recuperado: {result['records']} registros")
#                     else:
#                         failed += 1
#                         log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Falló nuevamente: {result['error'][:100]}...")
                    
#                     success_metric.metric("✅ Exitosos", successful)
#                     error_metric.metric("❌ Fallidos", failed)

#                 # Finalizar procesamiento
#                 progress_bar.progress(1.0)
#                 status_text.text("🎉 Procesamiento completado")
#                 current_file.empty()
                
#                 # Mostrar resumen final
#                 if successful == total_to_process:
#                     st.markdown(f"""
#                     <div class='success-box'>
#                         <h3>🎉 Procesamiento Completado Exitosamente</h3>
#                         <p><strong>Todos los archivos fueron procesados correctamente:</strong></p>
#                         <ul>
#                             <li>✅ Archivos exitosos: {successful}</li>
#                             <li>📊 Total de archivos: {total_to_process}</li>
#                         </ul>
#                     </div>
#                     """, unsafe_allow_html=True)
#                 elif successful > 0:
#                     st.markdown(f"""
#                     <div class='warning-box'>
#                         <h3>⚠️ Procesamiento Completado con Advertencias</h3>
#                         <p><strong>Resumen del procesamiento:</strong></p>
#                         <ul>
#                             <li>✅ Archivos exitosos: {successful}</li>
#                             <li>❌ Archivos fallidos: {failed}</li>
#                             <li>📊 Total procesados: {total_to_process}</li>
#                         </ul>
#                         <p><strong>Recomendación:</strong> Revisa los logs para identificar los errores en los archivos fallidos.</p>
#                     </div>
#                     """, unsafe_allow_html=True)
#                 else:
#                     st.markdown(f"""
#                     <div class='error-box'>
#                         <h3>❌ Procesamiento Fallido</h3>
#                         <p><strong>Ningún archivo pudo ser procesado exitosamente:</strong></p>
#                         <ul>
#                             <li>❌ Archivos fallidos: {failed}</li>
#                             <li>📊 Total intentados: {total_to_process}</li>
#                         </ul>
#                         <p><strong>Sugerencias:</strong></p>
#                         <ul>
#                             <li>Verifica el formato de los archivos CSV</li>
#                             <li>Revisa los logs de error para más detalles</li>
#                             <li>Confirma que las columnas requeridas estén presentes</li>
#                         </ul>
#                     </div>
#                     """, unsafe_allow_html=True)
                
#                 # Botón para limpiar y reiniciar
#                 if st.button("🔄 Procesar Nuevos Archivos", type="secondary"):
#                     st.rerun()
        
#         elif total_to_process == 0 and files['processed']:
#             st.markdown("""
#             <div class='info-box'>
#                 <h3>ℹ️ Todos los Archivos Ya Están Procesados</h3>
#                 <p>Todos los archivos CSV en la carpeta seleccionada ya han sido procesados exitosamente.</p>
#                 <p><strong>Opciones:</strong></p>
#                 <ul>
#                     <li>Selecciona una carpeta diferente con archivos nuevos</li>
#                     <li>Agrega nuevos archivos CSV a la carpeta actual y actualiza</li>
#                 </ul>
#             </div>
#             """, unsafe_allow_html=True)
        
#         else:
#             st.markdown("""
#             <div class='info-box'>
#                 <h3>📂 Carpeta Vacía</h3>
#                 <p>No se encontraron archivos CSV en la carpeta seleccionada.</p>
#                 <p><strong>Verifica que:</strong></p>
#                 <ul>
#                     <li>La carpeta contenga archivos con extensión .csv</li>
#                     <li>Los archivos no estén en subcarpetas</li>
#                     <li>Tengas permisos de lectura en la carpeta</li>
#                 </ul>
#             </div>
#             """, unsafe_allow_html=True)

#     # Footer con información adicional
#     st.markdown("---")
#     st.markdown("""
#     <div style='text-align: center; color: #666; padding: 20px 0;'>
#         <p><strong>🗂️ Sistema SIAMM - Carga Wolkvox Tipificación 3</strong></p>
#         <p>Procesador de archivos CSV de tipificaciones Wolkvox hacia PostgreSQL</p>
#         <p><em>Versión Streamlit - Desarrollado para AndesBPO</em></p>
#     </div>
#     """, unsafe_allow_html=True)
    
# if __name__ == "__main__":
#     main()

import streamlit as st
import os
from pathlib import Path
from tkinter import Tk, filedialog
from datetime import datetime
from dotenv import load_dotenv
from src.utils.data_ingestor_tip3 import DataIngestorTip3
from src.utils.fondo import set_background

# Configuración de página
st.set_page_config(
    page_title="SIAMM - Carga Wolkvox",
    page_icon="src/utils/favicon-114x114.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos CSS
st.markdown("""
    <style>
    [data-testid=stSidebar] { background: #a5d6a7 !important; padding: 20px 10px; }
    .main-container { padding: 2rem; background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%); min-height: 100vh; }
    .metric-card { background: white; padding: 20px; border-radius: 15px; box-shadow: 0 8px 32px rgba(0,0,0,0.1); margin: 10px 0; transition: transform 0.3s ease; }
    .metric-card:hover { transform: translateY(-5px); }
    .section-title { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-size: 2rem; margin: 30px 0 20px 0; text-align: center; }
    .section-header { color: #2e7d32; border-bottom: 2px solid #a5d6a7; padding-bottom: 0.5rem; margin-bottom: 1rem; }
    .stButton > button { background: linear-gradient(120deg, #a5d6a7 0%, #2ecc71 100%); color: white; border: none; border-radius: 25px; padding: 12px 30px; transition: all 0.3s ease; }
    .stButton > button:hover { transform: translateY(-2px); box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6); }
    .stProgress > div > div > div > div { background-color: #2e7d32; }
    .sidebar-title { color: #2c3e50; font-size: 1.2rem; margin-bottom: 1rem; font-weight: 600; }
    .success-box { background: #d4edda; border: 1px solid #c3e6cb; border-radius: 8px; padding: 15px; margin: 10px 0; }
    .error-box { background: #f8d7da; border: 1px solid #f5c6cb; border-radius: 8px; padding: 15px; margin: 10px 0; }
    .info-box { background: #d1ecf1; border: 1px solid #b3d4d8; border-radius: 8px; padding: 15px; margin: 10px 0; }
    .file-status-new { color: #28a745; font-weight: bold; }
    .file-status-modified { color: #ffc107; font-weight: bold; }
    .file-status-existing { color: #6c757d; }
    .directory-input { background: white; padding: 15px; border-radius: 10px; margin-bottom: 20px; }
    </style>
""", unsafe_allow_html=True)

# Logo y fondo
st.image("src/utils/logo-andesbpo-359x143.png", width=150)
set_background("src/utils/bg-seccion.png")

# Cargar variables de entorno
load_dotenv()

# Configuración de base de datos desde .env
DB_CONFIG = {
    'host': os.getenv('DB_SIMM_HOST', 'localhost'),
    'database': os.getenv('DB_SIMM_NAME', 'SIMM'),
    'user': os.getenv('DB_SIMM_USER', 'postgres'),
    'password': os.getenv('DB_SIMM_PASSWORD', 'admin'),
    'port': os.getenv('DB_SIMM_PORT', '5432')
}

def select_folder():
    """Abre explorador para seleccionar carpeta"""
    root = Tk()
    root.withdraw()
    root.wm_attributes('-topmost', 1)
    
    folder_path = filedialog.askdirectory(
        title="Seleccionar carpeta con archivos CSV"
    )
    
    root.destroy()
    return folder_path if folder_path else None

def show_tipificacion_3_ui():
    """Muestra la UI para Tipificación 3"""
    st.markdown("<h1 class='section-title'>🗂️ SIAMM - Carga Wolkvox Tipificación 3</h1>", unsafe_allow_html=True)
    
    # Inicializar procesador
    if 'processor_tip3' not in st.session_state:
        with st.spinner("Inicializando conexión a base de datos..."):
            st.session_state.processor_tip3 = DataIngestorTip3(DB_CONFIG)

    processor = st.session_state.processor_tip3

    # Verificar conexión
    if not processor.connection_status:
        st.markdown(f"""
        <div class='error-box'>
            <h3>❌ Error de Conexión</h3>
            <p>No se pudo conectar a la base de datos:</p>
            <code>{processor.error_message}</code>
            <p><strong>Verifica:</strong></p>
            <ul>
                <li>Que PostgreSQL esté ejecutándose</li>
                <li>Las credenciales en el archivo .env</li>
                <li>La conectividad de red</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        return

    # Conexión exitosa
    st.markdown("""
    <div class='success-box'>
        <h3>✅ Conexión Establecida</h3>
        <p>Sistema listo para procesar archivos CSV de Wolkvox</p>
    </div>
    """, unsafe_allow_html=True)

    # Selección de carpeta
    st.markdown("<h2 class='section-header'>📂 Selección de Carpeta</h2>", unsafe_allow_html=True)
    
    col1, col2 = st.columns([3, 1])
    with col1:
        if 'selected_folder_tip3' not in st.session_state:
            st.session_state.selected_folder_tip3 = None
        
        if st.session_state.selected_folder_tip3:
            st.info(f"📁 Carpeta seleccionada: `{st.session_state.selected_folder_tip3}`")
        else:
            st.warning("⚠️ No se ha seleccionado ninguna carpeta")
    
    with col2:
        if st.button("🗂️ Seleccionar Carpeta", type="primary", key="select_folder_tip3"):
            folder_path = select_folder()
            if folder_path:
                st.session_state.selected_folder_tip3 = folder_path
                st.rerun()

    # Análisis de archivos
    if st.session_state.selected_folder_tip3:
        st.markdown("<h2 class='section-header'>📋 Análisis de Archivos</h2>", unsafe_allow_html=True)
        
        with st.spinner("Analizando archivos..."):
            files = processor.get_files_to_process(st.session_state.selected_folder_tip3)
        
        # Estadísticas
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📄 Archivos Nuevos", len(files['new']), delta=None)
        
        with col2:
            st.metric("❌ Archivos Fallidos", len(files['failed']), delta=None)
        
        with col3:
            st.metric("✅ Ya Procesados", len(files['processed']), delta=None)
        
        with col4:
            total = len(files['new']) + len(files['failed'])
            st.metric("🎯 Total a Procesar", total, delta=None)

        # Detalles de archivos
        if files['new'] or files['failed'] or files['processed']:
            
            # Pestañas para organizar información
            tab1, tab2, tab3 = st.tabs(["📄 Nuevos", "❌ Fallidos", "✅ Procesados"])
            
            with tab1:
                if files['new']:
                    st.markdown("**Archivos nuevos encontrados:**")
                    for i, file in enumerate(files['new'], 1):
                        st.markdown(f"<span class='file-status-new'>{i}. {file}</span>", unsafe_allow_html=True)
                else:
                    st.info("No hay archivos nuevos para procesar")
            
            with tab2:
                if files['failed']:
                    st.markdown("**Archivos que fallaron previamente:**")
                    for i, file in enumerate(files['failed'], 1):
                        st.markdown(f"<span class='file-status-failed'>{i}. {file}</span>", unsafe_allow_html=True)
                else:
                    st.info("No hay archivos fallidos")
            
            with tab3:
                if files['processed']:
                    st.markdown("**Archivos ya procesados exitosamente:**")
                    for i, file in enumerate(files['processed'], 1):
                        st.markdown(f"<span class='file-status-processed'>{i}. {file}</span>", unsafe_allow_html=True)
                else:
                    st.info("No hay archivos procesados previamente")

        # Botón de procesamiento
        total_to_process = len(files['new']) + len(files['failed'])
        
        if total_to_process > 0:
            st.markdown("<h2 class='section-header'>🚀 Procesamiento</h2>", unsafe_allow_html=True)
            
            if st.button(f"🎯 Procesar {total_to_process} Archivos", type="primary", use_container_width=True, key="process_tip3"):
                
                # Contenedor para el progreso
                progress_container = st.container()
                log_container = st.container()
                
                with progress_container:
                    # Barra de progreso general
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    # Métricas en tiempo real
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        success_metric = st.empty()
                    with col2:
                        error_metric = st.empty()
                    with col3:
                        current_file = st.empty()

                with log_container:
                    st.markdown("### 📝 Log de Procesamiento")
                    log_placeholder = st.empty()
                    log_messages = []

                # Procesar archivos
                successful = 0
                failed = 0
                total_processed = 0
                
                # Procesar archivos nuevos
                for i, filename in enumerate(files['new']):
                    total_processed += 1
                    progress = total_processed / total_to_process
                    progress_bar.progress(progress)
                    status_text.text(f"Procesando archivo nuevo {i+1}/{len(files['new'])}: {filename}")
                    current_file.metric("📁 Archivo Actual", filename)
                    
                    log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] Procesando: {filename}")
                    log_placeholder.markdown(
                        f"<div class='log-container'>{'<br>'.join(log_messages[-10:])}</div>", 
                        unsafe_allow_html=True
                    )
                    
                    result = processor._process_single_file(
                        os.path.join(st.session_state.selected_folder_tip3, filename)
                    )
                    
                    if result['success']:
                        successful += 1
                        log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Éxito: {result['records']} registros")
                    else:
                        failed += 1
                        log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Error: {result['error'][:100]}...")
                    
                    success_metric.metric("✅ Exitosos", successful)
                    error_metric.metric("❌ Fallidos", failed)
                
                # Procesar archivos fallidos (reintentos)
                for i, filename in enumerate(files['failed']):
                    total_processed += 1
                    progress = total_processed / total_to_process
                    progress_bar.progress(progress)
                    status_text.text(f"Reintentando archivo {i+1}/{len(files['failed'])}: {filename}")
                    current_file.metric("🔄 Reintentando", filename)
                    
                    log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] Reintentando: {filename}")
                    log_placeholder.markdown(
                        f"<div class='log-container'>{'<br>'.join(log_messages[-10:])}</div>", 
                        unsafe_allow_html=True
                    )
                    
                    result = processor._process_single_file(
                        os.path.join(st.session_state.selected_folder_tip3, filename),
                        is_retry=True
                    )
                    
                    if result['success']:
                        successful += 1
                        log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Recuperado: {result['records']} registros")
                    else:
                        failed += 1
                        log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Falló nuevamente: {result['error'][:100]}...")
                    
                    success_metric.metric("✅ Exitosos", successful)
                    error_metric.metric("❌ Fallidos", failed)

                # Finalizar procesamiento
                progress_bar.progress(1.0)
                status_text.text("🎉 Procesamiento completado")
                current_file.empty()
                
                # Mostrar resumen final
                if successful == total_to_process:
                    st.markdown(f"""
                    <div class='success-box'>
                        <h3>🎉 Procesamiento Completado Exitosamente</h3>
                        <p><strong>Todos los archivos fueron procesados correctamente:</strong></p>
                        <ul>
                            <li>✅ Archivos exitosos: {successful}</li>
                            <li>📊 Total de archivos: {total_to_process}</li>
                        </ul>
                    </div>
                    """, unsafe_allow_html=True)
                elif successful > 0:
                    st.markdown(f"""
                    <div class='warning-box'>
                        <h3>⚠️ Procesamiento Completado con Advertencias</h3>
                        <p><strong>Resumen del procesamiento:</strong></p>
                        <ul>
                            <li>✅ Archivos exitosos: {successful}</li>
                            <li>❌ Archivos fallidos: {failed}</li>
                            <li>📊 Total procesados: {total_to_process}</li>
                        </ul>
                        <p><strong>Recomendación:</strong> Revisa los logs para identificar los errores en los archivos fallidos.</p>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div class='error-box'>
                        <h3>❌ Procesamiento Fallido</h3>
                        <p><strong>Ningún archivo pudo ser procesado exitosamente:</strong></p>
                        <ul>
                            <li>❌ Archivos fallidos: {failed}</li>
                            <li>📊 Total intentados: {total_to_process}</li>
                        </ul>
                        <p><strong>Sugerencias:</strong></p>
                        <ul>
                            <li>Verifica el formato de los archivos CSV</li>
                            <li>Revisa los logs de error para más detalles</li>
                            <li>Confirma que las columnas requeridas estén presentes</li>
                        </ul>
                    </div>
                    """, unsafe_allow_html=True)
                
                # Botón para limpiar y reiniciar
                if st.button("🔄 Procesar Nuevos Archivos", type="secondary", key="retry_tip3"):
                    st.rerun()
        
        elif total_to_process == 0 and files['processed']:
            st.markdown("""
            <div class='info-box'>
                <h3>ℹ️ Todos los Archivos Ya Están Procesados</h3>
                <p>Todos los archivos CSV en la carpeta seleccionada ya han sido procesados exitosamente.</p>
                <p><strong>Opciones:</strong></p>
                <ul>
                    <li>Selecciona una carpeta diferente con archivos nuevos</li>
                    <li>Agrega nuevos archivos CSV a la carpeta actual y actualiza</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)
        
        else:
            st.markdown("""
            <div class='info-box'>
                <h3>📂 Carpeta Vacía</h3>
                <p>No se encontraron archivos CSV en la carpeta seleccionada.</p>
                <p><strong>Verifica que:</strong></p>
                <ul>
                    <li>La carpeta contenga archivos con extensión .csv</li>
                    <li>Los archivos no estén en subcarpetas</li>
                    <li>Tengas permisos de lectura en la carpeta</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)

def show_cdr_ui():
    """Muestra la UI para CDR"""
    st.markdown("<h1 class='section-title'>📞 CDR - Próximamente</h1>", unsafe_allow_html=True)
    st.info("La funcionalidad para procesar CDR estará disponible en futuras actualizaciones.")

def show_campana_ui():
    """Muestra la UI para Campaña"""
    st.markdown("<h1 class='section-title'>📢 Campaña - Próximamente</h1>", unsafe_allow_html=True)
    st.info("La funcionalidad para procesar datos de Campaña estará disponible en futuras actualizaciones.")

def main():
    """Función principal de la aplicación Streamlit"""
    
    # Sidebar con opciones
    with st.sidebar:
        st.markdown("**📌 Seleccione Tipo de Datos**")
        
        # Opciones en el sidebar
        option = st.radio(
            "Tipo de datos a procesar:",
            ["Tipificación 3", "CDR", "Campaña"],
            index=0
        )

    # Mostrar la UI correspondiente según la opción seleccionada
    if option == "Tipificación 3":
        show_tipificacion_3_ui()
    elif option == "CDR":
        show_cdr_ui()
    elif option == "Campaña":
        show_campana_ui()

    # Footer con información adicional
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; padding: 20px 0;'>
        <p><strong>🗂️ Sistema SIAMM - Carga Wolkvox</strong></p>
        <p>Procesador de archivos CSV hacia PostgreSQL</p>
        <p><em>Versión Streamlit - Desarrollado para AndesBPO</em></p>
    </div>
    """, unsafe_allow_html=True)
    
if __name__ == "__main__":
    main()