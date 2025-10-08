import streamlit as st
import pandas as pd
import os
import tempfile
from datetime import datetime
from sqlalchemy import inspect, text
from src.database.postgres import get_engine, DatabaseManager
from src.utils.limpieza_archivo import preparar_datos
from src.utils.limpieza_sms import preparar_datos_sms
from src.utils.limpieza_pagos import procesar_pagos
from src.utils.limpieza_bases import preparar_datos_bases
from assets.fondo import set_background
import logging
import pathlib
from streamlit_option_menu import option_menu

# ==============================================================================
# CONFIGURACIÓN DEL LOGGING
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app_simm_actualizacion.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==============================================================================
# CONFIGURACIÓN INICIAL
# ==============================================================================
st.set_page_config(
    page_title="SIAMM - Actualización",
    page_icon="assets/images/favicon-114x114.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Cargar CSS global
css_path = pathlib.Path("assets/css/global.css")
if css_path.exists():
    with open(css_path, encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# Logo y fondo
st.image("assets/images/logo-andesbpo-359x143.png", width=350)
set_background("assets/images/bg-seccion.png")

st.markdown("<h1 class='section-title'>Actualización</h1>", unsafe_allow_html=True)

st.markdown("---")


# ==============================================================================
# CLASE BASE: DataProcessor
# ==============================================================================
class DataProcessor:
    """Clase base para el procesamiento de datos"""
    
    def __init__(self, engine, config):
        self.engine = engine
        self.config = config
        self.df_procesado = None
        self.df_errores = None
        self.nuevos = None
        
    def _validar_estructura(self, df):
        """Valida que las columnas coincidan con la estructura de la BD"""
        inspector = inspect(self.engine)
        columnas_bd = [col['name'] for col in inspector.get_columns(self.config['table_name'])]
        columnas_archivo = df.columns.tolist()
        
        set_bd = set(columnas_bd)
        set_archivo = set(columnas_archivo)
        
        if set_bd != set_archivo:
            st.error("❌ Error de estructura: Las columnas no coinciden con la base de datos")
            st.stop()
            
        return True
    
    def _buscar_duplicados(self):
        """Integra la función utilitaria de verificación de duplicados"""
        nuevos = verificar_duplicados(
            self.engine,
            self.df_procesado,
            self.config['table_name'],
            self.config['id_column']
        )
        self.nuevos = nuevos
        self.duplicados = self.df_procesado.shape[0] - len(nuevos)
        
        st.write(f"📊 Registros nuevos detectados: {len(nuevos)}")
        st.write(f"📋 Registros duplicados detectados: {self.duplicados}")
        
        return nuevos
    
    def procesar_archivo(self, archivo):
        """Ejecuta solo el procesamiento y validación (sin carga)"""
        with st.status("🔄 Procesando archivo ...", expanded=True) as status:
            try:
                logger.info(f"Procesando archivo: {archivo.name}")
                # Procesamiento individual
                self.df_procesado, self.df_errores, mensaje = self._procesar_archivo(archivo)
                
                if self.df_procesado.empty:
                    logger.warning("⚠️ El archivo no contiene datos válidos")
                    st.warning("⚠️ El archivo no contiene datos válidos")
                    return False
                
                # Validación estructural
                if not self._validar_estructura(self.df_procesado):
                    logger.error("❌ Error en la estructura del archivo")
                    st.error("❌ Error en la estructura del archivo")
                    return False
                
                # Búsqueda de duplicados
                self.nuevos = self._buscar_duplicados()
                
                status.update(label="✅ Procesamiento completado", state="complete")
                return True
                
            except Exception as e:
                status.update(label="❌ Error en el proceso", state="error")
                logger.error(f"Error crítico: {str(e)}")
                st.error(f"Error crítico: {str(e)}")
                return False

    def _mostrar_resultados(self):
        """Muestra los resultados del procesamiento"""
        cols = st.columns(3)
        cols[0].metric("✅ Válidos", len(self.df_procesado))
        cols[1].metric("⚠️ Errores", len(self.df_errores))
        
    def _mostrar_errores(self):
        """Muestra y permite descargar los errores"""
        if not self.df_errores.empty:
            with st.expander("🚨 Detalle de errores", expanded=False):
                st.dataframe(self.df_errores)
                csv = self.df_errores.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Descargar reporte",
                    data=csv,
                    file_name=f"errores_{self.config['table_name']}.csv",
                    mime="text/csv"
                )
    
    def ejecutar_flujo(self, archivo):
        """Ejecuta el flujo completo de procesamiento"""
        with st.status(f"🏁 Procesando {self.config['table_name']}", expanded=True) as status:
            try:
                # Procesamiento inicial
                self.df_procesado, self.df_errores, mensaje = self._procesar_archivo(archivo)
                
                if self.df_procesado.empty:
                    st.warning(mensaje)
                    return
                
                # Validación estructural
                if not self._validar_estructura(self.df_procesado):
                    st.error("Error de estructura en los datos")
                    return
                
                # Búsqueda de duplicados
                self.nuevos = self._buscar_duplicados()
                
                # Carga de datos
                if not self.nuevos.empty:
                    self._cargar_datos()
                
                # Mostrar resultados finales
                self._mostrar_resultados()
                self._mostrar_errores()
                status.update(label="✅ Proceso completado", state="complete")
                
            except Exception as e:
                status.update(label="❌ Error en el proceso", state="error")
                st.error(f"Error crítico: {str(e)}")
                st.stop()
    
    def _cargar_datos(self):
        """Carga los datos en la base de datos con tolerancia a fallos de UI"""
        if self.nuevos is None or self.nuevos.empty:
            logger.warning("⚠️ No hay registros nuevos para cargar.")
            st.warning("⚠️ No hay registros nuevos para cargar.")
            return False

        try:
            total_registros = len(self.nuevos)
            chunk_size = 1000  # más pequeño para evitar demoras largas
            chunks = [self.nuevos[i:i + chunk_size] for i in range(0, total_registros, chunk_size)]

            # Variables de UI que podrían fallar si el cliente se desconecta
            status_text = st.empty()
            progress_bar = st.progress(0)
            registros_insertados = 0

            with self.engine.begin() as conn:
                try:
                    conn.execute(text(f"ALTER TABLE {self.config['table_name']} DISABLE TRIGGER ALL"))
                except Exception as e:
                    print("⚠️ No se pudieron deshabilitar los triggers:", e)

                for i, chunk in enumerate(chunks):
                    # Insertar el chunk en la BD
                    chunk.to_sql(
                        name=self.config['table_name'],
                        con=conn,
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=chunk_size
                    )
                    registros_insertados += len(chunk)

                    # Solo actualizar la interfaz cada 5 chunks para evitar fallos
                    if i % 5 == 0 or i == len(chunks) - 1:
                        progress = (i + 1) / len(chunks)
                        try:
                            progress_bar.progress(progress)
                            status_text.markdown(f"""
                                **Progreso de carga:**  
                                • Lotes procesados: `{i+1}/{len(chunks)}`  
                                • Registros insertados: `{registros_insertados}/{total_registros}`
                            """)
                        except Exception as e:
                            print("❌ WebSocket posiblemente cerrado. No se puede actualizar UI:", e)

                try:
                    conn.execute(text(f"ALTER TABLE {self.config['table_name']} ENABLE TRIGGER ALL"))
                except Exception as e:
                    print("⚠️ No se pudieron reactivar los triggers:", e)

            # Limpiar elementos visuales
            try:
                progress_bar.empty()
                status_text.empty()
            except:
                pass

            st.cache_data.clear()

            # Mostrar resumen final
            try:
                st.success(f"✅ Carga exitosa: {registros_insertados} registros nuevos insertados")
                st.metric("Velocidad estimada", f"{(registros_insertados / max(len(chunks), 1)):.2f} registros por lote")
            except:
                print("⚠️ WebSocket cerrado. No se puede mostrar resumen en UI.")

            return True

        except Exception as e:
            try:
                progress_bar.empty()
                status_text.error(f"❌ Error en la carga: {str(e)}")
            except:
                print("Error crítico durante la carga:", str(e))
            raise

# ==============================================================================
# CLASES ESPECÍFICAS POR MÓDULO
# ==============================================================================
class GestionesProcessor(DataProcessor):
    """Procesador específico para gestiones"""
        
    def __init__(self, engine):
        config = {
            'table_name': 'gestiones',
            'mapeo_columnas': {
                'id registro': 'id_registro',
                'Id Gestion Campaña': 'id_gestion_campaña',          
                'Tipo documento': 'tipo_documento',
                'Número documento': 'documento',
                'Nombre': 'nombre_usuario',
                'Fecha gestión': 'fecha_gestion',
                'Tipo llamada': 'tipo_llamada',
                'Código gestión': 'id_gestion',
                'Resultado': 'resultado',
                'Fecha Compromiso': 'fecha_compromiso',
                'Funcionario': 'asesor',
                'Campaña': 'campana',
                'Teléfono': 'telefono',
                'Obligación': 'obligacion',
                'Nro. Comparendo': 'numero_comparendo',
                'Valor': 'valor',
                'identificador_infraccion': 'identificador_infraccion',
                'archivo_origen': 'archivo_origen',
                'fecha_carga': 'fecha_carga',
                'fecha_gestion_sencilla': 'fecha_gestion_sencilla',
                'Tipo_Chat': 'tipo_chat'
            },
            'id_column': 'id_registro',
            'clean_function': preparar_datos
        }
        super().__init__(engine, config)
        
    def _procesar_archivo(self, archivo):
        try:
            import tempfile
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
                tmp.write(archivo.getvalue())
                tmp_path = tmp.name
            df_procesado, df_errores, mensaje = preparar_datos(tmp_path, archivo.name)
            # Validación adicional
            campos_obligatorios = ['tipo_documento', 'nombre_usuario', 'documento', 'fecha_gestion']
            mask_errores = df_procesado[campos_obligatorios].isnull().any(axis=1)
            # Mover registros con errores
            nuevos_errores = df_procesado[mask_errores].copy()
            df_procesado = df_procesado[~mask_errores]
            # Combinar errores nuevos con los originales
            if not nuevos_errores.empty:
                nuevos_errores['error'] = 'Campos obligatorios faltantes'
                df_errores = pd.concat([df_errores, nuevos_errores])
            return df_procesado, df_errores, mensaje
        except Exception as e:
            return pd.DataFrame(), pd.DataFrame({'error': [str(e)]}), str(e)

class SMSProcessor(DataProcessor):
    def actualizar_resultados(self):
        """Actualiza la columna resultado en la base de datos si hay cambios y muestra estadísticas."""
        if self.df_procesado is None or self.df_procesado.empty:
            st.info("No hay datos procesados para actualizar resultados.")
            return
        engine = self.engine
        df = self.df_procesado
        id_col = self.config['id_column']
        total_existentes = 0
        total_actualizados = 0
        total_ya_actualizados = 0
        ids_actualizados = []
        ids_ya_actualizados = []
        with engine.connect() as conn:
            for _, row in df.iterrows():
                id_registro = row[id_col]
                nuevo_resultado = row['resultado']
                res = conn.execute(text(f"SELECT resultado FROM sms WHERE {id_col} = :id_registro"), {'id_registro': id_registro}).fetchone()
                if res is not None:
                    total_existentes += 1
                    resultado_actual = res[0]
                    if resultado_actual != nuevo_resultado:
                        conn.execute(text(f"UPDATE sms SET resultado = :nuevo_resultado WHERE {id_col} = :id_registro"), {'nuevo_resultado': nuevo_resultado, 'id_registro': id_registro})
                        total_actualizados += 1
                        ids_actualizados.append(id_registro)
                    else:
                        total_ya_actualizados += 1
                        ids_ya_actualizados.append(id_registro)
        
        if total_actualizados == 0:
            st.warning("No se encontraron cambios en la columna 'resultado'.")
        else:
            st.balloons()

        cols = st.columns(3)
        cols[0].metric("✅ Registros", total_existentes)
        cols[1].metric("🆕 Actualizados", total_actualizados)
        cols[2].metric("📋 Ya actualizados", total_ya_actualizados)

    """Procesador específico para SMS"""
    
    def __init__(self, engine):
        config = {
            'table_name': 'sms',
            'mapeo_columnas': {
                'id_registro': 'id_registro',
                'TIPO DOCUMENTO': 'tipo_documento',
                'DOCUMENTO': 'documento',
                'NOMBRE': 'nombre_usuario',
                'FECHA': 'fecha_sms',
                'RESULTADO': 'resultado',
                'SMS': 'texto_sms',
                'BASE': 'base',
                'TELEFONO': 'telefono',
                'NRO_COMPARENDO': 'numero_comparendo',
                'identificador_infraccion': 'identificador_infraccion',
                'archivo_origen': 'archivo_origen',
                'fecha_carga': 'fecha_carga'
            },
            'id_column': 'id_registro',
            'clean_function': preparar_datos_sms
        }
        super().__init__(engine, config)
        
    def _procesar_archivo(self, archivo):
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(archivo.getvalue())
            tmp_path = tmp.name
        return preparar_datos_sms(tmp_path, archivo.name)
    
class PagosProcessor(DataProcessor):
    """Procesador específico para pagos con procesamiento por lotes"""
    
    def __init__(self, engine):
        config = {
            'table_name': 'pagos',
            'mapeo_columnas': {
                'id_registro': 'id_registro',
                'codcliente': 'codcliente',
                'nitcliente': 'nitcliente',
                'numobligacion': 'numobligacion',
                'fechapago': 'fechapago',
                'valorpago': 'valorpago',
                'base': 'BASE',
                'fecha_gest': 'FECHA GEST',
                'aplica_pago_gestion': 'APLICA PAGO GESTIÓN',
                'fecha_sms': 'FECHA SMS',
                'campania': 'CAMPAÑA',
                'aplica_pago_sms': 'APLICA PAGO SMS',
                'aplicacion_final': 'APLICACIÓN FINAL',
                'fecha_sencilla': 'FECHA SENCILLA',
                'anio': 'AÑO',
                'dia': 'DIA',
                'mes': 'MES',
                'semana': 'SEMANA',
                'numero_mes': 'NÚMERO MES',
                'nombre_dia_semana': 'NOMBRE DÍA SEMANA',
                'cruce_sms': 'CRUCE SMS',
                'estrategia': 'ESTRATEGIA',
                'infraccion': 'INFRACCION',
                'periodo_21_20': 'Período 21 al 20',
                'archivo_origen': 'archivo_origen',
                'fecha_carga': 'fecha_carga'
            },
            'id_column': 'id_registro',
            'clean_function': procesar_pagos
        }
        super().__init__(engine, config)
        self.archivos_procesados = []
        self.resultados_procesamiento = []
        self.df_resumen = pd.DataFrame()
        
    def _procesar_archivo(self, archivo):
        """Procesamiento de archivo Excel de pagos con la nueva lógica"""
        try:
            # Validación: solo aceptar archivos Excel
            if hasattr(archivo, 'name') and not archivo.name.lower().endswith(('.xlsx', '.xls')):
                return pd.DataFrame(), pd.DataFrame({'error': ['El archivo debe ser .xlsx o .xls']}), "Formato inválido"
            temp_dir = os.path.join(tempfile.gettempdir(), "simm_pagos")
            os.makedirs(temp_dir, exist_ok=True)
            if hasattr(archivo, 'name'):
                temp_path = os.path.join(temp_dir, archivo.name)
                with open(temp_path, 'wb') as f:
                    f.write(archivo.getvalue())
                nombre_archivo = archivo.name
            else:
                temp_path = archivo
                nombre_archivo = os.path.basename(archivo)
            # Procesar el archivo usando la nueva función
            df_procesado, df_errores, mensaje = procesar_pagos(temp_path, nombre_archivo)
            if hasattr(archivo, 'name'):
                try:
                    os.unlink(temp_path)
                except:
                    pass
            if df_procesado.empty:
                return pd.DataFrame(), df_errores, mensaje
            # Registrar archivo procesado
            self.archivos_procesados.append(nombre_archivo)
            return df_procesado, df_errores, mensaje
        except Exception as e:
            error_msg = f"Error procesando {getattr(archivo, 'name', archivo)}: {str(e)}"
            return pd.DataFrame(), pd.DataFrame({'error': [error_msg]}), error_msg
        
    def procesar_archivos_multiples(self, archivos):
        """Procesa múltiples archivos y combina sus resultados"""
        total_archivos = len(archivos)
        
        with st.status(f"🔄 Procesando {total_archivos} archivos...", expanded=True) as status:
            self.df_procesado = pd.DataFrame()
            self.df_errores = pd.DataFrame()
            
            # Crear dataframe para almacenar resumen
            self.df_resumen = pd.DataFrame(columns=[
                'nombre_archivo', 'registros_totales', 'registros_validos',
                'registros_nuevos', 'registros_duplicados', 'registros_con_errores'
            ])
            
            # Procesar cada archivo y acumular resultados
            for idx, archivo in enumerate(archivos):
                status.update(label=f"🔄 Procesando archivo {idx+1}/{total_archivos}: {archivo.name}")
                
                # Procesar archivo individual
                df_actual, df_errores_actual, mensaje = self._procesar_archivo(archivo)
                
                if not df_actual.empty:
                    # Guardar resultados de este archivo
                    df_actual['archivo_origen'] = archivo.name
                    
                    # Verificar duplicados para este archivo
                    nuevos_actual = verificar_duplicados(
                        self.engine,
                        df_actual,
                        self.config['table_name'],
                        self.config['id_column']
                    )
                    duplicados_actual = df_actual.shape[0] - len(nuevos_actual)
                    
                    # Acumular resultados
                    self.df_procesado = pd.concat([self.df_procesado, df_actual])
                    
                    # Guardar resumen
                    self.df_resumen.loc[len(self.df_resumen)] = {
                        'nombre_archivo': archivo.name,
                        'registros_totales': len(df_actual) + len(df_errores_actual),
                        'registros_validos': len(df_actual),
                        'registros_nuevos': len(nuevos_actual),
                        'registros_duplicados': duplicados_actual,
                        'registros_con_errores': len(df_errores_actual)
                    }
                
                if not df_errores_actual.empty:
                    df_errores_actual['archivo'] = archivo.name
                    self.df_errores = pd.concat([self.df_errores, df_errores_actual])
            
            # Buscar duplicados en el conjunto final consolidado
            if not self.df_procesado.empty:
                self.nuevos = verificar_duplicados(
                    self.engine,
                    self.df_procesado,
                    self.config['table_name'],
                    self.config['id_column']
                )
                self.duplicados = self.df_procesado.shape[0] - len(self.nuevos)
                
                status.update(label=f"✅ Procesamiento completado: {len(self.nuevos)} registros nuevos", state="complete")
                return True
            else:
                status.update(label="⚠️ No se encontraron registros válidos", state="error")
                return False
                
    def mostrar_resumen_procesamiento(self):
        """Muestra una tabla resumen del procesamiento de todos los archivos"""
        if not self.df_resumen.empty:
            st.subheader("📊 Resumen de procesamiento por archivo")
            
            # Formateamos para mejor visualización
            df_display = self.df_resumen.copy()
            df_display = df_display.style.format({
                'registros_totales': '{:,.0f}',
                'registros_validos': '{:,.0f}',
                'registros_nuevos': '{:,.0f}',
                'registros_duplicados': '{:,.0f}',
                'registros_con_errores': '{:,.0f}'
            })
            
            st.dataframe(df_display, use_container_width=True)
            
            # Totales generales
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("📄 Total Archivos", len(self.df_resumen))
            col2.metric("✅ Total Registros Válidos", self.df_resumen['registros_validos'].sum())
            col3.metric("🆕 Total Registros Nuevos", self.df_resumen['registros_nuevos'].sum())
            col4.metric("📋 Total Registros Duplicados", self.df_resumen['registros_duplicados'].sum())
            col5.metric("⚠️ Total Errores", self.df_resumen['registros_con_errores'].sum())
    
    def _cargar_todos_datos(self):
        """Carga todos los datos nuevos en la base de datos de una sola vez"""
        if not hasattr(self, 'nuevos') or self.nuevos.empty:
            st.warning("No hay datos nuevos para cargar")
            return False
            
        try:
            total_registros = len(self.nuevos)
            chunk_size = 5000
            chunks = [self.nuevos[i:i + chunk_size] 
                    for i in range(0, total_registros, chunk_size)]
            
            status_text = st.empty()
            progress_bar = st.progress(0)
            registros_insertados = 0
            
            with self.engine.begin() as conn:
                for i, chunk in enumerate(chunks):
                    # Actualizar progreso
                    progress = (i + 1) / len(chunks)
                    status_text.markdown(f"""
                        **Progreso de carga:**  
                        • Lotes procesados: `{i+1}/{len(chunks)}`  
                        • Registros insertados: `{registros_insertados + len(chunk)}/{total_registros}`
                    """)
                    
                    # Insertar chunk
                    chunk.to_sql(
                        name=self.config['table_name'],
                        con=conn,
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=500
                    )
                    
                    # Actualizar contadores
                    registros_insertados += len(chunk)
                    progress_bar.progress(progress)

            # Limpiar elementos de progreso
            progress_bar.empty()
            status_text.empty()
            
            st.cache_data.clear()

            # Mostrar resumen final
            st.success(f"**Carga exitosa:** {registros_insertados} registros nuevos insertados")
            return True
            
        except Exception as e:
            st.error(f"❌ Error en la carga: {str(e)}")
            raise

class BasesProcessor(DataProcessor):
    """Procesador específico para Bases"""

    def __init__(self, engine):
        config = {
            'table_name': 'bases',
            'mapeo_columnas': {
                'id_registro': 'id_registro',
                'BASE': 'base',
                'FECHA DE ENTREGA': 'fecha_entrega',
                'TIPO DE DCTO': 'tipo_documento',
                'IDENTIFICACIÓN': 'documento',
                'NOMBRE': 'nombre',
                'NRO. COMPARENDO': 'numero_comparendo',
                'FECHA DE COMPARENDO': 'fecha_comparendo',
                'CODIGO DE INFRACCIÓN': 'codigo_infraccion',
                'PLACA': 'placa',
                'valor': 'valor_infraccion',
                'CEL1': 'telefono',
                'archivo_origen': 'archivo_origen',
                'fecha_carga': 'fecha_carga',
                'estado_telefono': 'estado_telefono',
                'base_cleaned': 'base_cleaned'
            },
            'id_column': 'id_registro',
            'clean_function': preparar_datos_bases
        }
        super().__init__(engine, config)

    def _procesar_archivo(self, archivo):
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(archivo.getvalue())
            tmp_path = tmp.name
        df_procesado, df_errores, mensaje = preparar_datos_bases(tmp_path, archivo.name)
        # Aplicar mapeo de columnas a df_procesado
        mapeo = self.config["mapeo_columnas"]
        df_procesado = df_procesado.rename(columns=mapeo)
        # Aplicar mapeo a df_errores si contiene columnas relevantes
        if not df_errores.empty:
            df_errores = df_errores.rename(columns={k: v for k, v in mapeo.items() if k in df_errores.columns})
        return df_procesado, df_errores, mensaje

# ==============================================================================
# FUNCIONES UTILITARIAS
# ==============================================================================
@st.cache_resource
def get_db_connection():
    """Obtiene y cachea la conexión a la base de datos"""
    try:
        engine = get_engine()
        # Prueba simple de conexión
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        st.error(f"❌ Error de conexión a la base de datos: {str(e)}")
        st.stop()

def obtener_ultima_insercion(engine, tabla, columna_fecha="fecha_carga"):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT MAX({columna_fecha}) FROM {tabla}"))
            fecha = result.scalar()
        return fecha
    except Exception as e:
        return None

def verificar_duplicados(engine, df, table_name, id_column):
    """Verifica registros duplicados en la base de datos"""
    try:
        chunk_size = 1000
        ids = df[id_column].unique().tolist()
        chunks = [ids[i:i + chunk_size] for i in range(0, len(ids), chunk_size)]
        
        existing_ids = set()
        progress_bar = st.progress(0)
        
        with engine.connect() as conn:
            for i, chunk in enumerate(chunks):
                progress = (i + 1) / len(chunks)
                progress_bar.progress(progress)
                
                result = conn.execute(
                    text(f"SELECT {id_column} FROM {table_name} WHERE {id_column} = ANY(:ids)"),
                    {'ids': chunk}
                )
                existing_ids.update([row[0] for row in result])
        
        return df[~df[id_column].isin(existing_ids)]
    
    except Exception as e:
        st.error(f"Error en verificación de duplicados: {str(e)}")
        st.stop()

# ==============================================================================
# INTERFAZ DE USUARIO - CORRECCIONES PRINCIPALES
# ==============================================================================
class StreamlitUI:
    def __init__(self):
        self.engine = get_db_connection()
        self._initialize_session_state()
            
    def _initialize_session_state(self):
        """Inicializa el estado de la sesión"""
        if 'modulo_actual' not in st.session_state:
            st.session_state.modulo_actual = "Carga de Gestiones"
        if 'uploaded_files' not in st.session_state:
            st.session_state.uploaded_files = None
        if 'procesado' not in st.session_state:
            st.session_state.procesado = False
        if 'processor' not in st.session_state:
            st.session_state.processor = None
            
    def _reset_upload_state(self):
        """Resetea el estado relacionado con la carga de archivos"""
        st.session_state.uploaded_files = None
        st.session_state.procesado = False
        st.session_state.processor = None
        
    def _mostrar_topbar(self):
        """Muestra la barra de navegación superior con option menu"""
        MODULOS = {
            "🧮 Carga de Gestiones": "🧮",
            "📲 Carga de SMS": "📲",
            "💰 Carga de Pagos": "💰",
            "📋 Carga de Bases": "📋"
        }
        modulo_seleccionado = option_menu(
            menu_title=None,
            options=list(MODULOS.keys()),
            icons=[MODULOS[k] for k in MODULOS.keys()],
            orientation="horizontal"
        )
        if modulo_seleccionado != st.session_state.modulo_actual:
            self._reset_upload_state()
        st.session_state.modulo_actual = modulo_seleccionado
    
    def _mostrar_carga_archivo(self):
        """Componente de carga de archivo mejorado"""
        MODULO_CONFIG = {
            "🧮 Carga de Gestiones": {"ext": ".xlsx", "multiple": False},
            "📲 Carga de SMS": {"ext": ".xlsx", "multiple": False},
            "💰 Carga de Pagos": {"ext": ".xlsx", "multiple": False},
            "📋 Carga de Bases": {"ext": ".xlsx", "multiple": False}
        }
        
        config = MODULO_CONFIG.get(st.session_state.modulo_actual, {})
        st.title(f"{st.session_state.modulo_actual}")

        uploaded_files = st.file_uploader(
            f"Subir archivo{'s' if config['multiple'] else ''}",
            type=[config["ext"]],  # Lista con la extensión
            accept_multiple_files=config["multiple"],
            key=f"uploader_{st.session_state.modulo_actual}"
        )
        
        if uploaded_files:
            st.session_state.uploaded_files = uploaded_files
            return True
        return False
    
    def ejecutar(self):
        """Ejecuta la aplicación principal con correcciones"""
        self._mostrar_topbar()
        col1, col2, col3 = st.columns([1,6,1])
        with col2:  
            tabla_map = {
                "🧮 Carga de Gestiones": "gestiones",
                "📲 Carga de SMS": "sms",
                "💰 Carga de Pagos": "pagos",
                "📋 Carga de Bases": "bases"
            }
            tabla = tabla_map.get(st.session_state.modulo_actual)
            if tabla:
                fecha = obtener_ultima_insercion(self.engine, tabla)
                if fecha:
                    st.info(f"Última Actualización en {tabla}: {fecha.strftime('%d/%m/%Y %H:%M') if hasattr(fecha, 'strftime') else fecha}")
                else:
                    st.info(f"No hay registros en {tabla}.")

        col1, col2, col3 = st.columns([1,2,1])
        with col2:    
            if self._mostrar_carga_archivo():
                files = st.session_state.uploaded_files
                if not isinstance(files, list):
                    files = [files]
                st.success(f"✅ Archivo{'s' if len(files)>1 else ''} cargado{'s' if len(files)>1 else ''} correctamente")

                st.subheader("⚙ Procesar Archivo")
                if st.button("✅ Confirmar Procesar Archivo", type="primary"):
                    MODULO_PROCESSORS = {
                        "🧮 Carga de Gestiones": GestionesProcessor,
                        "📲 Carga de SMS": SMSProcessor,
                        "💰 Carga de Pagos": PagosProcessor,
                        "📋 Carga de Bases": BasesProcessor
                    }
                    processor_class = MODULO_PROCESSORS.get(st.session_state.modulo_actual)
                    if processor_class is None:
                        st.error(f"No se encontró procesador para el módulo: {st.session_state.modulo_actual}")
                        return
                    processor = processor_class(self.engine)
                    try:
                        if st.session_state.modulo_actual == "💰 Carga de Pagos" and len(files) > 1:
                            success = processor.procesar_archivos_multiples(files)
                        else:
                            success = processor.procesar_archivo(files[0])
                        if success:
                            st.session_state.procesado = True
                            st.session_state.processor = processor
                            st.rerun()
                    except Exception as e:
                        st.error(f"Error al procesar: {str(e)}")
            if st.session_state.procesado and st.session_state.processor:
                processor = st.session_state.processor
                self._mostrar_resultados(processor)
    
    def _mostrar_resultados(self, processor):
        """Muestra los resultados del procesamiento"""
        
        st.subheader("📊 Resultados del Procesamiento")
        
        if hasattr(processor, 'df_procesado') and not processor.df_procesado.empty:
            cols = st.columns(4)
            cols[0].metric("✅ Válidos", len(processor.df_procesado))
            cols[1].metric("🆕 Nuevos", len(getattr(processor, 'nuevos', pd.DataFrame())))
            cols[2].metric("📋 Duplicados", getattr(processor, 'duplicados', 0))
            cols[3].metric("❌ Errores", len(getattr(processor, 'df_errores', pd.DataFrame())))
            # Imprimir estadísticas en el log
            logger.info(
                f"Estadísticas procesamiento: "
                f"Válidos={len(processor.df_procesado)}, "
                f"Nuevos={len(getattr(processor, 'nuevos', pd.DataFrame()))}, "
                f"Duplicados={getattr(processor, 'duplicados', 0)}, "
                f"Errores={len(getattr(processor, 'df_errores', pd.DataFrame()))}"
            )
        
        if hasattr(processor, 'nuevos') and len(processor.nuevos) > 0:
            st.divider()
            st.subheader("🚀 Carga de Datos")
            st.info(f"Se cargarán {len(processor.nuevos)} registros nuevos")
            if st.button("✅ Confirmar e Iniciar Carga", type="primary"):
                try:
                    if st.session_state.modulo_actual == "Carga de Pagos":
                        processor._cargar_todos_datos()
                    else:
                        processor._cargar_datos()
                    st.success("✅ Datos cargados exitosamente")
                    st.balloons()
                    self._reset_upload_state()
                except Exception as e:
                    logger.error(f"Error en carga: {str(e)}")
                    st.error(f"Error en carga: {str(e)}")

        # Botón para actualizar resultados de SMS si corresponde
        if st.session_state.modulo_actual == "📲 Carga de SMS" and hasattr(processor, 'df_procesado') and not processor.df_procesado.empty:
            if st.button("🔄 Actualizar columna resultado"):
                processor.actualizar_resultados()

# ==============================================================================
# EJECUCIÓN PRINCIPAL
# ==============================================================================
if __name__ == "__main__":
    app = StreamlitUI()
    app.ejecutar()


