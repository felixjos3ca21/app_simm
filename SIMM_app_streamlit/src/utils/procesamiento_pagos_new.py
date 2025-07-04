import pandas as pd
import os
from pathlib import Path
import chardet
import hashlib
from datetime import datetime
import numpy as np
import re
from typing import Set, List, Optional, Tuple
import json
from sqlalchemy import create_engine, text

# --- CONFIGURACIÓN ---
CONFIG = {
    'CHUNK_SIZE': 10000,  # Para procesar archivos grandes en chunks
    'MAX_ENCODING_BYTES': 100000
}

# --- CLASE PRINCIPAL ---
class ProcesadorPagos:
    def __init__(self, carpeta_base: str, db_config: dict = None):
        self.carpeta_base = carpeta_base
        self.config = CONFIG
        self.db_config = db_config or DB_CONFIG
        self.engine = create_engine(
            f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
        )
        self.archivos_procesados = self._cargar_archivos_procesados()
        self.stats = {
            'archivos_encontrados': 0,
            'archivos_nuevos': 0,
            'archivos_procesados_exitosos': 0,
            'archivos_con_errores': 0,
            'registros_ap': 0,
            'registros_comp': 0
        }
        
    def _cargar_archivos_procesados(self) -> Set[str]:
        """Carga la lista de archivos ya procesados desde la base de datos"""
        try:
            query = "SELECT ruta_archivo FROM archivos_procesados_pagos"
            df = pd.read_sql_query(query, self.engine)
            return set(df['ruta_archivo'].tolist())
        except Exception as e:
            print(f"Warning: Error cargando archivos procesados: {e}")
            return set()
    
    def _guardar_archivo_procesado(self, ruta_archivo: str, nombre_archivo: str, 
                                 tipo_archivo: str, registros_procesados: int, estado: str):
        """Registra un archivo como procesado en la base de datos"""
        try:
            hash_archivo = self._calcular_hash_archivo(ruta_archivo)
            
            query = """
            INSERT INTO archivos_procesados_pagos 
            (nombre_archivo, ruta_archivo, tipo_archivo, registros_procesados, estado, hash_archivo)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ruta_archivo) DO UPDATE SET
            fecha_carga = CURRENT_TIMESTAMP,
            registros_procesados = EXCLUDED.registros_procesados,
            estado = EXCLUDED.estado,
            hash_archivo = EXCLUDED.hash_archivo
            """
            
            with self.engine.connect() as conn:
                conn.execute(text(query), (
                    nombre_archivo, ruta_archivo, tipo_archivo, 
                    registros_procesados, estado, hash_archivo
                ))
                conn.commit()
                
        except Exception as e:
            print(f"Error guardando archivo procesado: {e}")
    
    def _calcular_hash_archivo(self, ruta_archivo: str) -> str:
        """Calcula hash del archivo para detectar cambios"""
        try:
            with open(ruta_archivo, 'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
        except Exception:
            return ""
    
    def _validar_configuracion(self) -> bool:
        """Valida que la configuración sea correcta"""
        carpeta_base = Path(self.carpeta_base)
        if not carpeta_base.exists():
            print(f"ERROR: La carpeta base no existe: {carpeta_base}")
            return False
        
        if not carpeta_base.is_dir():
            print(f"ERROR: La ruta no es una carpeta: {carpeta_base}")
            return False
        
        return True
    
    def detectar_encoding(self, archivo: Path) -> str:
        """Detecta la codificación de un archivo"""
        try:
            with open(archivo, 'rb') as f:
                rawdata = f.read(self.config['MAX_ENCODING_BYTES'])
            resultado = chardet.detect(rawdata)
            encoding = resultado['encoding']
            confidence = resultado['confidence']
            
            if confidence < 0.7:
                print(f"Warning: Baja confianza en encoding para {archivo.name}: {encoding} ({confidence:.2f})")
            
            return encoding or 'utf-8'
        except Exception as e:
            print(f"Warning: Error detectando encoding para {archivo.name}: {e}")
            return 'utf-8'
    
    def parse_fecha(self, fecha_str) -> Optional[pd.Timestamp]:
        """Parsea fechas con múltiples formatos"""
        if pd.isna(fecha_str) or fecha_str == '' or fecha_str is None:
            return None
        
        formatos = [
            '%d/%m/%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y', 
            '%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y %H:%M:%S', '%Y/%m/%d %H:%M:%S',
            '%d/%m/%y', '%y-%m-%d', '%m/%d/%y'
        ]
        
        for fmt in formatos:
            try:
                return pd.to_datetime(fecha_str, format=fmt, errors='raise')
            except (ValueError, TypeError):
                continue
        
        try:
            return pd.to_datetime(fecha_str, errors='raise')
        except:
            print(f"Warning: No se pudo parsear la fecha: {fecha_str}")
            return None
    
    def limpiar_nombres(self, df: pd.DataFrame, campos: List[str] = None) -> pd.DataFrame:
        """Limpia campos de nombres"""
        if campos is None:
            campos = ['nombres', 'apellidos']
        
        df_copia = df.copy()
        for col in campos:
            if col in df_copia.columns:
                df_copia[col] = (
                    df_copia[col].astype(str)
                    .str.replace(r'[\t\n\r"]+', ' ', regex=True)
                    .str.replace(r'\s+', ' ', regex=True)
                    .str.strip()
                    .replace('nan', '')
                )
        return df_copia
    
    def procesar_archivo_ap(self, archivo: Path) -> pd.DataFrame:
        """Procesa archivos de Acuerdos de Pago"""
        try:
            encoding = self.detectar_encoding(archivo)
            
            try:
                df = pd.read_csv(archivo, sep='\t', encoding=encoding, dtype=str)
            except UnicodeDecodeError:
                print(f"Warning: Fallback a latin-1 para {archivo.name}")
                df = pd.read_csv(archivo, sep='\t', encoding='latin-1', dtype=str)
            
            if df.empty:
                print(f"Warning: Archivo vacío: {archivo.name}")
                return pd.DataFrame()
            
            # Validar columnas requeridas
            columnas_requeridas = ['nro_acuerdo', 'id_usuario', 'valor', 'fecha_liquida']
            columnas_disponibles = [col for col in columnas_requeridas if col in df.columns]
            
            if len(columnas_disponibles) < 3:
                print(f"Warning: Columnas insuficientes en {archivo.name}: {df.columns.tolist()}")
                return pd.DataFrame()
            
            # Seleccionar columnas
            columnas_finales = ['nro_acuerdo', 'id_usuario', 'nombres', 'apellidos', 
                              'valor', 'fecha_liquida', 'consecutivo_cuota']
            df = df[[col for col in columnas_finales if col in df.columns]]
            
            # Procesar fechas
            if 'fecha_liquida' in df.columns:
                df['fecha_liquida'] = df['fecha_liquida'].apply(self.parse_fecha)
            
            # Limpiar nombres
            df = self.limpiar_nombres(df)
            
            # Crear nombre completo
            if 'nombres' in df.columns and 'apellidos' in df.columns:
                df['nombre_usuario'] = (
                    df['nombres'].fillna('').astype(str) + ' ' + 
                    df['apellidos'].fillna('').astype(str)
                ).str.strip()
            else:
                df['nombre_usuario'] = ''
            
            # Eliminar columnas originales de nombres
            df.drop(columns=[c for c in ['nombres', 'apellidos'] if c in df.columns], inplace=True)
            
            # Procesar campos
            df['nro_acuerdo'] = df['nro_acuerdo'].fillna('').astype(str)
            df['documento'] = df['id_usuario'].fillna('').astype(str) if 'id_usuario' in df.columns else ''
            df['valor'] = pd.to_numeric(df['valor'], errors='coerce').fillna(0)
            
            # Metadata
            df['archivo_origen'] = archivo.name
            df['fecha_carga'] = datetime.now()
            df['tipo_pago'] = 'AP'
            
            # ID único
            df['id_registro'] = df.apply(
                lambda x: hashlib.sha256(
                    f"AP_{x.get('nro_acuerdo','')}_{x.get('documento','')}_{x.get('consecutivo_cuota','')}_{x.get('fecha_liquida','')}".encode('utf-8')
                ).hexdigest(), axis=1
            )
            
            # Filtrar registros válidos
            df = df[
                (df['valor'] > 0) & 
                (df['fecha_liquida'].notna()) &
                (df['nro_acuerdo'] != '') &
                (df['documento'] != '')
            ]
            
            print(f"AP - {archivo.name}: {len(df)} registros válidos")
            return df
            
        except Exception as e:
            print(f"Error procesando AP {archivo.name}: {e}")
            return pd.DataFrame()
    
    def procesar_archivo_comparendos(self, archivo: Path) -> pd.DataFrame:
        """Procesa archivos de Comparendos"""
        try:
            encoding = self.detectar_encoding(archivo)
            
            try:
                df = pd.read_csv(archivo, sep='\t', encoding=encoding, dtype=str)
            except UnicodeDecodeError:
                print(f"Warning: Fallback a latin-1 para {archivo.name}")
                df = pd.read_csv(archivo, sep='\t', encoding='latin-1', dtype=str)
            
            if df.empty:
                print(f"Warning: Archivo vacío: {archivo.name}")
                return pd.DataFrame()
            
            # Seleccionar columnas
            columnas_finales = ['nro_comparendo', 'nro_recibo', 'fecha_liquida_contrav',
                              'compute_0004', 'id_usuario', 'nombres', 'apellidos',
                              'nro_resolucion', 'intereses']
            df = df[[col for col in columnas_finales if col in df.columns]]
            
            # Renombrar columna de fecha
            if 'fecha_liquida_contrav' in df.columns:
                df.rename(columns={'fecha_liquida_contrav': 'fecha_liquida'}, inplace=True)
            
            # Procesar fechas
            if 'fecha_liquida' in df.columns:
                df['fecha_liquida'] = df['fecha_liquida'].apply(self.parse_fecha)
            
            # Manejar número de comparendo
            if 'nro_comparendo' in df.columns and 'nro_resolucion' in df.columns:
                df['nro_comparendo'] = np.where(
                    df['nro_comparendo'].isna() | (df['nro_comparendo'] == ''),
                    df['nro_resolucion'], df['nro_comparendo']
                )
            
            # Limpiar nombres
            df = self.limpiar_nombres(df)
            
            if 'nombres' in df.columns and 'apellidos' in df.columns:
                df['nombre_usuario'] = (
                    df['nombres'].fillna('').astype(str) + ' ' + 
                    df['apellidos'].fillna('').astype(str)
                ).str.strip()
            else:
                df['nombre_usuario'] = ''
            
            df.drop(columns=[c for c in ['nombres', 'apellidos'] if c in df.columns], inplace=True)
            
            # Calcular valor total
            df['compute_0004'] = pd.to_numeric(df.get('compute_0004', 0), errors='coerce').fillna(0)
            df['intereses'] = pd.to_numeric(df.get('intereses', 0), errors='coerce').fillna(0)
            df['valor'] = df['compute_0004'] + df['intereses']
            
            # Metadata
            df['documento'] = df['id_usuario'].fillna('').astype(str) if 'id_usuario' in df.columns else ''
            df['archivo_origen'] = archivo.name
            df['fecha_carga'] = datetime.now()
            df['tipo_pago'] = 'COMP'
            
            # ID único
            df['id_registro'] = df.apply(
                lambda x: hashlib.sha256(
                    f"COMP_{x.get('nro_comparendo','')}_{x.get('documento','')}_{x.get('valor','')}_{x.get('fecha_liquida','')}".encode('utf-8')
                ).hexdigest(), axis=1
            )
            
            # Filtrar registros válidos
            df = df[
                (df['valor'] > 0) & 
                (df['fecha_liquida'].notna()) &
                (df['documento'] != '')
            ]
            
            print(f"COMP - {archivo.name}: {len(df)} registros válidos")
            return df
            
        except Exception as e:
            print(f"Error procesando COMP {archivo.name}: {e}")
            return pd.DataFrame()
    
    def obtener_archivos_nuevos(self) -> List[Path]:
        """Obtiene lista de archivos .txt que no han sido procesados"""
        carpeta_base = Path(self.carpeta_base)
        todos_txt = [
            Path(root) / file
            for root, _, files in os.walk(carpeta_base)
            for file in files if file.lower().endswith('.txt')
        ]
        
        self.stats['archivos_encontrados'] = len(todos_txt)
        
        # Filtrar archivos ya procesados
        archivos_nuevos = [
            archivo for archivo in todos_txt 
            if str(archivo.resolve()) not in self.archivos_procesados
        ]
        
        self.stats['archivos_nuevos'] = len(archivos_nuevos)
        print(f"Archivos encontrados: {len(todos_txt)}, Nuevos: {len(archivos_nuevos)}")
        
        return archivos_nuevos
    
    def procesar_tipo_archivo(self, tipo: str, patron_regex: re.Pattern, 
                            funcion_procesar, nombre_salida: str, archivos: List[Path]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Procesa archivos de un tipo específico y retorna DataFrame consolidado y filtrado"""
        archivos_tipo = [
            archivo for archivo in archivos 
            if patron_regex.search(archivo.name)
        ]
        
        print(f"[{tipo}] Archivos encontrados: {len(archivos_tipo)}")
        
        if not archivos_tipo:
            print(f"No se encontraron archivos para [{tipo}]")
            return pd.DataFrame(), pd.DataFrame()
        
        dfs_validos = []
        archivos_procesados_ok = 0
        archivos_con_error = 0
        
        for archivo in archivos_tipo:
            try:
                print(f"Procesando [{tipo}]: {archivo.name}")
                df_temp = funcion_procesar(archivo)
                
                if not df_temp.empty:
                    dfs_validos.append(df_temp)
                    self._guardar_archivo_procesado(
                        str(archivo.resolve()), archivo.name, tipo, len(df_temp), 'EXITOSO'
                    )
                    archivos_procesados_ok += 1
                else:
                    self._guardar_archivo_procesado(
                        str(archivo.resolve()), archivo.name, tipo, 0, 'SIN_DATOS'
                    )
                    
            except Exception as e:
                print(f"Error procesando {archivo.name}: {e}")
                self._guardar_archivo_procesado(
                    str(archivo.resolve()), archivo.name, tipo, 0, 'ERROR'
                )
                archivos_con_error += 1
        
        if dfs_validos:
            # DataFrame consolidado (con duplicados)
            df_consolidado = pd.concat(dfs_validos, ignore_index=True)
            
            # DataFrame filtrado (sin duplicados)
            df_filtrado = df_consolidado.drop_duplicates(subset='id_registro')
            
            # Guardar CSV consolidado
            archivo_consolidado = f'{nombre_salida}_consolidado.csv'
            df_consolidado.to_csv(archivo_consolidado, index=False)
            
            # Actualizar estadísticas
            if tipo == 'AP':
                self.stats['registros_ap'] = len(df_filtrado)
            else:
                self.stats['registros_comp'] = len(df_filtrado)
            
            print(f"[{tipo}] Resultados:")
            print(f"  • Registros totales: {len(df_consolidado)}")
            print(f"  • Registros únicos: {len(df_filtrado)}")
            print(f"  • Valor promedio: {df_filtrado['valor'].mean():.2f}")
            print(f"  • Archivo consolidado: {archivo_consolidado}")
            
            self.stats['archivos_procesados_exitosos'] += archivos_procesados_ok
            self.stats['archivos_con_errores'] += archivos_con_error
            
            return df_consolidado, df_filtrado
        
        return pd.DataFrame(), pd.DataFrame()
    
    def generar_resumen_final(self) -> dict:
        """Genera resumen final del procesamiento"""
        print("="*60)
        print("RESUMEN FINAL DEL PROCESAMIENTO")
        print("="*60)
        print(f"📁 Archivos encontrados: {self.stats['archivos_encontrados']}")
        print(f"🆕 Archivos nuevos: {self.stats['archivos_nuevos']}")
        print(f"✅ Archivos procesados exitosamente: {self.stats['archivos_procesados_exitosos']}")
        print(f"❌ Archivos con errores: {self.stats['archivos_con_errores']}")
        print(f"💰 Registros AP procesados: {self.stats['registros_ap']}")
        print(f"🚗 Registros Comparendos procesados: {self.stats['registros_comp']}")
        print(f"📊 Total registros: {self.stats['registros_ap'] + self.stats['registros_comp']}")
        
        # Guardar resumen en JSON
        resumen = {
            'fecha_procesamiento': datetime.now().isoformat(),
            'estadisticas': self.stats
        }
        
        with open(f"resumen_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", 'w', encoding='utf-8') as f:
            json.dump(resumen, f, indent=2, ensure_ascii=False)
        
        return resumen
    
    def ejecutar(self) -> Tuple[bool, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
        """Método principal de ejecución"""
        print("🚀 Iniciando procesamiento de pagos")
        
        # Validar configuración
        if not self._validar_configuracion():
            print("❌ Error en configuración. Abortando proceso.")
            return False, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}
        
        try:
            # Obtener archivos nuevos
            archivos_nuevos = self.obtener_archivos_nuevos()
            
            if not archivos_nuevos:
                print("✅ No hay archivos nuevos para procesar")
                return True, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), self.stats
            
            # Definir patrones
            patron_ap = re.compile(r'\bap[\s_\-]*pa?g?[a-z]{2,}', re.IGNORECASE)
            patron_comp = re.compile(r'(compa|compr)[a-z]*end?[oa]s?', re.IGNORECASE)
            
            # Procesar archivos
            df_ap_consolidado, df_ap_filtrado = self.procesar_tipo_archivo(
                'AP', patron_ap, self.procesar_archivo_ap, 'pagos_ap', archivos_nuevos
            )
            
            df_comp_consolidado, df_comp_filtrado = self.procesar_tipo_archivo(
                'Comparendos', patron_comp, self.procesar_archivo_comparendos, 
                'pagos_comparendos', archivos_nuevos
            )
            
            # Generar resumen
            resumen = self.generar_resumen_final()
            
            print("✅ Procesamiento completado exitosamente")
            return True, df_ap_consolidado, df_ap_filtrado, df_comp_consolidado, df_comp_filtrado, resumen
            
        except Exception as e:
            print(f"❌ Error crítico durante el procesamiento: {e}")
            return False, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

# --- FUNCIÓN PRINCIPAL ---
def main():
    """Función principal para pruebas"""
    try:
        # Crear tablas
        crear_todas_las_tablas()
        
        # Ejemplo de uso
        carpeta_base = r"C:\ruta\a\tus\archivos"
        procesador = ProcesadorPagos(carpeta_base)
        
        exito, df_ap_cons, df_ap_filt, df_comp_cons, df_comp_filt, resumen = procesador.ejecutar()
        
        if exito:
            print(f"✅ Procesamiento exitoso!")
            print(f"AP Consolidado: {len(df_ap_cons)} registros")
            print(f"AP Filtrado: {len(df_ap_filt)} registros")
            print(f"COMP Consolidado: {len(df_comp_cons)} registros")
            print(f"COMP Filtrado: {len(df_comp_filt)} registros")
        else:
            print("❌ Error en el procesamiento")
            
    except Exception as e:
        print(f"❌ Error fatal: {e}")

# Ejecución directa para pruebas locales
if __name__ == "__main__":
    main()