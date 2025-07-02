import pandas as pd
import os
from pathlib import Path
import chardet
import hashlib
from datetime import datetime
import numpy as np
import re
import logging
import sys
from typing import Set, List, Optional
import json

# --- CONFIGURACIÓN ---
CONFIG = {
    'CARPETA_BASE': r"C:\Users\1513873.ANDESBPO\ANDES BPO S.A.S\Johan Felipe JG. Gómez Arango - SIMM\BASES\Pagos",
    'ARCHIVO_PROCESADOS': 'archivos_procesados.csv',
    'ARCHIVO_CONFIG': 'config_procesamiento.json',
    'CHUNK_SIZE': 10000,  # Para procesar archivos grandes en chunks
    'MAX_ENCODING_BYTES': 100000
}

# --- CONFIGURACIÓN DE LOGGING ---
def configurar_logging():
    """Configura el sistema de logging con un único archivo"""
    log_filename = "procesamiento_pagos.log"  # Nombre fijo
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8', mode='a'),  # 'a' para append
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

# --- CLASE PRINCIPAL ---
class ProcesadorPagos:
    def __init__(self, config: dict):
        self.config = config
        self.logger = configurar_logging()
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
        """Carga la lista de archivos ya procesados"""
        archivo_procesados = self.config['ARCHIVO_PROCESADOS']
        if Path(archivo_procesados).exists():
            try:
                df = pd.read_csv(archivo_procesados)
                return set(df['ruta_archivo'].tolist())
            except Exception as e:
                self.logger.warning(f"Error cargando archivos procesados: {e}")
        return set()
    
    def _guardar_archivo_procesado(self, ruta_archivo: str, tipo_archivo: str, 
                                 registros_procesados: int, estado: str):
        """Registra un archivo como procesado"""
        nuevo_registro = {
            'ruta_archivo': ruta_archivo,
            'tipo_archivo': tipo_archivo,
            'fecha_procesamiento': datetime.now(),
            'registros_procesados': registros_procesados,
            'estado': estado,
            'hash_archivo': self._calcular_hash_archivo(ruta_archivo)
        }
        
        archivo_procesados = self.config['ARCHIVO_PROCESADOS']
        if Path(archivo_procesados).exists():
            df_existente = pd.read_csv(archivo_procesados)
            df_nuevo = pd.concat([df_existente, pd.DataFrame([nuevo_registro])], ignore_index=True)
        else:
            df_nuevo = pd.DataFrame([nuevo_registro])
        
        df_nuevo.to_csv(archivo_procesados, index=False)
    
    def _calcular_hash_archivo(self, ruta_archivo: str) -> str:
        """Calcula hash del archivo para detectar cambios"""
        try:
            with open(ruta_archivo, 'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
        except Exception:
            return ""
    
    def _validar_configuracion(self) -> bool:
        """Valida que la configuración sea correcta"""
        carpeta_base = Path(self.config['CARPETA_BASE'])
        if not carpeta_base.exists():
            self.logger.error(f"La carpeta base no existe: {carpeta_base}")
            return False
        
        if not carpeta_base.is_dir():
            self.logger.error(f"La ruta no es una carpeta: {carpeta_base}")
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
                self.logger.warning(f"Baja confianza en encoding para {archivo.name}: {encoding} ({confidence:.2f})")
            
            return encoding or 'utf-8'
        except Exception as e:
            self.logger.warning(f"Error detectando encoding para {archivo.name}: {e}")
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
        
        # Intento con pd.to_datetime automático como último recurso
        try:
            return pd.to_datetime(fecha_str, errors='raise')
        except:
            self.logger.warning(f"No se pudo parsear la fecha: {fecha_str}")
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
            
            # Leer en chunks si el archivo es muy grande
            try:
                df = pd.read_csv(archivo, sep='\t', encoding=encoding, dtype=str)
            except UnicodeDecodeError:
                # Fallback a latin-1 si falla
                self.logger.warning(f"Fallback a latin-1 para {archivo.name}")
                df = pd.read_csv(archivo, sep='\t', encoding='latin-1', dtype=str)
            
            if df.empty:
                self.logger.warning(f"Archivo vacío: {archivo.name}")
                return pd.DataFrame()
            
            # Validar columnas requeridas
            columnas_requeridas = ['nro_acuerdo', 'id_usuario', 'valor', 'fecha_liquida']
            columnas_disponibles = [col for col in columnas_requeridas if col in df.columns]
            
            if len(columnas_disponibles) < 3:  # Al menos 3 columnas críticas
                self.logger.warning(f"Columnas insuficientes en {archivo.name}: {df.columns.tolist()}")
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
            
            self.logger.info(f"AP - {archivo.name}: {len(df)} registros válidos")
            return df
            
        except Exception as e:
            self.logger.error(f"Error procesando AP {archivo.name}: {e}")
            return pd.DataFrame()
    
    def procesar_archivo_comparendos(self, archivo: Path) -> pd.DataFrame:
        """Procesa archivos de Comparendos"""
        try:
            encoding = self.detectar_encoding(archivo)
            
            try:
                df = pd.read_csv(archivo, sep='\t', encoding=encoding, dtype=str)
            except UnicodeDecodeError:
                self.logger.warning(f"Fallback a latin-1 para {archivo.name}")
                df = pd.read_csv(archivo, sep='\t', encoding='latin-1', dtype=str)
            
            if df.empty:
                self.logger.warning(f"Archivo vacío: {archivo.name}")
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
            
            self.logger.info(f"COMP - {archivo.name}: {len(df)} registros válidos")
            return df
            
        except Exception as e:
            self.logger.error(f"Error procesando COMP {archivo.name}: {e}")
            return pd.DataFrame()
    
    def obtener_archivos_nuevos(self) -> List[Path]:
        """Obtiene lista de archivos .txt que no han sido procesados"""
        carpeta_base = Path(self.config['CARPETA_BASE'])
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
        self.logger.info(f"Archivos encontrados: {len(todos_txt)}, Nuevos: {len(archivos_nuevos)}")
        
        return archivos_nuevos
    
    def procesar_tipo_archivo(self, tipo: str, patron_regex: re.Pattern, 
                            funcion_procesar, nombre_salida: str, archivos: List[Path]):
        """Procesa archivos de un tipo específico"""
        archivos_tipo = [
            archivo for archivo in archivos 
            if patron_regex.search(archivo.name)
        ]
        
        self.logger.info(f"[{tipo}] Archivos encontrados: {len(archivos_tipo)}")
        
        if not archivos_tipo:
            self.logger.info(f"No se encontraron archivos para [{tipo}]")
            return
        
        dfs_validos = []
        archivos_procesados_ok = 0
        archivos_con_error = 0
        
        for archivo in archivos_tipo:
            try:
                self.logger.info(f"Procesando [{tipo}]: {archivo.name}")
                df_temp = funcion_procesar(archivo)
                
                if not df_temp.empty:
                    dfs_validos.append(df_temp)
                    self._guardar_archivo_procesado(
                        str(archivo.resolve()), tipo, len(df_temp), 'EXITOSO'
                    )
                    archivos_procesados_ok += 1
                else:
                    self._guardar_archivo_procesado(
                        str(archivo.resolve()), tipo, 0, 'SIN_DATOS'
                    )
                    
            except Exception as e:
                self.logger.error(f"Error procesando {archivo.name}: {e}")
                self._guardar_archivo_procesado(
                    str(archivo.resolve()), tipo, 0, 'ERROR'
                )
                archivos_con_error += 1
        
        # Consolidar resultados
        if dfs_validos:
            df_final = pd.concat(dfs_validos, ignore_index=True)
            
            # Eliminar duplicados
            df_filtrado = df_final.drop_duplicates(subset='id_registro')
            
            # Generar archivos de salida
            archivo_procesados = f'{nombre_salida}_procesados.csv'
            archivo_filtrados = f'{nombre_salida}_filtrados.csv'
            
            df_final.to_csv(archivo_procesados, index=False)
            df_filtrado.to_csv(archivo_filtrados, index=False)
            
            # Estadísticas
            if tipo == 'AP':
                self.stats['registros_ap'] = len(df_filtrado)
            else:
                self.stats['registros_comp'] = len(df_filtrado)
            
            self.logger.info(f"[{tipo}] Resultados:")
            self.logger.info(f"  • Registros totales: {len(df_final)}")
            self.logger.info(f"  • Registros únicos: {len(df_filtrado)}")
            self.logger.info(f"  • Valor promedio: {df_filtrado['valor'].mean():.2f}")
            self.logger.info(f"  • Rango fechas: {df_filtrado['fecha_liquida'].min()} - {df_filtrado['fecha_liquida'].max()}")
            self.logger.info(f"  • Archivo generado: {archivo_filtrados}")
        
        self.stats['archivos_procesados_exitosos'] += archivos_procesados_ok
        self.stats['archivos_con_errores'] += archivos_con_error
    
    def generar_resumen_final(self):
        """Genera resumen final del procesamiento"""
        self.logger.info("="*60)
        self.logger.info("RESUMEN FINAL DEL PROCESAMIENTO")
        self.logger.info("="*60)
        self.logger.info(f"📁 Archivos encontrados: {self.stats['archivos_encontrados']}")
        self.logger.info(f"🆕 Archivos nuevos: {self.stats['archivos_nuevos']}")
        self.logger.info(f"✅ Archivos procesados exitosamente: {self.stats['archivos_procesados_exitosos']}")
        self.logger.info(f"❌ Archivos con errores: {self.stats['archivos_con_errores']}")
        self.logger.info(f"💰 Registros AP procesados: {self.stats['registros_ap']}")
        self.logger.info(f"🚗 Registros Comparendos procesados: {self.stats['registros_comp']}")
        self.logger.info(f"📊 Total registros: {self.stats['registros_ap'] + self.stats['registros_comp']}")
        
        # Guardar resumen en JSON para procesamiento posterior
        resumen = {
            'fecha_procesamiento': datetime.now().isoformat(),
            'estadisticas': self.stats
        }
        
        with open(f"resumen_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", 'w', encoding='utf-8') as f:
            json.dump(resumen, f, indent=2, ensure_ascii=False)
    
    def ejecutar(self):
        """Método principal de ejecución"""
        self.logger.info("🚀 Iniciando procesamiento de pagos")
        
        # Validar configuración
        if not self._validar_configuracion():
            self.logger.error("❌ Error en configuración. Abortando proceso.")
            return False
        
        try:
            # Obtener archivos nuevos
            archivos_nuevos = self.obtener_archivos_nuevos()
            
            if not archivos_nuevos:
                self.logger.info("✅ No hay archivos nuevos para procesar")
                return True
            
            # Definir patrones
            patron_ap = re.compile(r'\bap[\s_\-]*pa?g?[a-z]{2,}', re.IGNORECASE)
            patron_comp = re.compile(r'(compa|compr)[a-z]*end?[oa]s?', re.IGNORECASE)
            
            # Procesar cada tipo
            self.procesar_tipo_archivo(
                'AP', patron_ap, self.procesar_archivo_ap, 'pagos_ap', archivos_nuevos
            )
            
            self.procesar_tipo_archivo(
                'Comparendos', patron_comp, self.procesar_archivo_comparendos, 
                'pagos_comparendos', archivos_nuevos
            )
            
            # Generar resumen
            self.generar_resumen_final()
            
            self.logger.info("✅ Procesamiento completado exitosamente")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error crítico durante el procesamiento: {e}")
            return False

# --- FUNCIÓN PRINCIPAL ---
def main():
    """Función principal"""
    try:
        procesador = ProcesadorPagos(CONFIG)
        exito = procesador.ejecutar()
        
        if not exito:
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⚠️ Proceso interrumpido por el usuario")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error fatal: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()