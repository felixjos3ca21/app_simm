import pandas as pd
from datetime import datetime
import hashlib
import numpy as np
from typing import Tuple

def preparar_datos_sms(ruta_archivo, nombre_archivo, update_progress=None) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    """
    Versión adaptada para procesamiento de SMS
    """
    try:
        # =============================================================================
        # 1. Configuración inicial y lectura
        # =============================================================================
        total_steps = 10
        current_step = 0
        
        def update_step(message: str):
            nonlocal current_step
            current_step += 1
            if update_progress:
                progress = current_step / total_steps
                update_progress(progress, f"Paso {current_step}/{total_steps}: {message}")

        update_step("Iniciando procesamiento de SMS")
        
        # Columnas obligatorias modificadas para SMS
        columnas_requeridas = {
            'TIPO DOCUMENTO',
            'DOCUMENTO',
            'NOMBRE',
            'FECHA',
            'RESULTADO',
            'SMS',
            'BASE',
            'TELEFONO',
            'NRO_COMPARENDO',
            'RESULTADO'
        } 

        # =============================================================================
        # 2. Lectura y validación de hojas
        # =============================================================================
        update_step("Leyendo archivo Excel")
        hojas = pd.read_excel(ruta_archivo, sheet_name=None)
        
        update_step("Validando estructura de hojas")
        hojas_validas = []
        errores_hojas = []
        
        for hoja, datos in hojas.items():
            faltantes = columnas_requeridas - set(datos.columns)
            if not faltantes:
                hojas_validas.append(datos)
            else:
                errores_hojas.append(f"Hoja '{hoja}': Faltan {', '.join(faltantes)}")
        
        if not hojas_validas:
            raise ValueError(f"Ninguna hoja válida:\n" + "\n".join(errores_hojas))

        # =============================================================================
        # 3. Unificación y limpieza inicial
        # =============================================================================
        update_step("Unificando hojas válidas")
        df = pd.concat(hojas_validas, ignore_index=True).replace({
            '': pd.NA, ' ': pd.NA, 'NULL': pd.NA, 'nan': pd.NA, 'NaN': pd.NA
        })

        # =============================================================================
        # 4. Llenado de documento (Regla especial para SMS)
        # =============================================================================
        update_step("Procesando documento y teléfono")
        
        # Prioridad: documento > teléfono > comparendo
        # Asegurar conversión a string y manejar NaNs
        df['TELEFONO'] = df['TELEFONO'].astype(str).str.strip().replace('nan', pd.NA)
        df['NRO_COMPARENDO'] = df['NRO_COMPARENDO'].astype(str).str.strip().replace('nan', pd.NA)

        # Nueva lógica de llenado
        df['DOCUMENTO'] = (
            df['DOCUMENTO']
            .astype(str).str.strip()
            .replace('nan', pd.NA)
            .fillna(df['TELEFONO'])
            .fillna(df['NRO_COMPARENDO'])
        )

        # =============================================================================
        # 5. Generación de identificador_infraccion
        # =============================================================================
        update_step("Generando identificador de infracción")
        df['identificador_infraccion'] = np.where(
            df['NRO_COMPARENDO'].notna(),
            df['NRO_COMPARENDO'].astype(str),
            df['DOCUMENTO'].astype(str)
        )

        # =============================================================================
        # 6. Generación de ID único con contador de grupo
        # =============================================================================
        update_step("Creando ID único SMS")

        # 1. Definir grupo para identificar duplicados potenciales
        grupo = [
            'DOCUMENTO',
            'TELEFONO',
            'FECHA',
            'identificador_infraccion',
            'BASE'
        ]

        # 2. Crear contador de grupo
        df['contador_grupo'] = df.groupby(grupo).cumcount() + 1

        # 3. Generar hash SHA-256 incluyendo el contador
        # Asegurar todos los campos son strings y manejar NaT
        df['id_registro'] = df.apply(
            lambda x: hashlib.sha256((
                f"{str(x['DOCUMENTO'])}_"  # Forzar conversión a string
                f"{str(x['TELEFONO'])}_"
                f"{x['FECHA'].timestamp() if pd.notnull(x['FECHA']) else 'fecha_vacia'}_"  # Manejar NaT
                f"{str(x['identificador_infraccion'])}_"
                f"{str(x['BASE'])}_"
                f"{x['contador_grupo']}"
            ).encode()).hexdigest(),
            axis=1
        )

        # 4. Eliminar columna temporal
        df.drop('contador_grupo', axis=1, inplace=True)

        # =============================================================================
        # 7. Validación de fechas SMS (Solo fecha)
        # =============================================================================
        update_step("Validando fechas SMS")

        # 1. Conversión a fecha
        try:
            # Convertir a datetime y extraer solo fecha
            df['FECHA'] = df['FECHA'].astype(str)
            df['FECHA'] = pd.to_datetime(
                df['FECHA'],              # Columna origen
                format='%Y-%m-%d',        # Formato estricto YYYY-MM-DD
                errors='coerce'           # Convertir errores a NaT
            ).dt.date                     # Extraer componente fecha (type datetime.date)
            
        except KeyError:
            raise ValueError("Columna 'FECHA' no encontrada en el archivo")

        # 2. Detección y reporte de fechas inválidas
        fechas_invalidas = df[df['FECHA'].isna()]
        if not fechas_invalidas.empty:
            print("\n⚠️ ERRORES EN FECHAS - Registros afectados:")
            print(fechas_invalidas[['FECHA', 'DOCUMENTO', 'TELEFONO']].head(5))
            print(f"Total registros con fechas inválidas: {len(fechas_invalidas)}")
            print("Formato requerido: YYYY-MM-DD (Ej: 2024-03-15)")

        # =============================================================================
        # 8. Mapeo final a estructura de la tabla SMS (Versión Corregida)
        # =============================================================================
        update_step("Mapeando a estructura final")

        df_final = pd.DataFrame({
            # Hash generado previamente
            'id_registro': df['id_registro'], 
            
            # Mapeo directo de columnas
            'tipo_documento': df['TIPO DOCUMENTO'].str[:50],  # Ajuste a VARCHAR(50)
            'documento': df['DOCUMENTO'].str[:30],            # Ajuste a VARCHAR(30)
            'nombre_usuario': df['NOMBRE'].str[:100],         # Ajuste a VARCHAR(100)
            
            # Fecha ya validada
            'fecha_sms': df['FECHA'],                     # Tipo DATE
            
            # Resultado del SMS
            'resultado': df['RESULTADO'].str[:100],       # Ajuste a VARCHAR(100)
            
            # Texto del mensaje SMS
            'texto_sms': df['SMS'].str[:250],                 # Ajuste a VARCHAR(250)
            
            # Base/Funcionario
            'base': df['BASE'].str[:50],                      # Ajuste a VARCHAR(50)
            
            # Datos de contacto
            'telefono': df['TELEFONO'].astype(str).str[:20],             # Ajuste a VARCHAR(20)
            
            # Datos de comparendos
            'numero_comparendo': df['NRO_COMPARENDO'].astype(str).str[:50],

            # Campo calculado (usando NRO COMPARENDO como fallback)
            'identificador_infraccion': df['NRO_COMPARENDO'].fillna(''),  # VARCHAR(50)
            
            # Metadatos
            'archivo_origen': nombre_archivo[:100],       # Ajuste a VARCHAR(100)
            'fecha_carga': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

        # =============================================================================
        # 9. Separación de registros válidos/erróneos
        # =============================================================================
        update_step("Clasificando registros")
        
        # Campos obligatorios para validación
        campos_obligatorios = ['documento', 'telefono', 'fecha_sms']

        # 1. Conversión segura a string
        df_final[campos_obligatorios] = df_final[campos_obligatorios].apply(
            lambda col: col.astype(str).str.strip().replace(['nan', 'NaT', '<NA>'], pd.NA)
        )

        # 2. Detección de valores faltantes o inválidos
        mask_errores = df_final[campos_obligatorios].isna().any(axis=1)

        # 3. Separación de registros
        df_errores = df_final[mask_errores].copy()
        df_procesado = df_final[~mask_errores].copy()

        # 4. Validación adicional de formato para teléfono (opcional)
        df_errores = pd.concat([
            df_errores,
            df_procesado[~df_procesado['telefono'].str.match(r'^\d{7,15}$', na=False)]
        ])
        df_procesado = df_procesado[df_procesado['telefono'].str.match(r'^\d{7,15}$', na=False)]

        return df_procesado, df_errores, "\n".join(errores_hojas)

    except Exception as e:
        raise ValueError(f"Error en paso {current_step}: {str(e)}")