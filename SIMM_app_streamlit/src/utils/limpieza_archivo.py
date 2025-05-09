import pandas as pd
from datetime import datetime
import hashlib
import numpy as np
from typing import Tuple

def preparar_datos(ruta_arcivo, nombre_archivo, update_progress=None) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    """
    Versión corregida con manejo robusto de datos faltantes
    """
    try:
        # =============================================================================
        # 1. Configuración inicial
        # =============================================================================
        total_steps = 12  # Reducido porque eliminamos pasos redundantes
        current_step = 0
        
        def update_step(message: str):
            nonlocal current_step
            current_step += 1
            progress = current_step / total_steps
            if update_progress:
                update_progress(progress, f"Paso {current_step}/{total_steps}: {message}")

        update_step("Iniciando procesamiento")
        
        # =============================================================================
        # 2. Lectura y validación de hojas
        # =============================================================================
        update_step("Leyendo archivo Excel")
        hojas = pd.read_excel(ruta_arcivo, sheet_name=None)
        
        # Columnas requeridas (usando nombres originales del Excel)
        columnas_requeridas = [
            'Id Gestion Campaña', 'Tipo documento', 'Número documento', 'Nombre',
            'Fecha gestión', 'Tipo llamada', 'Código gestión', 'Resultado',
            'Fecha Compromiso', 'Funcionario', 'Campaña', 'Teléfono',
            'Obligación', 'Nro. Comparendo', 'Valor'
        ]
        
        update_step("Validando estructura de hojas")
        hojas_validas = []
        errores_hojas = []
        
        for hoja, datos in hojas.items():
            faltantes = [col for col in columnas_requeridas if col not in datos.columns]
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
        # 4. Manejo de datos faltantes (MOVIDO AL INICIO)
        # =============================================================================
        update_step("Manejando datos faltantes")
        
        # Valores por defecto
        df['Id Gestion Campaña'] = df['Id Gestion Campaña'].fillna('sin registro')
        df['Tipo documento'] = df['Tipo documento'].fillna('sin registro')
        df['Nombre'] = df['Nombre'].fillna('sin registro')
        df['Tipo llamada'] = df['Tipo llamada'].fillna('Saliente')
        df['Campaña'] = df['Campaña'].fillna('SIN CAMPAÑA')
        df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce').fillna(0)
        
        # Teléfono = Número documento si está vacío
        df['Teléfono'] = np.where(
            df['Teléfono'].isna() | (df['Teléfono'] == ''),
            df['Número documento'].astype(str),
            df['Teléfono']
        )

        # =============================================================================
        # 5. Generación de Código Gestión 
        # =============================================================================
        update_step("Generando códigos de gestión")
        mask = df['Código gestión'].isna() | (df['Código gestión'] == '')
        df.loc[mask, 'Código gestión'] = (
            df['Número documento'].astype(str).str.strip() + '_' +
            df['Teléfono'].astype(str).str.strip()
        )

        # =============================================================================
        # 6. Validación de fechas
        # =============================================================================
        update_step("Procesando fechas")
        df['Fecha gestión'] = pd.to_datetime(df['Fecha gestión'], errors='coerce')
        df['Fecha Compromiso'] = pd.to_datetime(df['Fecha Compromiso'], errors='coerce').dt.normalize()

        # =============================================================================
        # 7. Generación de identificador_infraccion
        # =============================================================================
        update_step("Generando identificador de infracción")
        df['identificador_infraccion'] = np.where(
            df['Obligación'].notna(),
            df['Obligación'].astype(str),
            df['Nro. Comparendo'].astype(str)
        )

        # =============================================================================
        # 8. Generación de ID único
        # =============================================================================
        update_step("Creando ID único")
        grupo = ['Número documento', 'Teléfono', 'Funcionario', 'Fecha gestión', 
                'Código gestión', 'identificador_infraccion', 'Resultado']
        
        df['contador_grupo'] = df.groupby(grupo).cumcount() + 1
        df['id registro'] = df.apply(lambda x: hashlib.sha256((
            f"{x['Número documento']}_{x['Teléfono']}_{x['Funcionario']}_"
            f"{x['Fecha gestión'].timestamp() if pd.notnull(x['Fecha gestión']) else ''}_"
            f"{x['Código gestión']}_{x['identificador_infraccion']}_"
            f"{x['Resultado']}_{x['contador_grupo']}"
        ).encode()).hexdigest(), axis=1)
        df.drop('contador_grupo', axis=1, inplace=True)

        # =============================================================================
        # 9. Normalización de nombres de columnas (ANTES de validaciones)
        # =============================================================================
        update_step("Normalizando nombres de columnas")
        mapeo_columnas = {
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
            'id registro': 'id_registro'
        }
        df = df.rename(columns=mapeo_columnas)

        # =============================================================================
        # 10. Validación final de datos
        # =============================================================================
        update_step("Validando integridad")
        
        # Verificar IDs únicos
        if df['id_registro'].duplicated().any():
            duplicados = df[df.duplicated('id_registro', keep=False)]
            raise ValueError(f"IDs duplicados:\n{duplicados[['documento', 'telefono']]}")
            
        # Verificar fechas válidas
        if df['fecha_gestion'].isna().any():
            raise ValueError("Fecha gestión contiene valores no válidos")

        # =============================================================================
        # 11. Separación de registros válidos/erróneos (SIMPLIFICADO)
        # =============================================================================
        update_step("Clasificando registros")
        campos_obligatorios = ['documento', 'telefono', 'asesor', 'fecha_gestion', 'id_gestion']
        mask_errores = df[campos_obligatorios].isna().any(axis=1)
        
        # Agregar motivo del error
        df['error'] = ''
        for campo in campos_obligatorios:
            df.loc[df[campo].isna(), 'error'] += f'{campo} faltante; '
        
        df_errores = df[mask_errores].copy()
        df_procesado = df[~mask_errores].copy()

        # =============================================================================
        # 12. Metadatos finales
        # =============================================================================
        update_step("Finalizando procesamiento")
        df_procesado['fecha_gestion_sencilla'] = df_procesado['fecha_gestion'].dt.strftime('%Y-%m-%d')
        df_procesado['archivo_origen'] = nombre_archivo
        df_procesado['fecha_carga'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df_errores['archivo_origen'] = nombre_archivo
        df_errores['fecha_carga'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Asegurar columnas finales
        columnas_finales = [
            'id_registro', 'id_gestion_campaña', 'tipo_documento', 'documento',
            'nombre_usuario', 'fecha_gestion', 'tipo_llamada', 'id_gestion',
            'resultado', 'fecha_compromiso', 'asesor', 'campana', 'telefono',
            'obligacion', 'numero_comparendo', 'valor', 'identificador_infraccion',
            'archivo_origen', 'fecha_carga', 'fecha_gestion_sencilla'
        ]
        
        return (
            df_procesado.reindex(columns=[c for c in columnas_finales if c in df_procesado.columns]),
            df_errores.reindex(columns=[c for c in columnas_finales if c in df_errores.columns]),
            "\n".join(errores_hojas)
        )

    except Exception as e:
        raise ValueError(f"Error en paso {current_step}: {str(e)}")