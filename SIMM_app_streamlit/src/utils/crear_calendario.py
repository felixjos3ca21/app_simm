# -*- coding: utf-8 -*-
import psycopg2
from datetime import datetime, timedelta
import pandas as pd
from dateutil.relativedelta import relativedelta

# Configuración de la conexión a PostgreSQL (ajusta estos valores)
DB_CONFIG = {
    'host': 'localhost',
    'database': 'SIMM',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

def crear_tabla_calendario():
    """Crea la tabla dim_calendario si no existe"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_calendario (
            fecha DATE PRIMARY KEY,
            dia INT,
            nombre_dia VARCHAR(10),
            dia_semana INT,
            mes INT,
            nombre_mes VARCHAR(10),
            trimestre INT,
            año INT,
            semana_mes INT,
            semana_anio INT,
            es_fin_de_semana BOOLEAN,
            es_feriado BOOLEAN DEFAULT FALSE,
            nombre_feriado VARCHAR(100) DEFAULT NULL
        );
        """)
        conn.commit()
        print("✅ Tabla 'dim_calendario' creada/existe")
    except Exception as e:
        print(f"❌ Error al crear tabla: {e}")
    finally:
        cursor.close()
        conn.close()

def obtener_rango_fechas():
    """Obtiene el rango de fechas desde la tabla gestiones"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        query = """
        SELECT 
            MIN(fecha_gestion) as fecha_minima,
            MAX(fecha_gestion) as fecha_maxima
        FROM gestiones;
        """
        df = pd.read_sql(query, conn)
        fecha_min = df.iloc[0]['fecha_minima'].date()
        fecha_max = df.iloc[0]['fecha_maxima'].date()
        print(f"📅 Rango de fechas: {fecha_min} a {fecha_max}")
        return fecha_min, fecha_max
    except Exception as e:
        print(f"❌ Error al obtener rango: {e}")
        # Valores por defecto si hay error
        hoy = datetime.now().date()
        return hoy - timedelta(days=365), hoy
    finally:
        conn.close()

def generar_datos_calendario(fecha_inicio, fecha_fin):
    """Genera un DataFrame con todos los datos del calendario"""
    rango_fechas = pd.date_range(start=fecha_inicio, end=fecha_fin, freq='D')
    datos = []
    
    for fecha in rango_fechas:
        datos.append({
            "fecha": fecha.date(),
            "dia": fecha.day,
            "nombre_dia": fecha.strftime('%A'),
            "dia_semana": fecha.weekday() + 1,  # 1=Lunes, 7=Domingo
            "mes": fecha.month,
            "nombre_mes": fecha.strftime('%B'),
            "trimestre": (fecha.month - 1) // 3 + 1,
            "año": fecha.year,
            "semana_mes": (fecha.day - 1) // 7 + 1,  # Semana 1: días 1-7
            "semana_anio": fecha.isocalendar()[1],
            "es_fin_de_semana": fecha.weekday() >= 5,  # 5=Sab, 6=Dom
            "es_feriado": False,
            "nombre_feriado": None
        })
    
    return pd.DataFrame(datos)

def insertar_calendario_postgres(df):
    """Inserta los datos del calendario en PostgreSQL"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        # Borrar datos existentes para evitar duplicados
        cursor.execute("TRUNCATE TABLE dim_calendario;")
        
        # Insertar nuevos datos
        for _, row in df.iterrows():
            cursor.execute("""
            INSERT INTO dim_calendario (
                fecha, dia, nombre_dia, dia_semana, mes, nombre_mes,
                trimestre, año, semana_mes, semana_anio, es_fin_de_semana
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                row['fecha'], row['dia'], row['nombre_dia'], row['dia_semana'],
                row['mes'], row['nombre_mes'], row['trimestre'], row['año'],
                row['semana_mes'], row['semana_anio'], row['es_fin_de_semana']
            ))
        
        conn.commit()
        print(f"✅ Insertados {len(df)} registros en dim_calendario")
    except Exception as e:
        print(f"❌ Error al insertar datos: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    print("\n⚡ Iniciando generación de calendario...")
    
    # Paso 1: Crear tabla si no existe
    crear_tabla_calendario()
    
    # Paso 2: Obtener rango de fechas desde gestiones
    fecha_inicio, fecha_fin = obtener_rango_fechas()
    
    # Paso 3: Generar DataFrame con todos los datos
    df_calendario = generar_datos_calendario(fecha_inicio, fecha_fin)
    print(f"📊 Datos generados: {len(df_calendario)} filas")
    
    # Paso 4: Insertar en PostgreSQL
    insertar_calendario_postgres(df_calendario)
    
    print("🎉 ¡Proceso completado!")