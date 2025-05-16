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

# Diccionarios para nombres en español
MESES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}

DIAS = {
    0: "Lunes", 1: "Martes", 2: "Miércoles", 3: "Jueves",
    4: "Viernes", 5: "Sábado", 6: "Domingo"
}

def crear_tabla_calendario():
    """Crea la tabla dim_calendario si no existe"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS calendario (
            fecha DATE PRIMARY KEY,
            dia INT,
            nombre_dia VARCHAR(10),
            dia_semana INT,
            mes INT,
            nombre_mes VARCHAR(10),
            trimestre INT,
            año INT,
            semana_mes INT,
            semana_anio INT
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
        hoy = datetime.now().date()
        return hoy - timedelta(days=365), hoy
    finally:
        conn.close()


def generar_datos_calendario(fecha_inicio, fecha_fin):
    """Genera un DataFrame con todos los datos del calendario en español"""
    rango_fechas = pd.date_range(start=fecha_inicio, end=fecha_fin, freq='D')
    datos = []
    
    for fecha in rango_fechas:
        dia_num = fecha.day
        mes_num = fecha.month
        datos.append({
            "fecha": fecha.date(),
            "dia": dia_num,
            "nombre_dia": DIAS[fecha.weekday()],
            "dia_semana": fecha.weekday() + 1,  # 1=Lunes, 7=Domingo
            "mes": mes_num,
            "nombre_mes": MESES[mes_num],
            "trimestre": (mes_num - 1) // 3 + 1,
            "año": fecha.year,
            "semana_mes": (dia_num - 1) // 7 + 1,
            "semana_anio": fecha.isocalendar()[1]
        })
    
    return pd.DataFrame(datos)

def insertar_calendario_postgres(df):
    """Inserta los datos del calendario en PostgreSQL"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        cursor.execute("TRUNCATE TABLE calendario;")
        
        for _, row in df.iterrows():
            cursor.execute("""
            INSERT INTO calendario (
                fecha, dia, nombre_dia, dia_semana, mes, nombre_mes,
                trimestre, año, semana_mes, semana_anio
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                row['fecha'], row['dia'], row['nombre_dia'], row['dia_semana'],
                row['mes'], row['nombre_mes'], row['trimestre'], row['año'],
                row['semana_mes'], row['semana_anio']
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
    print("\n⚡ Iniciando generación de calendario en español...")
    
    crear_tabla_calendario()
    fecha_inicio, fecha_fin = obtener_rango_fechas()
    df_calendario = generar_datos_calendario(fecha_inicio, fecha_fin)
    
    print("\n🔍 Muestra de datos generados:")
    print(df_calendario.head(3))
    
    insertar_calendario_postgres(df_calendario)
    print("🎉 ¡Calendario generado con éxito!")