from sqlalchemy import create_engine, text

# Configuración de conexión
DB_CONFIG = {
    'host': 'localhost',
    'database': 'SIMM',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

def crear_tablas():
    try:
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )

        # scripts debe ser UN DICCIONARIO: nombre_tabla -> SQL
        scripts = {
            'pagos': """
                CREATE TABLE IF NOT EXISTS pagos (
                    id_registro INT PRIMARY KEY,
                    codcliente VARCHAR(50),
                    nitcliente VARCHAR(50),
                    numobligacion VARCHAR(50),
                    fechapago DATE,
                    valorpago NUMERIC(15, 2),
                    base VARCHAR(100),
                    fecha_gest DATE,
                    aplica_pago_gestion VARCHAR(20),
                    fecha_sms DATE,
                    campania VARCHAR(50),
                    aplica_pago_sms VARCHAR(50),
                    aplicacion_final VARCHAR(50),
                    fecha_sencilla DATE,
                    anio INT,
                    dia INT,
                    mes INT,
                    semana INT,
                    numero_mes INT,
                    nombre_dia_semana VARCHAR(20),
                    cruce_sms VARCHAR(50),
                    estrategia VARCHAR(100),
                    infraccion VARCHAR(100),
                    periodo_21_20 VARCHAR(50),
                    archivo_origen VARCHAR(255),
                    fecha_carga DATE
                );
            """
        }

        # Usar engine.begin() abre una transacción y la commitea al salir
        with engine.begin() as conn:
            for nombre, script in scripts.items():
                conn.execute(text(script))
                print(f"✅ Tabla '{nombre}' creada/verificada exitosamente.")

    except Exception as e:
        print(f"❌ Error al crear las tablas: {e}")

if __name__ == "__main__":
    crear_tablas()
