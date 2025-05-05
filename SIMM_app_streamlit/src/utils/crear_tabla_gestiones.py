from sqlalchemy import create_engine, text

# Configuración de conexión
DB_CONFIG = {
    'host': 'localhost',
    'database': 'SIMM',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

def crear_tabla_gestiones():
    try:
        # Conexión a PostgreSQL
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )

        # Script SQL corregido
        sql_script = """
        CREATE TABLE IF NOT EXISTS gestiones (
            id_registro VARCHAR(64) PRIMARY KEY,         -- ID hash SHA-256 (64 caracteres)
            id_gestion_Campaña VARCHAR(50),              -- "Id Gestion Campaña"
            tipo_documento VARCHAR(50),                   -- "Tipo documento"
            documento VARCHAR(30) NOT NULL,               -- "Número documento"
            nombre_usuario VARCHAR(100),                  -- "Nombre"
            fecha_gestion TIMESTAMP NOT NULL,            -- "Fecha gestión"
            tipo_llamada VARCHAR(50),                    -- "Tipo llamada"
            id_gestion VARCHAR(50) NOT NULL,              -- "Código gestión"
            resultado VARCHAR(100) NOT NULL,             -- "Resultado"
            fecha_compromiso DATE,                       -- "Fecha Compromiso"
            asesor VARCHAR(100),                         -- "Funcionario"
            campana VARCHAR(50) NOT NULL,                 -- "Campaña"
            telefono VARCHAR(20),                        -- "Teléfono"
            obligacion VARCHAR(50),                      -- "Obligación"
            numero_comparendo VARCHAR(50),               -- "Nro. Comparendo"
            valor NUMERIC(12, 2),                        -- "Valor"
            identificador_infraccion VARCHAR(50),        -- Campo calculado
            archivo_origen VARCHAR(100),                 -- Nombre del archivo fuente
            fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Fecha de carga automática
        );"""

        # Ejecutar el script
        with engine.connect() as conn:
            conn.execute(text(sql_script))
            conn.commit()

        print("✅ Tabla 'gestiones' creada exitosamente con tus modificaciones")

    except Exception as e:
        print(f"❌ Error al crear la tabla: {e}")

if __name__ == "__main__":
    crear_tabla_gestiones()