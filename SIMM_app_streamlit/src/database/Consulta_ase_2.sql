select * from asesor_2
limit 5
;

-- Vaciar la tabla principal de asesor_2
DROP TABLE asesor_2;

-- Vaciar la tabla de control de archivos procesados
DROP TABLE archivos_procesados_ase2

ALTER TABLE asesor_2 ALTER COLUMN time TYPE INTERVAL 
USING (make_interval(secs => time::numeric / 1000000000.0));

SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'asesor_2' AND column_name = 'time';-- Suponiendo que la columna 'time' está en nanosegundos
SELECT
    time,
    -- Convertir nanosegundos a segundos y luego a intervalo
    (time / 1000000000)::int AS segundos,
    -- Mostrar como HH:MM:SS usando make_interval
    TO_CHAR(
        make_interval(secs => (time / 1000000000)::int),
        'HH24:MI:SS'
    ) AS tiempo_hhmmss
FROM asesor_2;