select * from mv_pagos_relacionados;


CREATE MATERIALIZED VIEW mv_pagos_relacionados AS
WITH pagos_filtrados AS (
    SELECT 
        p.id_registro,
        p.nro_acuerdo,
        p.nro_comparendo,
        p.documento,
        p.nombre_usuario,
        p.valor,
        p.fecha_pago,
        -- Añadimos ROW_NUMBER para identificar duplicados
        ROW_NUMBER() OVER (
            PARTITION BY p.id_registro 
            ORDER BY p.fecha_pago DESC, p.valor DESC
        ) AS rn_dedup
    FROM pagos p
    --WHERE p.fecha_pago BETWEEN '2024-12-01' AND '2024-12-15'
),
pagos_unicos AS (
    -- Solo tomamos un registro por id_registro
    SELECT 
        id_registro,
        nro_acuerdo,
        nro_comparendo,
        documento,
        nombre_usuario,
        valor,
        fecha_pago
    FROM pagos_filtrados 
    WHERE rn_dedup = 1
),
gestiones_asociadas AS (
    SELECT 
        pu.*,
        g.fecha_gestion,
        g.resultado AS resultado_gestion,
        g.asesor,
        'GESTION' AS fuente,
        ROW_NUMBER() OVER (PARTITION BY pu.id_registro ORDER BY g.fecha_gestion DESC) AS rn_gestion
    FROM pagos_unicos pu
    LEFT JOIN mv_gestiones_comparendo g ON 
        (pu.nro_comparendo = g.numero_comparendo OR pu.nro_acuerdo = g.numero_comparendo)
        AND pu.documento = g.documento
        AND g.fecha_gestion <= pu.fecha_pago
),
sms_asociados AS (
    SELECT 
        pu.*,
        s.fecha_sms AS fecha_sms,
        s.resultado AS resultado_sms,
        'SMS' AS fuente,
        ROW_NUMBER() OVER (PARTITION BY pu.id_registro ORDER BY s.fecha_sms DESC) AS rn_sms
    FROM pagos_unicos pu
    LEFT JOIN mv_sms_comparendos s ON 
        (pu.nro_comparendo = s.numero_comparendo OR pu.nro_acuerdo = s.numero_comparendo)
        AND pu.documento = s.documento
        AND s.fecha_sms <= pu.fecha_pago
)
SELECT 
    p.id_registro,
    p.nro_acuerdo,
    p.nro_comparendo,
    p.documento,
    p.nombre_usuario,
    p.valor,
    p.fecha_pago,
    
    -- Datos de la última gestión
    g.fecha_gestion AS ultima_gestion_fecha,
    g.resultado_gestion AS ultima_gestion_resultado,
    g.asesor AS ultima_gestion_asesor,
    
    -- Datos del último SMS
    s.fecha_sms AS ultimo_sms_fecha,
    s.resultado_sms AS ultimo_sms_resultado,
    
    -- Flags para saber si tuvo gestiones o SMS
    CASE WHEN g.fecha_gestion IS NOT NULL THEN TRUE ELSE FALSE END AS tiene_gestiones,
    CASE WHEN s.fecha_sms IS NOT NULL THEN TRUE ELSE FALSE END AS tiene_sms,
    
    -- Contadores (usando pagos_unicos en lugar de pagos_filtrados)
    (SELECT COUNT(*) FROM mv_gestiones_comparendo gc 
     WHERE (p.nro_comparendo = gc.numero_comparendo OR p.nro_acuerdo = gc.numero_comparendo)
     AND p.documento = gc.documento
     AND gc.fecha_gestion <= p.fecha_pago) AS total_gestiones,
     
    (SELECT COUNT(*) FROM mv_sms_comparendos sc 
     WHERE (p.nro_comparendo = sc.numero_comparendo OR p.nro_acuerdo = sc.numero_comparendo)
     AND p.documento = sc.documento
     AND sc.fecha_sms <= p.fecha_pago) AS total_sms
FROM pagos_unicos p
LEFT JOIN gestiones_asociadas g ON p.id_registro = g.id_registro AND g.rn_gestion = 1
LEFT JOIN sms_asociados s ON p.id_registro = s.id_registro AND s.rn_sms = 1;



CREATE MATERIALIZED VIEW mv_pagos_relacionados_3 AS
WITH pagos_unicos AS (
    SELECT DISTINCT ON (id_registro)
        id_registro,
        nro_acuerdo,
        nro_comparendo,
        documento,
        nombre_usuario,
        valor,
        fecha_pago
    FROM pagos
	where fecha_pago >= '2025-04-01'::DATE
		and fecha_pago < '2025-05-01'::DATE 
    ORDER BY id_registro, fecha_pago DESC, valor DESC
)
SELECT 
    pu.*,
    -- ¿Tuvo al menos 1 gestión antes del pago?
    CASE WHEN EXISTS (
        SELECT 1 
        FROM gestiones g 
        WHERE (pu.nro_comparendo = g.numero_comparendo OR pu.nro_acuerdo = g.numero_comparendo)
        AND pu.documento = g.documento
        AND g.fecha_gestion <= pu.fecha_pago
    ) THEN TRUE ELSE FALSE END AS tiene_gestion,
    
    -- ¿Tuvo al menos 1 SMS antes del pago?
    CASE WHEN EXISTS (
        SELECT 1 
        FROM sms s 
        WHERE (pu.nro_comparendo = s.numero_comparendo OR pu.nro_acuerdo = s.numero_comparendo)
        AND pu.documento = s.documento
        AND s.fecha_sms <= pu.fecha_pago
    ) THEN TRUE ELSE FALSE END AS tiene_sms,
    
    -- Fecha de la última gestión (si existe)
    (
        SELECT g.fecha_gestion
        FROM gestiones g
        WHERE (pu.nro_comparendo = g.numero_comparendo OR pu.nro_acuerdo = g.numero_comparendo)
        AND pu.documento = g.documento
        AND g.fecha_gestion <= pu.fecha_pago
        ORDER BY g.fecha_gestion DESC
        LIMIT 1
    ) AS ultima_gestion_fecha,
    
    -- Fecha del último SMS (si existe)
    (
        SELECT s.fecha_sms
        FROM sms s
        WHERE (pu.nro_comparendo = s.numero_comparendo OR pu.nro_acuerdo = s.numero_comparendo)
        AND pu.documento = s.documento
        AND s.fecha_sms <= pu.fecha_pago
        ORDER BY s.fecha_sms DESC
        LIMIT 1
    ) AS ultimo_sms_fecha
FROM pagos_unicos pu;

select * from mv_pagos_relacionados_2
;


SELECT
    'Pagos con al menos 1 gestión' AS tipo,
    SUM(valor) AS total_valor
FROM mv_pagos_relacionados_2
WHERE 
    tiene_gestion = TRUE
    AND fecha_pago >= '2025-04-01' 
    AND fecha_pago < '2025-05-01'

UNION ALL

SELECT
    'Pagos con al menos 1 SMS' AS tipo,
    SUM(valor) AS total_valor
FROM mv_pagos_relacionados_2
WHERE 
    tiene_sms = TRUE
    AND fecha_pago >= '2025-04-01' 
    AND fecha_pago < '2025-05-01'

UNION ALL

SELECT
    'Pagos con gestión Y SMS' AS tipo,
    SUM(valor) AS total_valor
FROM mv_pagos_relacionados_2
WHERE 
    (tiene_gestion = TRUE or
     tiene_sms = TRUE)
    AND fecha_pago >= '2025-04-01' 
    AND fecha_pago < '2025-05-01'

UNION ALL

SELECT
    'Pagos sin gestión ni SMS' AS tipo,
    SUM(valor) AS total_valor
FROM mv_pagos_relacionados_2
WHERE 
    tiene_gestion = FALSE
    AND tiene_sms = FALSE
    AND fecha_pago >= '2025-04-01' 
    AND fecha_pago < '2025-05-01';


SELECT 
    SUM(total_valor) AS suma_categorias,
    (SELECT SUM(valor) FROM mv_pagos_relacionados_2 
     WHERE fecha_pago >= '2025-04-01' 
    		AND fecha_pago < '2025-05-01') AS total_real
FROM (
    SELECT tipo_interaccion, SUM(valor) AS total_valor
    FROM (
        SELECT
            CASE
                WHEN tiene_gestion AND tiene_sms THEN 'Gestión Y SMS'
                WHEN tiene_gestion THEN 'Solo Gestión'
                WHEN tiene_sms THEN 'Solo SMS'
                ELSE 'Sin interacción'
            END AS tipo_interaccion,
            valor
        FROM mv_pagos_relacionados_2
        WHERE fecha_pago >= '2025-04-01' 
    		AND fecha_pago < '2025-05-01'
    ) t
    GROUP BY tipo_interaccion
) categorias;


SELECT
    CASE
        WHEN tiene_gestion = TRUE and tiene_sms = TRUE THEN 'Gestión Y SMS'
        WHEN tiene_gestion = TRUE THEN 'Solo Gestión'
        WHEN tiene_sms = TRUE THEN 'Solo SMS'
        ELSE 'Sin interacción'
    END AS tipo_interaccion,
    SUM(valor) AS total_valor,
    COUNT(*) AS cantidad_pagos
FROM mv_pagos_relacionados_2
WHERE fecha_pago >= '2025-04-01' 
    AND fecha_pago < '2025-05-01'
GROUP BY tipo_interaccion
ORDER BY total_valor DESC;

SELECT 
    'Pagos con al menos 1 gestión' AS tipo,
    SUM(valor) AS total_pagado,
    COUNT(*) AS cantidad_pagos,
    ROUND(AVG(valor), 2) AS promedio_pago
FROM mv_pagos_relacionados_2
WHERE 
    tiene_gestion = TRUE  -- Filtra solo pagos con gestión
    AND fecha_pago >= '2025-04-01' 
    AND fecha_pago < '2025-05-01';

SELECT 
    'Pagos con al menos 1 sms' AS tipo,
    SUM(valor) AS total_pagado,
    COUNT(*) AS cantidad_pagos,
    ROUND(AVG(valor), 2) AS promedio_pago
FROM mv_pagos_relacionados_2
WHERE 
    tiene_sms = TRUE  -- Filtra solo pagos con gestión
    AND fecha_pago >= '2025-04-01' 
    AND fecha_pago < '2025-05-01';



-- Suma de valores donde hubo gestión, SMS o ambos (Abril 2025)

SELECT 
    'Pagos con gestión y/o SMS' AS tipo,
    SUM(valor) AS total_pagado,
    COUNT(*) AS cantidad_pagos
FROM mv_pagos_relacionados_2
WHERE 
    (tiene_gestion = TRUE OR tiene_sms = TRUE)  -- Al menos uno de los dos
    AND fecha_pago >= '2025-04-01' 
    AND fecha_pago < '2025-05-01';

SELECT
    CASE
        WHEN tiene_gestion AND tiene_sms THEN 'Ambos (Gestión + SMS)'
        WHEN tiene_gestion THEN 'Solo Gestión'
        WHEN tiene_sms THEN 'Solo SMS'
    END AS tipo_interaccion,
    SUM(valor) AS total_pagado,
    COUNT(*) AS cantidad_pagos,
    ROUND(SUM(valor) * 100.0 / (
        SELECT SUM(valor) 
        FROM mv_pagos_relacionados_2 
        WHERE fecha_pago BETWEEN '2025-04-01' AND '2025-04-30'
    ), 2) AS porcentaje_total
FROM mv_pagos_relacionados_2
WHERE 
    (tiene_gestion = TRUE OR tiene_sms = TRUE)
    AND fecha_pago BETWEEN '2025-04-01' AND '2025-04-30'
GROUP BY tipo_interaccion
ORDER BY total_pagado DESC;


select * from mv_gestiones_comparendo;
