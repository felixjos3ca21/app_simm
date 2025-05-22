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