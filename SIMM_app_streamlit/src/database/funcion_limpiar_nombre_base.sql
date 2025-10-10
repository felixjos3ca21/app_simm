-- Crear la función de limpieza
CREATE OR REPLACE FUNCTION limpiar_base(valor_base TEXT)
RETURNS TEXT AS $$
BEGIN
    -- Manejar valores NULL
    IF valor_base IS NULL THEN
        RETURN NULL;
    END IF;
    
    -- Convertir a mayúsculas y limpiar espacios
    valor_base := UPPER(TRIM(valor_base));
    
    -- Coincidencias EXACTAS y literales
    RETURN CASE 
        -- ============ 25% DCTO ============
        WHEN valor_base = 'CAMPA?A DCTO 25%' THEN '25% DCTO'
        WHEN valor_base = 'CAMPA?A DTO 25%' THEN '25% DCTO'
        WHEN valor_base = 'CTE DCTO 25%' THEN '25% DCTO'
        WHEN valor_base LIKE 'DCTO 25% HASTA EL%' THEN '25% DCTO'
        WHEN valor_base = 'CAMPAÑA DTO_25%' THEN '25% DCTO'
        WHEN valor_base = 'DCTO 25%_HASTA EL 28 FEB' THEN '25% DCTO'
        WHEN valor_base = 'DCTO 25%_HASTA EL 07 MARZO' THEN '25% DCTO'
        WHEN valor_base = 'CAMPAÑA DCTO 25%' THEN '25% DCTO'
        WHEN valor_base = 'DCTO 25%_HASTA EL 26 FEB' THEN '25% DCTO'
        WHEN valor_base = 'DCTO 25%_HASTA EL 05 MARZO' THEN '25% DCTO'
        
        -- ============ 50% DCTO ============
        WHEN valor_base = '50% DCTO' THEN '50% DCTO'
        WHEN valor_base = '50%DCTO' THEN '50% DCTO'
        WHEN valor_base = 'DCTO 50%' THEN '50% DCTO'
		WHEN valor_base = 'CAMPAÑA 50%DCTO' THEN '50% DCTO'
        
        -- ============ 70-30 ============
        WHEN valor_base = '70-30' THEN '70-30'
        
        -- ============ AP VIGENTES ============
        WHEN valor_base = 'AP VIGENTES' THEN 'AP VIGENTES'
        WHEN valor_base = 'AP VIGENTES CUOTAS VENCIDAS' THEN 'AP VIGENTES'
        WHEN valor_base = 'AP VIGENTES CUOTAS VIGENTES' THEN 'AP VIGENTES'
        WHEN valor_base = 'AP VIGENTE' THEN 'AP VIGENTES'
        WHEN valor_base = 'AP_VIGENTE' THEN 'AP VIGENTES'
        
        -- ============ APIS ============
        WHEN valor_base = 'APIS' THEN 'APIS'
        WHEN valor_base = 'APIS CONTRACTUALES' THEN 'APIS'
        WHEN valor_base = 'APIS 2021 AL 2024' THEN 'APIS'
        WHEN valor_base = 'APIS NOVIEMBRE' THEN 'APIS'
        WHEN valor_base = 'APIS - CONTRACTUALES' THEN 'APIS'
        
        -- ============ AVISO ============
        WHEN valor_base = 'AVISO AGOSTO' THEN 'AVISO'
		WHEN valor_base = 'AVISO PREVIO JUNIO' THEN 'AVISO'
        WHEN valor_base = 'AVISO DCTO 25%' THEN 'AVISO'
        WHEN valor_base = 'AVISO DICIEMBRE 50%' THEN 'AVISO'
        WHEN valor_base = 'AVISO ENERO 25%' THEN 'AVISO'
        WHEN valor_base = 'AVISO FEBRERO' THEN 'AVISO'
        WHEN valor_base = 'AVISO FEBRERO PARTE 2' THEN 'AVISO'
        WHEN valor_base = 'AVISO MARZO' THEN 'AVISO'
        WHEN valor_base = 'AVISO NOVIEMBRE PREVIO' THEN 'AVISO'
        WHEN valor_base = 'AVISO OCTUBRE' THEN 'AVISO'
        WHEN valor_base = 'AVISO PREVIO C32, C26, D04' THEN 'AVISO'
        WHEN valor_base = 'AVISO PREVIO DICIEMBRE' THEN 'AVISO'
        WHEN valor_base = 'AVISO PREVIO FEBRERO C26 - C32 - D04' THEN 'AVISO'
        WHEN valor_base = 'AVISO PREVIO MARZO C26' THEN 'AVISO'
        WHEN valor_base = 'AVISO PREVIO MAYO' THEN 'AVISO'
        WHEN valor_base = 'AVISO RESTANTES' THEN 'AVISO'
        WHEN valor_base = 'AVISO SEP' THEN 'AVISO'
        WHEN valor_base = 'AVISO SEPTIEMBRE' THEN 'AVISO'
        WHEN valor_base = 'AVISO SIN DATOS' THEN 'AVISO'
        WHEN valor_base = 'C14 Aviso previo Enero' THEN 'AVISO'
        WHEN valor_base = 'C14 AVISO PREVIO FEBRERO' THEN 'AVISO'
        WHEN valor_base = 'C26 - C29 Aviso previo Enero' THEN 'AVISO'
        WHEN valor_base = 'C32 - C35 Aviso previo Enero' THEN 'AVISO'
        WHEN valor_base = 'Deudor API para notificar por aviso' THEN 'AVISO'
        WHEN valor_base = 'Deudor notificado por Aviso' THEN 'AVISO'
        WHEN valor_base = 'Deudor para notificar por aviso' THEN 'AVISO'
        WHEN valor_base = 'PREVIO DICIEMBRE' THEN 'AVISO'
        WHEN valor_base = 'REGISTROS DE PREVIO FEBRERO' THEN 'AVISO'
        WHEN valor_base = 'SOAT PREVIO AVISO' THEN 'AVISO'
        WHEN valor_base = 'AVISO PREVIO ABRIL' THEN 'AVISO'
        WHEN valor_base = 'C32 - C35 AVISO PREVIO ENERO' THEN 'AVISO'
        WHEN valor_base = 'C26 - C29 AVISO PREVIO ENERO' THEN 'AVISO'
        WHEN valor_base = 'C14 AVISO PREVIO ENERO' THEN 'AVISO'
        WHEN valor_base = 'AVISO PREVIO FEBRERO C29' THEN 'AVISO'
        WHEN valor_base = 'PREVIO DICIEMBRE C14' THEN 'AVISO'
        WHEN valor_base = 'AVISO PREVIO C32_C26_D04' THEN 'AVISO'
        WHEN valor_base = 'DEUDOR PARA NOTIFICAR POR AVISO' THEN 'AVISO'
        WHEN valor_base = 'DEUDOR API PARA NOTIFICAR POR AVISO' THEN 'AVISO'
        WHEN valor_base = 'DEUDOR NOTIFICADO POR AVISO' THEN 'AVISO'
        
        -- ============ BD MOROSOS ============
        WHEN valor_base = 'BD MOROSOS' THEN 'BD MOROSOS'
        WHEN valor_base = 'CARTERA MENORES' THEN 'BD MOROSOS'
        WHEN valor_base = 'CARTERA MOROSA' THEN 'BD MOROSOS'
        WHEN valor_base = 'MOROSOS 2019 A 2023' THEN 'BD MOROSOS'
        WHEN valor_base = 'MOROSOS N1 2DA PARTE' THEN 'BD MOROSOS'
		WHEN valor_base = 'MOROSOS N2 2021 A 2025' THEN 'BD MOROSOS'
        WHEN valor_base = 'MOROSOS 2022 A 2024' THEN 'BD MOROSOS'
        WHEN valor_base = 'MOROSOS 2024 - 2025' THEN 'BD MOROSOS'
        WHEN valor_base = 'MOROSOS 2023 A 2025' THEN 'BD MOROSOS'
		WHEN valor_base = 'MOROSOS N1 2DA PARTE' THEN 'BD MOROSOS'
        WHEN valor_base = 'MOROSOS 2012 AL 2014' THEN 'BD MOROSOS'
        WHEN valor_base = 'MOROSOS URGENTE' THEN 'BD MOROSOS'
        WHEN valor_base = 'NUEVA CAMPA?A SEGUNDO BARRIDOS DE CON' THEN 'BD MOROSOS'
        WHEN valor_base = 'NUEVOS MOROSOS' THEN 'BD MOROSOS'
		WHEN valor_base = 'NUEVOS MOROSOS COMPRA DE DATOS' THEN 'BD MOROSOS'
        WHEN valor_base = 'NUEVOS MOROSOS COACTIVOS' THEN 'BD MOROSOS'
        WHEN valor_base = 'RECIEN CONFORMADOS (NUEVOS MOROSOS)' THEN 'BD MOROSOS'
        
        -- ============ CADUCIDADES ============
        WHEN valor_base = 'CADUCIDADES' THEN 'CADUCIDADES'
        
        -- ============ CONTACTOS ============
        WHEN valor_base = 'Contactos_29042021503' THEN 'CONTACTOS'
        WHEN valor_base = 'CONTACTOS_290420251503' THEN 'CONTACTOS'
        
        -- ============ CORRESPONDENCIA ============
        WHEN valor_base = 'CORRESPONDENCIA' THEN 'CORRESPONDENCIA'
        WHEN valor_base = 'CORREPONDENCIA 2DA PARTE' THEN 'CORRESPONDENCIA'
        
        -- ============ CORRIENTE SIN DCTO ============
        WHEN valor_base = 'Cartera corriente ID - Febrero' THEN 'CORRIENTE SIN DCTO'
        WHEN valor_base = 'CORRIENTE SERVICIO PUBLICO' THEN 'CORRIENTE SIN DCTO'
		WHEN valor_base = 'CTE JULIO' THEN 'CORRIENTE SIN DCTO'
        WHEN valor_base = 'CTE TOTAL' THEN 'CORRIENTE SIN DCTO'
        WHEN valor_base = 'CTE SIN DCTO' THEN 'CORRIENTE SIN DCTO'
        WHEN valor_base = 'CTE SIN DTO' THEN 'CORRIENTE SIN DCTO'
        WHEN valor_base = 'CTE SIN DCTO CORRESPONDENCIA' THEN 'CORRIENTE SIN DCTO'
        WHEN valor_base = 'CTE SIN DCTO N2' THEN 'CORRIENTE SIN DCTO'
        WHEN valor_base = 'CORRIENTE A DIC 2024' THEN 'CORRIENTE SIN DCTO'
        WHEN valor_base = 'CARTERA CORRIENTE ID_FEBRERO' THEN 'CORRIENTE SIN DCTO'
        WHEN valor_base = 'CTE SN DCTO CORRESPONDENCIA' THEN 'CORRIENTE SIN DCTO'
        
        -- ============ DEVOLUCIONES ============
        WHEN valor_base = 'DEVOLUCIONES' THEN 'DEVOLUCIONES'
        WHEN valor_base = 'DEVOLUCIONES DIC 2024 ENERO Y FEBRERO 2025' THEN 'DEVOLUCIONES'
        WHEN valor_base = 'FD CORRIENTE SIN_DIC' THEN 'DEVOLUCIONES'
        WHEN valor_base = 'GUIAS DEVUELTAS' THEN 'DEVOLUCIONES'
        WHEN valor_base = 'GUIAS DEVUELTAS OCTUBRE' THEN 'DEVOLUCIONES'
        WHEN valor_base = 'FD DEVUELTAS NOV_DIC' THEN 'DEVOLUCIONES'
        
        -- ============ FD EN ESTUDIO ============
        WHEN valor_base = 'FD EN ESTUDIO' THEN 'FD EN ESTUDIO'
        WHEN valor_base = 'FD EN ESTUDIO SIN DATO CAMIONES Y CAMP' THEN 'FD EN ESTUDIO'
        WHEN valor_base = 'FD EN ESTUDIO SIN DATO CONTACTO NOV DIC' THEN 'FD EN ESTUDIO'
        WHEN valor_base = 'FD ESTUDIO SIN DATOS SEPTIEMBRE' THEN 'FD EN ESTUDIO'
        WHEN valor_base = 'FD EN ESTUDIO_Enero_una sola FD' THEN 'FD EN ESTUDIO'
        WHEN valor_base = 'FD ESTUDIO_ENERO_UNA SOLA FD' THEN 'FD EN ESTUDIO'
        WHEN valor_base = 'FD EN ESTUDIO ENERO 2025 CAMIONETA Y CAMPERO' THEN 'FD EN ESTUDIO'
        
        -- ============ IMPOSICION DIARIA ============
        WHEN valor_base = 'IMPOSICION DIARIA' THEN 'IMPOSICION DIARIA'
        WHEN valor_base = 'IS ENERO SIN REGISTRO' THEN 'IMPOSICION DIARIA'
        WHEN valor_base = 'IS MARZO SIN REGISTRO' THEN 'IMPOSICION DIARIA'
        WHEN valor_base = 'IS SEGUNDO BARRIDO' THEN 'IMPOSICION DIARIA'
        WHEN valor_base = 'IMPOSICION DIARI' THEN 'IMPOSICION DIARIA'
		WHEN valor_base = 'COMPRA DATOS IMPOSICION' THEN 'IMPOSICION DIARIA'
        WHEN valor_base = 'IMPOSICION DIARIO' THEN 'IMPOSICION DIARIA'
        WHEN valor_base = 'IMPOSICIO''N DIARIA' THEN 'IMPOSICION DIARIA'
        WHEN valor_base = 'ID SEGUNDO BARRIDO' THEN 'IMPOSICION DIARIA'
        WHEN valor_base = 'ID' THEN 'IMPOSICION DIARIA'
        WHEN valor_base = 'ID ENERO SIN REGISTRO' THEN 'IMPOSICION DIARIA'
        WHEN valor_base = 'IMPOSICION DIARA' THEN 'IMPOSICION DIARIA'
        WHEN valor_base = 'IMPOSICIÓN DIARIA' THEN 'IMPOSICION DIARIA'
        WHEN valor_base = 'ID MARZO SIN REGISTRO' THEN 'IMPOSICION DIARIA'
        
        -- ============ INFRACCIONES VEHICULOS ============
        WHEN valor_base = 'INFRACCIONES VEHICULOS ENTREGADOS' THEN 'INFRACCIONES VEHICULOS ENTREGADOS'
        WHEN valor_base = 'Reporte de ingresos y retiros de los vehiculos' THEN 'INFRACCIONES VEHICULOS ENTREGADOS'
        WHEN valor_base = 'RETENIDOS Y ENTREGA DE VEHICULOS' THEN 'INFRACCIONES VEHICULOS ENTREGADOS'
        WHEN valor_base = 'INFRACCIONES_VEHICULOS ENTREGADOS' THEN 'INFRACCIONES VEHICULOS ENTREGADOS'
        WHEN valor_base = 'REPORTE DE INGRESOS Y RETIROS DE LOS VEHICULOS' THEN 'INFRACCIONES VEHICULOS ENTREGADOS'
        WHEN valor_base = 'RETENCION Y ENTREGA DE VEHICULOS' THEN 'INFRACCIONES VEHICULOS ENTREGADOS'
        WHEN valor_base = 'VEHÍCULOS ENTREGADOS SIN PAGO DE COMPARENDO' THEN 'INFRACCIONES VEHICULOS ENTREGADOS'
        
        -- ============ NO GESTIONADO ============
        WHEN valor_base = 'NO GESTIONADO' THEN 'NO GESTIONADO'
        
        -- ============ NOTIFICACION ============
        WHEN valor_base = 'CONSTANCIA NOTIFICACION AVISO APIS' THEN 'AVISO'
        WHEN valor_base = 'NOT CORRESPONDENCIA' THEN 'NOTIFICACION'
        WHEN valor_base = 'NOTIFICADA SIN OCTUBRE' THEN 'NOTIFICACION'
        WHEN valor_base = 'NOTIFICADOS AVISO OCTUBRE ZDA' THEN 'NOTIFICACION'
        WHEN valor_base = 'NOTIFICADOS AVISO OCTUBRE' THEN 'NOTIFICACION'
        WHEN valor_base = 'NOTIFICADOS AVISO OCTUBRE 2DA' THEN 'NOTIFICACION'
        WHEN valor_base = 'NOTIFICACION' THEN 'NOTIFICACION'
        
        -- ============ USUARIOS LOCALIZADOS ============
        WHEN valor_base = 'USUARIOS LOCALIZADOS' THEN 'USUARIOS ILOCALIZADOS'
        WHEN valor_base = 'USUARIOS SIN CONTACTO FEBRERO FD' THEN 'USUARIOS ILOCALIZADOS'
        WHEN valor_base = 'USUARIOS ILOCALIZADOS' THEN 'USUARIOS ILOCALIZADOS'
        WHEN valor_base = 'NUEVA CAMPAÑA USUARIOS SIN DATOS DE CONTACTO' THEN 'USUARIOS ILOCALIZADOS'
        
        -- ============ VEHICULOS RETENIDOS ============
        WHEN valor_base = 'CARTERA VEHICULOS RETENIDOS' THEN 'VEHICULOS RETENIDOS'
        WHEN valor_base = 'VEHICULOS RETENIDOS' THEN 'VEHICULOS RETENIDOS'
		WHEN valor_base = 'CAMPAÑA VEHICULOS RETIRADOS' THEN 'VEHICULOS RETENIDOS'
        
        -- Valor por defecto (sin cambios)
        ELSE valor_base
    END;
END;
$$ LANGUAGE plpgsql;
-------------------//--------------------------------

-- Agregar la nueva columna
ALTER TABLE bases ADD COLUMN IF NOT EXISTS base_cleaned TEXT;

-- Actualizar todos los registros existentes
UPDATE bases 
SET base_cleaned = limpiar_base(base);

-- Crear índice para mejor rendimiento
CREATE INDEX IF NOT EXISTS idx_bases_cleaned ON bases(base_cleaned);

SELECT Base_cleaned, count(*) as cantidad
from bases
GROUP by Base_cleaned
ORDER by cantidad DESC

SELECT base, count(*) as cantidad
from bases
where	bases.fecha_entrega >= '2025-10-01'
	and bases.fecha_entrega <= '2025-10-09'
GROUP by base
ORDER by cantidad DESC

SELECT base, base_cleaned from bases

