-- ==============================================================================
-- INDICADORES DE GESTIÓN (DATA MART) - ADUANAS DE CHILE
-- Cálculo idempotente de los 5 KPIs mediante SQL sobre el Data Warehouse (dw)
-- Incluye metadatos obligatorios de pauta: Indicador, Fuente, Periodicidad, Fórmula y Meta.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- KPI 1: Balanza Comercial y Cobertura por Aduana y Mes
-- ------------------------------------------------------------------------------
TRUNCATE TABLE mart.kpi_01_balanza_comercial_aduana;

INSERT INTO mart.kpi_01_balanza_comercial_aduana (
    indicador, objetivo, fuente, periodicidad, formula, meta_umbral,
    codigo_aduana, nombre_aduana, anio, mes,
    exportaciones_fob_usd, importaciones_cif_usd, saldo_comercial_usd,
    tasa_cobertura_pct, cumple_meta, fecha_calculo
)
SELECT
    'Balanza Comercial y Cobertura por Aduana' AS indicador,
    'Medir el superávit o déficit comercial (FOB - CIF) por aduana para priorizar inversiones en infraestructura.' AS objetivo,
    'API datos.gob.cl - Aduanas (Importaciones y Exportaciones)' AS fuente,
    'MENSUAL' AS periodicidad,
    'Saldo USD = FOB Export - CIF Import | Cobertura % = (FOB / CIF) * 100' AS formula,
    'Tasa de Cobertura >= 100.0% (Superávit comercial)' AS meta_umbral,
    a.codigo_aduana,
    a.nombre_aduana,
    t.anio,
    t.mes,
    COALESCE(SUM(CASE WHEN f.tipo_flujo = 'EXPORTACION' THEN f.monto_fob_usd ELSE 0 END), 0) AS exportaciones_fob_usd,
    COALESCE(SUM(CASE WHEN f.tipo_flujo = 'IMPORTACION' THEN f.monto_cif_usd ELSE 0 END), 0) AS importaciones_cif_usd,
    COALESCE(SUM(CASE WHEN f.tipo_flujo = 'EXPORTACION' THEN f.monto_fob_usd ELSE 0 END), 0) -
    COALESCE(SUM(CASE WHEN f.tipo_flujo = 'IMPORTACION' THEN f.monto_cif_usd ELSE 0 END), 0) AS saldo_comercial_usd,
    CASE
        WHEN COALESCE(SUM(CASE WHEN f.tipo_flujo = 'IMPORTACION' THEN f.monto_cif_usd ELSE 0 END), 0) > 0 THEN
            ROUND((SUM(CASE WHEN f.tipo_flujo = 'EXPORTACION' THEN f.monto_fob_usd ELSE 0 END) /
                   SUM(CASE WHEN f.tipo_flujo = 'IMPORTACION' THEN f.monto_cif_usd ELSE 0 END)) * 100.0, 2)
        ELSE 100.00
    END AS tasa_cobertura_pct,
    CASE
        WHEN (COALESCE(SUM(CASE WHEN f.tipo_flujo = 'EXPORTACION' THEN f.monto_fob_usd ELSE 0 END), 0) >=
              COALESCE(SUM(CASE WHEN f.tipo_flujo = 'IMPORTACION' THEN f.monto_cif_usd ELSE 0 END), 0))
        THEN TRUE ELSE FALSE
    END AS cumple_meta,
    CURRENT_TIMESTAMP
FROM dw.fact_comercio_exterior f
JOIN dw.dim_tiempo t ON t.id_tiempo = f.id_tiempo
JOIN dw.dim_aduana a ON a.id_aduana = f.id_aduana
GROUP BY a.codigo_aduana, a.nombre_aduana, t.anio, t.mes;

-- ------------------------------------------------------------------------------
-- KPI 2: Incidencia del Costo Logístico (% Flete + Seguro sobre CIF)
-- ------------------------------------------------------------------------------
TRUNCATE TABLE mart.kpi_02_incidencia_costo_logistico;

INSERT INTO mart.kpi_02_incidencia_costo_logistico (
    indicador, objetivo, fuente, periodicidad, formula, meta_umbral,
    codigo_via_transporte, nombre_via_transporte,
    codigo_aduana, nombre_aduana,
    monto_flete_usd, monto_seguro_usd, costo_logistico_usd,
    monto_cif_usd, pct_costo_logistico, cumple_meta, fecha_calculo
)
SELECT
    'Incidencia del Costo Logístico sobre Valor CIF' AS indicador,
    'Evaluar el sobrecosto de transporte y seguro sobre el valor CIF según modo de transporte y aduana de internación.' AS objetivo,
    'API datos.gob.cl - Aduanas (Importaciones DIN)' AS fuente,
    'MENSUAL' AS periodicidad,
    'Incidencia % = ((Flete USD + Seguro USD) / CIF USD) * 100' AS formula,
    'Marítima <= 10.0%, Aérea <= 18.0%, Otros <= 15.0%' AS meta_umbral,
    v.codigo_via_transporte,
    v.nombre_via_transporte,
    a.codigo_aduana,
    a.nombre_aduana,
    COALESCE(SUM(f.monto_flete_usd), 0) AS monto_flete_usd,
    COALESCE(SUM(f.monto_seguro_usd), 0) AS monto_seguro_usd,
    COALESCE(SUM(f.costo_logistico_usd), 0) AS costo_logistico_usd,
    COALESCE(SUM(f.monto_cif_usd), 0) AS monto_cif_usd,
    CASE
        WHEN COALESCE(SUM(f.monto_cif_usd), 0) > 0 THEN
            ROUND((SUM(f.costo_logistico_usd) / SUM(f.monto_cif_usd)) * 100.0, 2)
        ELSE 0.00
    END AS pct_costo_logistico,
    CASE
        WHEN v.codigo_via_transporte = 1 AND
             (COALESCE(SUM(f.costo_logistico_usd), 0) / NULLIF(SUM(f.monto_cif_usd), 0)) * 100.0 <= 10.0 THEN TRUE
        WHEN v.codigo_via_transporte = 4 AND
             (COALESCE(SUM(f.costo_logistico_usd), 0) / NULLIF(SUM(f.monto_cif_usd), 0)) * 100.0 <= 18.0 THEN TRUE
        WHEN v.codigo_via_transporte NOT IN (1, 4) AND
             (COALESCE(SUM(f.costo_logistico_usd), 0) / NULLIF(SUM(f.monto_cif_usd), 0)) * 100.0 <= 15.0 THEN TRUE
        ELSE FALSE
    END AS cumple_meta,
    CURRENT_TIMESTAMP
FROM dw.fact_comercio_exterior f
JOIN dw.dim_via_transporte v ON v.id_via_transporte = f.id_via_transporte
JOIN dw.dim_aduana a ON a.id_aduana = f.id_aduana
WHERE f.tipo_flujo = 'IMPORTACION'
GROUP BY v.codigo_via_transporte, v.nombre_via_transporte, a.codigo_aduana, a.nombre_aduana;

-- ------------------------------------------------------------------------------
-- KPI 3: Concentración de Carga Aduanera (% de Toneladas por Aduana)
-- ------------------------------------------------------------------------------
TRUNCATE TABLE mart.kpi_03_concentracion_carga_aduanera;

WITH total_nacional AS (
    SELECT COALESCE(SUM(peso_toneladas), 0) AS total_toneladas
    FROM dw.fact_comercio_exterior
)
INSERT INTO mart.kpi_03_concentracion_carga_aduanera (
    indicador, objetivo, fuente, periodicidad, formula, meta_umbral,
    codigo_aduana, nombre_aduana, toneladas_aduana,
    toneladas_nacionales, pct_concentracion, alerta_cuello_botella, fecha_calculo
)
SELECT
    'Índice de Concentración Logística por Aduana' AS indicador,
    'Detectar cuellos de botella operacionales midiendo qué porcentaje del tonelaje nacional se concentra en cada aduana.' AS objetivo,
    'API datos.gob.cl - Aduanas (Importaciones y Exportaciones)' AS fuente,
    'MENSUAL' AS periodicidad,
    'Concentración % = (Toneladas Aduana / Toneladas Nacionales) * 100' AS formula,
    'Alerta si Concentración > 40.0% (Riesgo de cuello de botella)' AS meta_umbral,
    a.codigo_aduana,
    a.nombre_aduana,
    COALESCE(SUM(f.peso_toneladas), 0) AS toneladas_aduana,
    tn.total_toneladas AS toneladas_nacionales,
    CASE
        WHEN tn.total_toneladas > 0 THEN
            ROUND((SUM(f.peso_toneladas) / tn.total_toneladas) * 100.0, 2)
        ELSE 0.00
    END AS pct_concentracion,
    CASE
        WHEN tn.total_toneladas > 0 AND (SUM(f.peso_toneladas) / tn.total_toneladas) * 100.0 > 40.0 THEN TRUE
        ELSE FALSE
    END AS alerta_cuello_botella,
    CURRENT_TIMESTAMP
FROM dw.fact_comercio_exterior f
JOIN dw.dim_aduana a ON a.id_aduana = f.id_aduana
CROSS JOIN total_nacional tn
GROUP BY a.codigo_aduana, a.nombre_aduana, tn.total_toneladas;

-- ------------------------------------------------------------------------------
-- KPI 4: Valor Agregado por Tonelada (USD/Ton) por Flujo y País
-- ------------------------------------------------------------------------------
TRUNCATE TABLE mart.kpi_04_valor_agregado_tonelada;

INSERT INTO mart.kpi_04_valor_agregado_tonelada (
    indicador, objetivo, fuente, periodicidad, formula, meta_umbral,
    tipo_flujo, codigo_pais, nombre_pais,
    monto_fob_total_usd, toneladas_totales,
    valor_usd_por_tonelada, cumple_meta, fecha_calculo
)
SELECT
    'Densidad de Valor Económico por Tonelada' AS indicador,
    'Comparar el valor FOB por tonelada exportada para medir si Chile exporta materias primas o bienes de valor agregado.' AS objetivo,
    'API datos.gob.cl - Aduanas (Comercio Exterior)' AS fuente,
    'MENSUAL' AS periodicidad,
    'USD/Ton = Monto FOB USD / Peso Toneladas' AS formula,
    'Exportaciones >= 1.200 USD/Ton (Manufacturas de valor)' AS meta_umbral,
    f.tipo_flujo,
    p.codigo_pais,
    p.nombre_pais,
    COALESCE(SUM(f.monto_fob_usd), 0) AS monto_fob_total_usd,
    COALESCE(SUM(f.peso_toneladas), 0) AS toneladas_totales,
    CASE
        WHEN COALESCE(SUM(f.peso_toneladas), 0) > 0 THEN
            ROUND((SUM(f.monto_fob_usd) / SUM(f.peso_toneladas)), 2)
        ELSE 0.00
    END AS valor_usd_por_tonelada,
    CASE
        WHEN f.tipo_flujo = 'EXPORTACION' AND
             COALESCE(SUM(f.peso_toneladas), 0) > 0 AND
             (SUM(f.monto_fob_usd) / SUM(f.peso_toneladas)) >= 1200.0 THEN TRUE
        WHEN f.tipo_flujo = 'IMPORTACION' THEN TRUE
        ELSE FALSE
    END AS cumple_meta,
    CURRENT_TIMESTAMP
FROM dw.fact_comercio_exterior f
JOIN dw.dim_pais p ON p.id_pais = f.id_pais
GROUP BY f.tipo_flujo, p.codigo_pais, p.nombre_pais;

-- ------------------------------------------------------------------------------
-- KPI 5: Participación y Dependencia por Socio Comercial
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart.kpi_05_participacion_socios_comerciales (
    id SERIAL PRIMARY KEY,
    indicador VARCHAR(100) NOT NULL DEFAULT 'Participación y Dependencia por Socio Comercial',
    objetivo TEXT NOT NULL DEFAULT 'Evaluar el grado de diversificación del comercio exterior chileno y alertar sobreexposición económica con socios específicos.',
    fuente VARCHAR(150) NOT NULL DEFAULT 'API datos.gob.cl - Aduanas (Importaciones y Exportaciones 2026)',
    periodicidad VARCHAR(50) NOT NULL DEFAULT 'MENSUAL',
    formula TEXT NOT NULL DEFAULT 'Participación % = ((FOB Export + CIF Import País) / Intercambio Total Nacional) * 100',
    meta_umbral TEXT NOT NULL DEFAULT 'Ningún socio comercial debe superar el 35.0% de participación (Alerta de Sobreexposición)',
    codigo_pais INT NOT NULL,
    nombre_pais VARCHAR(120) NOT NULL,
    exportaciones_fob_usd NUMERIC(18,2) DEFAULT 0,
    importaciones_cif_usd NUMERIC(18,2) DEFAULT 0,
    intercambio_total_usd NUMERIC(18,2) DEFAULT 0,
    intercambio_nacional_usd NUMERIC(18,2) DEFAULT 0,
    pct_participacion NUMERIC(8,2) DEFAULT 0,
    alerta_sobreexposicion BOOLEAN DEFAULT FALSE,
    cumple_meta BOOLEAN DEFAULT TRUE,
    fecha_calculo TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_kpi_05 UNIQUE (codigo_pais)
);

TRUNCATE TABLE mart.kpi_05_participacion_socios_comerciales;

WITH totales_pais AS (
    SELECT
        p.codigo_pais,
        p.nombre_pais,
        COALESCE(SUM(CASE WHEN f.tipo_flujo = 'EXPORTACION' THEN f.monto_fob_usd ELSE 0 END), 0) AS exportaciones_fob_usd,
        COALESCE(SUM(CASE WHEN f.tipo_flujo = 'IMPORTACION' THEN f.monto_cif_usd ELSE 0 END), 0) AS importaciones_cif_usd,
        COALESCE(SUM(CASE WHEN f.tipo_flujo = 'EXPORTACION' THEN f.monto_fob_usd ELSE f.monto_cif_usd END), 0) AS intercambio_total_usd
    FROM dw.fact_comercio_exterior f
    JOIN dw.dim_pais p ON p.id_pais = f.id_pais
    GROUP BY p.codigo_pais, p.nombre_pais
),
total_nacional AS (
    SELECT COALESCE(SUM(intercambio_total_usd), 0) AS intercambio_nacional_usd
    FROM totales_pais
)
INSERT INTO mart.kpi_05_participacion_socios_comerciales (
    indicador, objetivo, fuente, periodicidad, formula, meta_umbral,
    codigo_pais, nombre_pais,
    exportaciones_fob_usd, importaciones_cif_usd,
    intercambio_total_usd, intercambio_nacional_usd,
    pct_participacion, alerta_sobreexposicion, cumple_meta, fecha_calculo
)
SELECT
    'Participación y Dependencia por Socio Comercial' AS indicador,
    'Evaluar el grado de diversificación del comercio exterior chileno y alertar sobreexposición económica con socios específicos.' AS objetivo,
    'API datos.gob.cl - Aduanas (Importaciones y Exportaciones 2026)' AS fuente,
    'MENSUAL' AS periodicidad,
    'Participación % = ((FOB Export + CIF Import País) / Intercambio Total Nacional) * 100' AS formula,
    'Ningún socio comercial debe superar el 35.0% de participación (Alerta de Sobreexposición)' AS meta_umbral,
    tp.codigo_pais,
    tp.nombre_pais,
    tp.exportaciones_fob_usd,
    tp.importaciones_cif_usd,
    tp.intercambio_total_usd,
    tn.intercambio_nacional_usd,
    CASE
        WHEN tn.intercambio_nacional_usd > 0 THEN
            ROUND((tp.intercambio_total_usd / tn.intercambio_nacional_usd) * 100.0, 2)
        ELSE 0.00
    END AS pct_participacion,
    CASE
        WHEN tn.intercambio_nacional_usd > 0 AND
             (tp.intercambio_total_usd / tn.intercambio_nacional_usd) * 100.0 > 35.0 THEN TRUE
        ELSE FALSE
    END AS alerta_sobreexposicion,
    CASE
        WHEN tn.intercambio_nacional_usd > 0 AND
             (tp.intercambio_total_usd / tn.intercambio_nacional_usd) * 100.0 <= 35.0 THEN TRUE
        ELSE FALSE
    END AS cumple_meta,
    CURRENT_TIMESTAMP
FROM totales_pais tp
CROSS JOIN total_nacional tn
ORDER BY tp.intercambio_total_usd DESC;
