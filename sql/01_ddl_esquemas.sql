-- ==============================================================================
-- DDL COMPLETO DEL PROYECTO ETL ADUANAS DE CHILE
-- Base de Datos: PostgreSQL | Modelo Dimensional y Esquemas de Calidad
-- ==============================================================================

-- 1. CREACIÓN DE ESQUEMAS OBLIGATORIOS
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS mart;

-- 2. TABLA DE AUDITORÍA Y REGISTRO DE FALLAS (Esquema public)
CREATE TABLE IF NOT EXISTS public.etl_audit (
    id_audit SERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    run_id VARCHAR(150) NOT NULL,
    fuente VARCHAR(50),
    fecha_logical VARCHAR(50),
    fecha_inicio TIMESTAMP NOT NULL,
    fecha_fin TIMESTAMP NOT NULL,
    duracion_seg NUMERIC(10,2) NOT NULL,
    filas_leidas INT DEFAULT 0,
    filas_cargadas INT DEFAULT 0,
    porcentaje_nulos NUMERIC(6,2) DEFAULT 0.00,
    estado VARCHAR(30) NOT NULL, -- 'SUCCESS' o 'FAILED'
    mensaje_error TEXT
);

-- 3. CAPA RAW (Datos crudos tal como llegan de la API + metadatos de extracción)
CREATE TABLE IF NOT EXISTS raw.importaciones_raw (
    id_raw SERIAL PRIMARY KEY,
    run_id VARCHAR(150) NOT NULL,
    fecha_logical DATE,
    resource_id VARCHAR(100),
    offset_pagina INT,
    payload JSONB NOT NULL,
    fecha_extraccion TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.exportaciones_raw (
    id_raw SERIAL PRIMARY KEY,
    run_id VARCHAR(150) NOT NULL,
    fecha_logical DATE,
    resource_id VARCHAR(100),
    offset_pagina INT,
    payload JSONB NOT NULL,
    fecha_extraccion TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 4. CAPA STAGING (Datos limpios, tipados, normalizados y validados)
CREATE TABLE IF NOT EXISTS staging.stg_operaciones_aduanas (
    codigo_operacion VARCHAR(120) PRIMARY KEY,
    tipo_flujo VARCHAR(20) NOT NULL, -- 'IMPORTACION' o 'EXPORTACION'
    fecha_operacion DATE NOT NULL,
    anio INT NOT NULL,
    mes INT NOT NULL,
    codigo_aduana INT NOT NULL,
    nombre_aduana VARCHAR(120) NOT NULL,
    codigo_pais INT NOT NULL,
    nombre_pais VARCHAR(120) NOT NULL,
    codigo_via_transporte INT NOT NULL,
    nombre_via_transporte VARCHAR(100) NOT NULL,
    peso_bruto_kg NUMERIC(18,2) NOT NULL DEFAULT 0,
    cantidad_bultos NUMERIC(14,2) NOT NULL DEFAULT 0,
    monto_fob_usd NUMERIC(18,2) NOT NULL DEFAULT 0,
    monto_flete_usd NUMERIC(18,2) NOT NULL DEFAULT 0,
    monto_seguro_usd NUMERIC(18,2) NOT NULL DEFAULT 0,
    monto_cif_usd NUMERIC(18,2) NOT NULL DEFAULT 0,
    costo_logistico_usd NUMERIC(18,2) NOT NULL DEFAULT 0,
    run_id VARCHAR(150),
    fecha_procesamiento TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 5. CAPA DW (Modelo Dimensional Estrella: 4 Dimensiones + 1 Tabla de Hechos)

-- Dimensión Tiempo (Obligatoria por pauta con jerarquías temporales)
CREATE TABLE IF NOT EXISTS dw.dim_tiempo (
    id_tiempo INT PRIMARY KEY, -- Formato YYYYMMDD
    fecha DATE UNIQUE NOT NULL,
    anio INT NOT NULL,
    mes INT NOT NULL,
    nombre_mes VARCHAR(20) NOT NULL,
    trimestre INT NOT NULL,
    semestre INT NOT NULL
);

-- Dimensión Aduana (Punto de cruce 1 entre Importaciones y Exportaciones)
CREATE TABLE IF NOT EXISTS dw.dim_aduana (
    id_aduana SERIAL PRIMARY KEY,
    codigo_aduana INT UNIQUE NOT NULL,
    nombre_aduana VARCHAR(120) NOT NULL,
    zona_geografica VARCHAR(50) NOT NULL
);

-- Dimensión País de Origen/Destino (Punto de cruce 2)
CREATE TABLE IF NOT EXISTS dw.dim_pais (
    id_pais SERIAL PRIMARY KEY,
    codigo_pais INT UNIQUE NOT NULL,
    nombre_pais VARCHAR(120) NOT NULL,
    continente_region VARCHAR(80) NOT NULL
);

-- Dimensión Vía de Transporte (Punto de cruce 3)
CREATE TABLE IF NOT EXISTS dw.dim_via_transporte (
    id_via_transporte SERIAL PRIMARY KEY,
    codigo_via_transporte INT UNIQUE NOT NULL,
    nombre_via_transporte VARCHAR(100) NOT NULL
);

-- Tabla de Hechos de Comercio Exterior
CREATE TABLE IF NOT EXISTS dw.fact_comercio_exterior (
    id_hecho BIGSERIAL PRIMARY KEY,
    codigo_operacion VARCHAR(120) UNIQUE NOT NULL, -- Clave natural única para idempotencia estricta
    id_tiempo INT NOT NULL REFERENCES dw.dim_tiempo(id_tiempo),
    id_aduana INT NOT NULL REFERENCES dw.dim_aduana(id_aduana),
    id_pais INT NOT NULL REFERENCES dw.dim_pais(id_pais),
    id_via_transporte INT NOT NULL REFERENCES dw.dim_via_transporte(id_via_transporte),
    tipo_flujo VARCHAR(20) NOT NULL CHECK (tipo_flujo IN ('IMPORTACION', 'EXPORTACION')),
    peso_bruto_kg NUMERIC(18,2) NOT NULL DEFAULT 0,
    peso_toneladas NUMERIC(18,4) NOT NULL DEFAULT 0,
    cantidad_bultos NUMERIC(14,2) NOT NULL DEFAULT 0,
    monto_fob_usd NUMERIC(18,2) NOT NULL DEFAULT 0,
    monto_flete_usd NUMERIC(18,2) NOT NULL DEFAULT 0,
    monto_seguro_usd NUMERIC(18,2) NOT NULL DEFAULT 0,
    monto_cif_usd NUMERIC(18,2) NOT NULL DEFAULT 0,
    costo_logistico_usd NUMERIC(18,2) NOT NULL DEFAULT 0,
    fecha_carga_dw TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 6. CAPA MART (Tablas para los 5 Indicadores de Gestión con Documentación de Pauta)
CREATE TABLE IF NOT EXISTS mart.kpi_01_balanza_comercial_aduana (
    id SERIAL PRIMARY KEY,
    indicador VARCHAR(100) NOT NULL DEFAULT 'Balanza Comercial y Cobertura por Aduana',
    objetivo TEXT NOT NULL DEFAULT 'Medir el superávit o déficit comercial (FOB - CIF) por aduana para priorizar inversiones en infraestructura.',
    fuente VARCHAR(150) NOT NULL DEFAULT 'API datos.gob.cl - Aduanas (Importaciones y Exportaciones)',
    periodicidad VARCHAR(50) NOT NULL DEFAULT 'MENSUAL',
    formula TEXT NOT NULL DEFAULT 'Saldo USD = FOB Export - CIF Import | Cobertura % = (FOB / CIF) * 100',
    meta_umbral TEXT NOT NULL DEFAULT 'Tasa de Cobertura >= 100.0% (Superávit)',
    codigo_aduana INT NOT NULL,
    nombre_aduana VARCHAR(120) NOT NULL,
    anio INT NOT NULL,
    mes INT NOT NULL,
    exportaciones_fob_usd NUMERIC(18,2) DEFAULT 0,
    importaciones_cif_usd NUMERIC(18,2) DEFAULT 0,
    saldo_comercial_usd NUMERIC(18,2) DEFAULT 0,
    tasa_cobertura_pct NUMERIC(8,2) DEFAULT 0,
    cumple_meta BOOLEAN DEFAULT FALSE,
    fecha_calculo TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_kpi_01 UNIQUE (codigo_aduana, anio, mes)
);

CREATE TABLE IF NOT EXISTS mart.kpi_02_incidencia_costo_logistico (
    id SERIAL PRIMARY KEY,
    indicador VARCHAR(100) NOT NULL DEFAULT 'Incidencia del Costo Logístico sobre Valor CIF',
    objetivo TEXT NOT NULL DEFAULT 'Evaluar el sobrecosto de transporte y seguro sobre el valor CIF según modo de transporte y aduana de internación.',
    fuente VARCHAR(150) NOT NULL DEFAULT 'API datos.gob.cl - Aduanas (Importaciones DIN)',
    periodicidad VARCHAR(50) NOT NULL DEFAULT 'MENSUAL',
    formula TEXT NOT NULL DEFAULT 'Incidencia % = ((Flete USD + Seguro USD) / CIF USD) * 100',
    meta_umbral TEXT NOT NULL DEFAULT 'Marítima <= 10.0%, Aérea <= 18.0%, Otros <= 15.0%',
    codigo_via_transporte INT NOT NULL,
    nombre_via_transporte VARCHAR(100) NOT NULL,
    codigo_aduana INT NOT NULL,
    nombre_aduana VARCHAR(120) NOT NULL,
    monto_flete_usd NUMERIC(18,2) DEFAULT 0,
    monto_seguro_usd NUMERIC(18,2) DEFAULT 0,
    costo_logistico_usd NUMERIC(18,2) DEFAULT 0,
    monto_cif_usd NUMERIC(18,2) DEFAULT 0,
    pct_costo_logistico NUMERIC(8,2) DEFAULT 0,
    cumple_meta BOOLEAN DEFAULT FALSE,
    fecha_calculo TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_kpi_02 UNIQUE (codigo_via_transporte, codigo_aduana)
);

CREATE TABLE IF NOT EXISTS mart.kpi_03_concentracion_carga_aduanera (
    id SERIAL PRIMARY KEY,
    indicador VARCHAR(100) NOT NULL DEFAULT 'Índice de Concentración Logística por Aduana',
    objetivo TEXT NOT NULL DEFAULT 'Detectar cuellos de botella operacionales midiendo qué porcentaje del tonelaje nacional se concentra en cada aduana.',
    fuente VARCHAR(150) NOT NULL DEFAULT 'API datos.gob.cl - Aduanas (Importaciones y Exportaciones)',
    periodicidad VARCHAR(50) NOT NULL DEFAULT 'MENSUAL',
    formula TEXT NOT NULL DEFAULT 'Concentración % = (Toneladas Aduana / Toneladas Nacionales) * 100',
    meta_umbral TEXT NOT NULL DEFAULT 'Alerta si Concentración > 40.0% (Riesgo de cuello de botella)',
    codigo_aduana INT NOT NULL,
    nombre_aduana VARCHAR(120) NOT NULL,
    toneladas_aduana NUMERIC(18,4) DEFAULT 0,
    toneladas_nacionales NUMERIC(18,4) DEFAULT 0,
    pct_concentracion NUMERIC(8,2) DEFAULT 0,
    alerta_cuello_botella BOOLEAN DEFAULT FALSE,
    fecha_calculo TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_kpi_03 UNIQUE (codigo_aduana)
);

CREATE TABLE IF NOT EXISTS mart.kpi_04_valor_agregado_tonelada (
    id SERIAL PRIMARY KEY,
    indicador VARCHAR(100) NOT NULL DEFAULT 'Densidad de Valor Económico por Tonelada',
    objetivo TEXT NOT NULL DEFAULT 'Comparar el valor FOB por tonelada exportada para medir si Chile exporta materias primas o bienes de valor agregado.',
    fuente VARCHAR(150) NOT NULL DEFAULT 'API datos.gob.cl - Aduanas (Comercio Exterior)',
    periodicidad VARCHAR(50) NOT NULL DEFAULT 'MENSUAL',
    formula TEXT NOT NULL DEFAULT 'USD/Ton = Monto FOB USD / Peso Toneladas',
    meta_umbral TEXT NOT NULL DEFAULT 'Exportaciones >= 1.200 USD/Ton (Manufacturas de valor)',
    tipo_flujo VARCHAR(20) NOT NULL,
    codigo_pais INT NOT NULL,
    nombre_pais VARCHAR(120) NOT NULL,
    monto_fob_total_usd NUMERIC(18,2) DEFAULT 0,
    toneladas_totales NUMERIC(18,4) DEFAULT 0,
    valor_usd_por_tonelada NUMERIC(18,2) DEFAULT 0,
    cumple_meta BOOLEAN DEFAULT FALSE,
    fecha_calculo TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_kpi_04 UNIQUE (tipo_flujo, codigo_pais)
);

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
    CONSTRAINT uq_kpi_05_socio UNIQUE (codigo_pais)
);
