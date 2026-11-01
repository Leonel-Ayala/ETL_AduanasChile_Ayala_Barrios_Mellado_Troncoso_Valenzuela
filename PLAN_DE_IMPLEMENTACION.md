# PLAN DE IMPLEMENTACIÓN: PROYECTO ETL ADUANAS DE CHILE (IMPORTACIONES Y EXPORTACIONES)
**Asignatura:** INF-522 | **Motor BD:** PostgreSQL | **Orquestador:** Apache Airflow (Docker) | **Lenguaje:** Python[cite: 1]

---

## 1. CONTEXTO E INSTRUCCIÓN PARA EL AGENTE DE ANTIGRAVITY IDE

Actualmente, este workspace contiene un proyecto de ejemplo en Docker con Apache Airflow que extrae datos desde un archivo CSV local. Tu misión como Agente de Ingeniería de Datos es **refactorizar y construir desde cero la arquitectura completa del proyecto** para que consuma datos exclusivamente mediante peticiones HTTP a la **API pública de Datos Abiertos de Chile (`datos.gob.cl`) del Servicio Nacional de Aduanas**[cite: 1, 3], utilizando dos endpoints/recursos independientes:
1. **Endpoint 1:** Operaciones de **Importaciones** de Aduanas[cite: 3].
2. **Endpoint 2:** Operaciones de **Exportaciones** de Aduanas[cite: 3].

Todo el flujo debe extraer los datos hacia una capa cruda (`raw`), depurarlos hacia una capa limpia (`staging`), poblar un modelo dimensional en estrella (`dw`) de forma idempotente y calcular indicadores de gestión en SQL (`mart`), registrando la auditoría y las fallas en `etl_audit`, todo orquestado mediante 3 DAGs independientes en Apache Airflow[cite: 3, 6, 7].

---

## 2. REGLAS ESTRICTAS DE EVALUACIÓN (CUMPLIMIENTO OBLIGATORIO)

Para obtener el puntaje máximo en todos los criterios, el código que generes debe respetar estas restricciones sin excepción:
1. **Prohibido usar archivos locales o descargas manuales:** Elimina toda lectura de archivos `.csv` o `.xlsx` locales; los datos deben ingresar únicamente vía API con `requests` en Python[cite: 1, 3, 6].
2. **Cero contraseñas, tokens o URLs fijas en el código:** Ningún script `.py` ni archivo entregado puede contener contraseñas ni claves expuestas[cite: 1, 7]. Todas las credenciales de base de datos deben usar Airflow `Connections` (`postgres_dw`) y los endpoints/parámetros deben leerse desde Airflow `Variables`[cite: 7].
3. **Al menos 3 DAGs desacoplados:**
   - `dag_01_etl_importaciones`: Procesa el endpoint de Importaciones[cite: 3, 7].
   - `dag_02_etl_exportaciones`: Procesa el endpoint de Exportaciones[cite: 3, 7].
   - `dag_03_indicadores_mart`: Se dispara automáticamente al terminar `dag_01` o `dag_02` y calcula los KPIs en SQL[cite: 3, 7].
   - **Aislamiento de fallas:** Si `dag_01` falla, `dag_02` y `dag_03` deben poder ejecutarse sin detener el resto del proceso[cite: 2, 7].
4. **Extracción robusta:** Debe implementar tarea de validación previa de la API, manejo de autenticación (headers/token si se configura en Variables), paginación por bloques (`limit` y `offset`), control de límites de uso (`rate limit` / `sleep`), `timeout` y captura de errores HTTP[cite: 3, 6, 7].
5. **Modelo de 4 esquemas + Auditoría en PostgreSQL:** Debe existir el DDL para los esquemas `raw`, `staging`, `dw` y `mart`, además de la tabla `public.etl_audit`[cite: 3, 6].
6. **Carga Idempotente:** Volver a ejecutar cualquier DAG para la misma fecha o lote no debe duplicar registros en `dw` ni en `mart` (obligatorio usar `INSERT ... ON CONFLICT DO UPDATE` o reemplazo por partición de fecha)[cite: 3, 6].
7. **Soporte de Backfill (recarga de fechas pasadas):** Los DAGs deben estar parametrizados con la fecha lógica de ejecución de Airflow (`logical_date` / `ds`) para permitir recargar rangos históricos[cite: 3].

---

## 3. ESTRUCTURA DE DIRECTORIOS OBJETIVO

Reorganiza el workspace para que quede exactamente con esta estructura limpia y reproducible mediante el `README.md`[cite: 4, 6]:

```text
proyecto_etl_aduanas/
│
├── docker-compose.yml                  # Orquestación de contenedores (Airflow + PostgreSQL)
├── Dockerfile                          # (Opcional) Imagen extendida de Airflow con librerías
├── requirements.txt                    # Dependencias de Python
├── .env.example                        # Plantilla de variables de entorno sin claves reales
├── .gitignore                          # Ignorar .env, __pycache__, logs/*, datos locales
├── README.md                           # Manual completo de instalación, configuración y ejecución
├── PLAN_DE_IMPLEMENTACION.md           # Este documento maestro
│
├── scripts/
│   ├── init_airflow_resources.sh       # Crea automáticamente Connections y Variables en Airflow
│   └── descubrir_endpoints_aduanas.py  # Script utilitario para consultar resource_id activos en datos.gob.cl
│
├── sql/
│   ├── 01_ddl_esquemas.sql             # Creación de esquemas raw, staging, dw, mart y tabla etl_audit
│   └── 02_indicadores_mart.sql         # Consultas SQL de los 5 indicadores de gestión (KPIs)
│
├── src/
│   ├── __init__.py
│   ├── extract.py                      # Validación HTTP, paginación CKAN API y guardado en esquema raw
│   ├── transform.py                    # Limpieza, normalización, cálculo de % nulos y paso a staging
│   ├── load.py                         # Carga idempotente (ON CONFLICT) de dimensiones y hechos en dw
│   └── audit.py                        # Registro de métricas en etl_audit y on_failure_callback
│
└── dags/
    ├── dag_01_etl_importaciones.py     # DAG 1: ETL completo de Importaciones de Aduanas
    ├── dag_02_etl_exportaciones.py     # DAG 2: ETL completo de Exportaciones de Aduanas
    └── dag_03_indicadores_mart.py      # DAG 3: Cálculo automático de KPIs en SQL sobre esquema mart
```

---

## 4. PLAN DE TRABAJO PASO A PASO PARA EL IDE

### FASE 1: Inspección y Limpieza del Proyecto CSV Actual
1. Analiza los archivos actuales del workspace (`docker-compose.yml`, carpetas `dags/`, `data/`, `plugins/`, etc.).
2. Elimina cualquier script de extracción desde CSV y cualquier archivo `.csv` de prueba para cumplir con la restricción de no usar archivos descargados[cite: 1, 3, 6].
3. Conserva la configuración funcional de red y puertos de `docker-compose.yml`, adaptándola según la Fase 2.

---

### FASE 2: Infraestructura Docker y Configuración de Entorno
1. **Archivo `requirements.txt`:**
   Asegúrate de incluir e instalar en los contenedores de Airflow[cite: 1, 3]:
   ```text
   requests>=2.31.0
   pandas>=2.1.0
   psycopg2-binary>=2.9.9
   sqlalchemy>=1.4.50
   apache-airflow-providers-postgres>=5.10.0
   ```
2. **Archivo `docker-compose.yml`:**
   - Montar los volúmenes `./dags:/opt/airflow/dags`, `./src:/opt/airflow/src`, `./sql:/opt/airflow/sql`, `./scripts:/opt/airflow/scripts` y `./logs:/opt/airflow/logs`[cite: 4, 6].
   - Definir `PYTHONPATH: /opt/airflow` en las variables de entorno de los servicios de Airflow para permitir importar `from src.extract import ...`.
   - En el contenedor de PostgreSQL, montar `./sql/01_ddl_esquemas.sql:/docker-entrypoint-initdb.d/01_ddl_esquemas.sql` para que los esquemas y tablas se creen automáticamente al iniciar la base de datos[cite: 3, 6].
   - Leer usuarios y contraseñas desde variables de entorno (`.env`) y entregar un archivo `.env.example` limpio (sin contraseñas reales)[cite: 1, 7].
3. **Script `scripts/init_airflow_resources.sh`:**
   Crea un script ejecutable que configure mediante el CLI de Airflow (`airflow connections add` y `airflow variables set`) lo siguiente[cite: 7]:
   - **Connection `postgres_dw`:** Conecta Airflow con la base de datos PostgreSQL de destino[cite: 1, 7].
   - **Variables de Airflow:**
     - `aduanas_api_base_url`: `https://datos.gob.cl/api/3/action/datastore_search`[cite: 3, 7]
     - `aduanas_resource_id_importaciones`: ID del recurso DataStore de Importaciones[cite: 3, 7].
     - `aduanas_resource_id_exportaciones`: ID del recurso DataStore de Exportaciones[cite: 3, 7].
     - `aduanas_api_page_limit`: `500` (tamaño de página para la extracción paginada)[cite: 3, 6].
     - `aduanas_api_max_pages`: `5` (configurable para controlar el volumen y tiempos de prueba)[cite: 3, 6].
     - `aduanas_api_token`: `""` (opcional, enviado en el header `Authorization` si se define, cumpliendo soporte de autenticación)[cite: 6, 7].

---

### FASE 3: Descubrimiento y Configuración de los Endpoints de Aduanas (`datos.gob.cl`)
1. Crea el script utilitario `scripts/descubrir_endpoints_aduanas.py` que consulte la API de catálogo de `datos.gob.cl`:
   - `https://datos.gob.cl/api/3/action/package_search?q=servicio_nacional_de_aduanas&rows=50`
   - `https://datos.gob.cl/api/3/action/package_search?q=importaciones+aduanas&rows=30`
   - `https://datos.gob.cl/api/3/action/package_search?q=exportaciones+aduanas&rows=30`
2. El script debe filtrar automáticamente los recursos que tengan `"datastore_active": true` (es decir, que respondan en formato JSON vía `/api/3/action/datastore_search?resource_id=...` sin requerir descarga manual de archivos)[cite: 1, 3, 6] e imprimir en consola los `resource_id` válidos para Importaciones y Exportaciones, actualizándolos en las Variables de Airflow[cite: 7].

---

### FASE 4: Modelo de Datos en PostgreSQL (`sql/01_ddl_esquemas.sql`)
Genera el script DDL completo con creación de esquemas, claves primarias, claves foráneas y restricciones de unicidad para garantizar cargas idempotentes[cite: 3, 6]:

```sql
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

-- 3. CAPA RAW (Datos crudos tal como llegan de la API + fecha y hora de extracción)
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
-- Dimensión Tiempo (Obligatoria por pauta)
CREATE TABLE IF NOT EXISTS dw.dim_tiempo (
    id_tiempo INT PRIMARY KEY, -- Formato YYYYMMDD o YYYYMM01
    fecha DATE UNIQUE NOT NULL,
    anio INT NOT NULL,
    mes INT NOT NULL,
    nombre_mes VARCHAR(20) NOT NULL,
    trimestre INT NOT NULL,
    semestre INT NOT NULL
);

-- Dimensión Aduana (Campo de cruce 1 entre Importaciones y Exportaciones)
CREATE TABLE IF NOT EXISTS dw.dim_aduana (
    id_aduana SERIAL PRIMARY KEY,
    codigo_aduana INT UNIQUE NOT NULL,
    nombre_aduana VARCHAR(120) NOT NULL,
    zona_geografica VARCHAR(50) NOT NULL
);

-- Dimensión País de Origen/Destino (Campo de cruce 2)
CREATE TABLE IF NOT EXISTS dw.dim_pais (
    id_pais SERIAL PRIMARY KEY,
    codigo_pais INT UNIQUE NOT NULL,
    nombre_pais VARCHAR(120) NOT NULL,
    continente_region VARCHAR(80) NOT NULL
);

-- Dimensión Vía de Transporte (Campo de cruce 3)
CREATE TABLE IF NOT EXISTS dw.dim_via_transporte (
    id_via_transporte SERIAL PRIMARY KEY,
    codigo_via_transporte INT UNIQUE NOT NULL,
    nombre_via_transporte VARCHAR(100) NOT NULL
);

-- Tabla de Hechos de Comercio Exterior
CREATE TABLE IF NOT EXISTS dw.fact_comercio_exterior (
    id_hecho BIGSERIAL PRIMARY KEY,
    codigo_operacion VARCHAR(120) UNIQUE NOT NULL, -- Garantiza Idempotencia con ON CONFLICT
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
```

---

### FASE 5: Desarrollo de los Módulos ETL en Python (`src/`)

#### 1. `src/extract.py` (Extracción Robusta desde la API)
Debe implementar dos funciones principales parametrizadas por `tipo_flujo` (`IMPORTACION` o `EXPORTACION`):
- **`validar_api_disponible(tipo_flujo)`:**
  - Lee `aduanas_api_base_url` y el `resource_id` correspondiente desde `airflow.models.Variable`[cite: 7].
  - Realiza una petición `requests.get(url, params={'resource_id': resource_id, 'limit': 1}, headers=headers, timeout=10)`[cite: 3, 7].
  - Verifica que el código HTTP sea `200` y que el JSON contenga `"success": True`[cite: 3, 6]. Si no responde o la URL fue alterada (por ejemplo, durante la demostración de falla controlada), lanza `RuntimeError` para activar los reintentos de Airflow y el `on_failure_callback`[cite: 4, 7].
- **`extraer_datos_api_a_raw(tipo_flujo, run_id, logical_date)`:**
  - Consume la API paginando mediante un ciclo `while` con `limit` y `offset` (`offset = pagina * limit`)[cite: 3, 6].
  - Incluye control de límites de uso (`time.sleep(0.5)` entre páginas), manejo de excepciones HTTP (`HTTPError`, `Timeout`, `ConnectionError`) y soporte de filtro por fecha/periodo (`q` o `filters`) para permitir recargar rangos de fechas pasados[cite: 3, 6].
  - Inserta cada página tal como llega (JSON crudo) en `raw.importaciones_raw` o `raw.exportaciones_raw` junto con `CURRENT_TIMESTAMP` (`fecha_extraccion`), `run_id` y `logical_date`[cite: 3].
  - Devuelve el total de registros leídos mediante XCom para la auditoría[cite: 3].

#### 2. `src/transform.py` (Limpieza, Normalización y Calidad de Datos)
Lee el JSON crudo recién insertado en `raw` para ese `run_id` y aplica todas las transformaciones exigidas en la pauta antes de insertar en `staging.stg_operaciones_aduanas`[cite: 3]:
1. **Cálculo de métrica de nulos:** Cuenta el porcentaje de celdas nulas o vacías en los registros crudos (`porcentaje_nulos`) y lo envía por XCom para registrarlo en `public.etl_audit`[cite: 3, 4].
2. **Renombrado y homologación de columnas:** Mapea los nombres de campos crudos de los formularios DIN (Importaciones) y DUS (Exportaciones) de Aduanas a un esquema estándar (`codigo_operacion`, `fecha_operacion`, `codigo_aduana`, `codigo_pais`, `codigo_via_transporte`, `peso_bruto_kg`, `monto_fob_usd`, `monto_flete_usd`, `monto_seguro_usd`, `monto_cif_usd`)[cite: 3].
3. **Conversión de tipos de datos:** Convierte fechas a `DATE` y textos numéricos (con comas o puntos decimales) a `NUMERIC(18,2)`[cite: 3].
4. **Normalización de textos y recodificación (mapeo de códigos):** Convierte textos a mayúsculas sin espacios laterales (`strip().upper()`) y mapea los códigos oficiales de Aduanas de Chile a sus descripciones reales (ej. Código Aduana `34` -> `'SAN ANTONIO'`, `33` -> `'VALPARAISO'`, `48` -> `'METROPOLITANA'`, `07` -> `'IQUIQUE'`; Vía de Transporte `1` -> `'MARITIMA'`, `4` -> `'AEREA'`, `7` -> `'TERRESTRE'`)[cite: 3].
5. **Tratamiento de nulos y valores atípicos:** Imputa `0.0` en fletes/seguros nulos (común en exportaciones FOB), descarta o corrige registros con montos o pesos negativos, y filtra registros sin código de operación válido[cite: 3].
6. **Eliminación de duplicados:** Ejecuta `drop_duplicates(subset=['codigo_operacion'], keep='last')`[cite: 3].
7. **Creación de campos calculados:**
   - `costo_logistico_usd = monto_flete_usd + monto_seguro_usd`[cite: 3].
   - Si en un registro `monto_cif_usd == 0` pero existe `monto_fob_usd`, calcula `monto_cif_usd = monto_fob_usd + costo_logistico_usd`[cite: 3].
8. **Carga en `staging`:** Guarda los registros limpios en `staging.stg_operaciones_aduanas` usando `ON CONFLICT (codigo_operacion) DO UPDATE`[cite: 3, 6].

#### 3. `src/load.py` (Carga Idempotente hacia el Modelo Dimensional `dw`)
Lee desde `staging.stg_operaciones_aduanas` filtrando por `tipo_flujo` y `run_id`, y ejecuta en una transacción SQL[cite: 3, 6]:
1. **Poblado idempotente de Dimensiones:**
   - Inserta en `dw.dim_tiempo`, `dw.dim_aduana`, `dw.dim_pais` y `dw.dim_via_transporte` utilizando `INSERT INTO ... ON CONFLICT (...) DO UPDATE SET ...` (o `DO NOTHING`)[cite: 3, 6].
2. **Poblado idempotente de la Tabla de Hechos (`dw.fact_comercio_exterior`):**
   - Cruza `staging.stg_operaciones_aduanas` con los IDs subrogados de las 4 dimensiones e inserta en `dw.fact_comercio_exterior` usando:
     ```sql
     INSERT INTO dw.fact_comercio_exterior (
         codigo_operacion, id_tiempo, id_aduana, id_pais, id_via_transporte,
         tipo_flujo, peso_bruto_kg, peso_toneladas, cantidad_bultos,
         monto_fob_usd, monto_flete_usd, monto_seguro_usd, monto_cif_usd,
         costo_logistico_usd, fecha_carga_dw
     )
     VALUES (...)
     ON CONFLICT (codigo_operacion) DO UPDATE SET
         peso_bruto_kg = EXCLUDED.peso_bruto_kg,
         peso_toneladas = EXCLUDED.peso_toneladas,
         monto_fob_usd = EXCLUDED.monto_fob_usd,
         monto_flete_usd = EXCLUDED.monto_flete_usd,
         monto_seguro_usd = EXCLUDED.monto_seguro_usd,
         monto_cif_usd = EXCLUDED.monto_cif_usd,
         costo_logistico_usd = EXCLUDED.costo_logistico_usd,
         fecha_carga_dw = CURRENT_TIMESTAMP;
     ```
   - Retorna por XCom la cantidad exacta de `filas_cargadas`[cite: 3].

#### 4. `src/audit.py` (Auditoría y Tolerancia a Fallas)
- **`registrar_auditoria_exito(**context)`:** Recupera de XCom `filas_leidas`, `filas_cargadas`, `porcentaje_nulos` y el tiempo de inicio, calcula `duracion_seg` e inserta una fila con `estado = 'SUCCESS'` en `public.etl_audit`[cite: 3, 4].
- **`registrar_falla_callback(context)`:** Función asignada al parámetro `on_failure_callback` de todos los DAGs[cite: 3, 7]. Cuando cualquier tarea agota sus reintentos y falla, extrae automáticamente `dag_id`, `run_id`, `logical_date`, calcula la duración hasta el fallo, captura la excepción (`str(context.get('exception'))`) e inserta una fila en `public.etl_audit` con `estado = 'FAILED'`[cite: 3, 7].

---

### FASE 6: Orquestación de los 3 DAGs en Apache Airflow (`dags/`)

Configura los 3 archivos en `dags/` con estas especificaciones exactas[cite: 3, 7]:
- `default_args`:
  - `retries: 2`[cite: 3, 7]
  - `retry_delay: timedelta(seconds=15)` (tiempo breve para que durante los 8 minutos de exposición se vean los reintentos en vivo sin esperar varios minutos)[cite: 4, 5, 7].
  - `execution_timeout: timedelta(minutes=5)`[cite: 7].
  - `on_failure_callback: registrar_falla_callback`[cite: 3, 7].
- **Estructura de `dag_01_etl_importaciones.py` y `dag_02_etl_exportaciones.py`:**
  ```text
  validar_api >> extraer_a_raw >> transformar_a_staging >> cargar_a_dw >> registrar_auditoria >> disparar_dag_indicadores
  ```
  - Donde `disparar_dag_indicadores` usa `TriggerDagRunOperator(task_id="disparar_dag_indicadores", trigger_dag_id="dag_03_indicadores_mart", wait_for_completion=False, reset_dag_run=True)`[cite: 7].
- **Estructura de `dag_03_indicadores_mart.py`:**
  - Ejecuta el script `sql/02_indicadores_mart.sql` para recalcular las 5 tablas de indicadores sobre los datos disponibles en `dw` y registra su propia corrida en `public.etl_audit`[cite: 3, 4, 7].

---

### FASE 7: Consultas SQL de los 5 Indicadores de Gestión (`sql/02_indicadores_mart.sql`)

El esquema `mart` debe calcular con SQL estos **5 indicadores de gestión** cruzando Importaciones y Exportaciones[cite: 4, 7]. Para que sea idempotente, cada tabla en `mart` debe crearse y actualizarse con `TRUNCATE TABLE ...; INSERT INTO ...` o `CREATE OR REPLACE TABLE / INSERT ... ON CONFLICT`[cite: 3, 6]:

1. **`mart.kpi_01_balanza_comercial_aduana` (Balanza Comercial y Cobertura por Aduana y Mes):**
   - **Objetivo:** Medir el superávit o déficit comercial (Exportaciones FOB menos Importaciones CIF) en cada aduana del país para priorizar infraestructura aduanera[cite: 4, 7].
   - **Fórmula:** $\text{Saldo USD} = \sum \text{Exportaciones FOB} - \sum \text{Importaciones CIF}$ y $\text{Tasa Cobertura (\%)} = (\frac{\sum \text{Exportaciones FOB}}{\sum \text{Importaciones CIF}}) \times 100$[cite: 4].
   - **Meta:** Tasa de Cobertura Comercial $\ge 100\%$ a nivel agregado nacional[cite: 4, 7].

2. **`mart.kpi_02_incidencia_costo_logistico` (% de Flete y Seguro sobre Valor CIF de Importación):**
   - **Objetivo:** Evaluar el peso del transporte internacional y seguros sobre el costo total de internación según vía de transporte y aduana[cite: 4, 7].
   - **Fórmula:** $\frac{\sum (\text{monto\_flete\_usd} + \text{monto\_seguro\_usd})}{\sum \text{monto\_cif\_usd}} \times 100$[cite: 4].
   - **Meta / Umbral:** $\le 10\%$ en vía marítima y $\le 18\%$ en vía aérea[cite: 4, 7].

3. **`mart.kpi_03_concentracion_carga_aduanera` (Índice de Concentración Logística por Aduana):**
   - **Objetivo:** Detectar cuellos de botella operacionales midiendo qué porcentaje del tonelaje total nacional (Importaciones + Exportaciones) se concentra en cada aduana[cite: 4, 7].
   - **Fórmula:** $\frac{\sum \text{Toneladas en Aduana } i}{\sum \text{Toneladas Nacionales}} \times 100$[cite: 4].
   - **Meta / Umbral:** Ninguna aduana individual debe superar el $40\%$ de la carga nacional sin plan de contingencia[cite: 4, 7].

4. **`mart.kpi_04_valor_agregado_tonelada` (Valor Promedio USD por Tonelada Exportada vs. Importada por País):**
   - **Objetivo:** Comparar la densidad de valor económico de la carga que Chile exporta frente a la que importa con cada socio comercial[cite: 4, 7].
   - **Fórmula:** $\frac{\sum \text{monto\_fob\_usd}}{\sum \text{peso\_toneladas}}$ por `tipo_flujo` y `nombre_pais`[cite: 4].
   - **Meta:** Valor promedio de exportación $\ge 1.200 \text{ USD/Ton}$[cite: 4, 7].

5. **`mart.kpi_05_participacion_socios_comerciales` (Participación y Dependencia por Socio Comercial):**
   - **Objetivo:** Evaluar el grado de diversificación del comercio exterior chileno y alertar sobreexposición económica con socios específicos[cite: 4, 7].
   - **Fórmula:** $(\frac{\text{Intercambio Comercial País } i}{\text{Intercambio Comercial Nacional Total}}) \times 100$, donde $\text{Intercambio} = \text{FOB Export} + \text{CIF Import}$[cite: 4].
   - **Meta / Umbral:** Ningún socio comercial individual debe concentrar más del $35.0\%$ del intercambio comercial nacional[cite: 4, 7].

---

### FASE 8: Verificación, Pruebas de Falla Controlada y Documentación (`README.md`)

Una vez que hayas creado todos los archivos anteriores, ejecuta desde la terminal las siguientes validaciones:
1. Levantar los servicios con `docker compose up -d --build`.
2. Ejecutar `docker exec airflow-webserver bash /opt/airflow/scripts/init_airflow_resources.sh` (ajustando al nombre del contenedor en `docker-compose.yml`) para registrar `postgres_dw` y las Variables de la API[cite: 7].
3. Ejecutar `dag_01_etl_importaciones` y `dag_02_etl_exportaciones`, verificando que ambos terminen en `SUCCESS`, disparen automáticamente `dag_03_indicadores_mart` y poblen los esquemas `raw`, `staging`, `dw`, `mart` y `public.etl_audit`[cite: 3, 6, 7].
4. Ejecutar por segunda vez `dag_01_etl_importaciones` y comprobar con un `SELECT COUNT(*) FROM dw.fact_comercio_exterior;` que **no se dupliquen los registros** (prueba de idempotencia)[cite: 3, 6].
5. Redactar un `README.md` completo que explique:
   - Arquitectura y descripción de los 4 esquemas (`raw`, `staging`, `dw`, `mart`)[cite: 3, 6].
   - Comandos exactos para levantar Docker e inicializar las Variables y Conexiones en Airflow[cite: 6, 7].
   - Instrucciones paso a paso para ejecutar el **escenario de falla controlada en vivo** (cambiar la variable `aduanas_api_base_url` a una URL inválida, disparar `dag_01`, observar los reintentos y el registro `FAILED` en `public.etl_audit`, comprobar que `dag_02` y `dag_03` siguen operativos y restaurar la variable)[cite: 2, 4, 7].