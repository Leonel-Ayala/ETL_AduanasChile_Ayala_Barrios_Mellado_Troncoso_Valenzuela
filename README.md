# PROYECTO ETL ADUANAS DE CHILE (IMPORTACIONES Y EXPORTACIONES)
**Asignatura:** INF-522 Gestión Informática II | **Carrera:** Ingeniería Civil Informática  
**Motor BD:** PostgreSQL 15 | **Orquestador:** Apache Airflow 2.9 (Docker) | **Lenguaje:** Python 3.11  

---

## 1. DESCRIPCIÓN Y OBJETIVO DE LA SOLUCIÓN

Este proyecto implementa una arquitectura moderna de ingeniería de datos para la ingesta, depuración, almacenamiento dimensional y cálculo analítico de operaciones de comercio exterior de Chile, consumiendo exclusivamente las **APIs públicas oficiales de Datos Abiertos del Gobierno de Chile (`datos.gob.cl`)** provistas por el **Servicio Nacional de Aduanas**.

### Cumplimiento Estricto de Restricciones
- **100% Consumo por API:** No se utilizan archivos `.csv` ni `.xlsx` locales ni descargas manuales. Toda la extracción se realiza mediante peticiones HTTP con la librería `requests`.
- **Cero Claves Expuestas:** No existen contraseñas, URLs fijas ni credenciales en el código fuente. Se gestionan mediante Airflow Connections (`postgres_dw`) y Airflow Variables.
- **Arquitectura de 3 DAGs Desacoplados:**
  - `dag_01_etl_importaciones`: Ingesta y modelado del flujo de importaciones.
  - `dag_02_etl_exportaciones`: Ingesta y modelado del flujo de exportaciones.
  - `dag_03_indicadores_mart`: Recálculo SQL de los 5 Indicadores de Gestión en el Data Mart.
- **Aislamiento de Fallas:** Si un DAG o fuente falla, los demás continúan ejecutándose sin interrumpir el servicio.
- **Carga Idempotente:** Todas las inserciones en staging, DW (`dw.fact_comercio_exterior`) y Data Mart utilizan `ON CONFLICT DO UPDATE` y transacciones atómicas, permitiendo re-ejecuciones y reprocesamiento histórico (backfill) sin duplicación de registros.

---

## 2. ARQUITECTURA DE DATOS (4 CAPAS + AUDITORÍA)

```mermaid
flowchart TD
    API1["API datos.gob.cl<br/>Importaciones"] -->|requests + paginación| RAW_IMP[("raw.importaciones_raw<br/>(Payload JSONB)")]
    API2["API datos.gob.cl<br/>Exportaciones"] -->|requests + paginación| RAW_EXP[("raw.exportaciones_raw<br/>(Payload JSONB)")]
    
    RAW_IMP -->|Limpieza, % nulos, recodificación| STG[("staging.stg_operaciones_aduanas<br/>(Esquema homologado)")]
    RAW_EXP -->|Limpieza, % nulos, recodificación| STG
    
    STG -->|Carga Idempotente ON CONFLICT| DIMS[("dw.dim_tiempo<br/>dw.dim_aduana<br/>dw.dim_pais<br/>dw.dim_via_transporte")]
    STG -->|Carga de Hechos| FACTS[("dw.fact_comercio_exterior")]
    
    FACTS -->|Trigger automático| MART["Data Mart SQL<br/>5 Indicadores de Gestión (mart.*)"]
    
    DAG1["dag_01_etl_importaciones"] -.-> AUDIT[("public.etl_audit<br/>(Auditoría y Fallas)")]
    DAG2["dag_02_etl_exportaciones"] -.-> AUDIT
    DAG3["dag_03_indicadores_mart"] -.-> AUDIT
```

### Descripción de Esquemas en PostgreSQL:
1. **`raw` (Capa Cruda):**
   - Tablas: `raw.importaciones_raw`, `raw.exportaciones_raw`.
   - Guarda el payload JSON íntegro retornado por la API, junto con `run_id`, `fecha_logical`, `resource_id`, `offset_pagina` y timestamp de extracción.
2. **`staging` (Capa Limpia y Estandarizada):**
   - Tabla: `staging.stg_operaciones_aduanas`.
   - Datos tipados, homologados a nombres estándar, normalizados a mayúsculas, recodificados con diccionarios aduaneros, imputados y validados.
3. **`dw` (Modelo Dimensional Estrella):**
   - **4 Dimensiones:**
     - `dw.dim_tiempo`: Jerarquía de fecha, año, mes, nombre de mes, trimestre y semestre.
     - `dw.dim_aduana`: Aduana de tramitación (San Antonio, Valparaíso, Metropolitana, etc.) y zona geográfica (Centro, Norte, Sur, Austral).
     - `dw.dim_pais`: País de origen/destino y continente/región.
     - `dw.dim_via_transporte`: Vía marítima, aérea, terrestre o multimodal.
   - **1 Tabla de Hechos:**
     - `dw.fact_comercio_exterior`: Registra cada operación con métricas numéricas (`peso_bruto_kg`, `peso_toneladas`, `cantidad_bultos`, `monto_fob_usd`, `monto_flete_usd`, `monto_seguro_usd`, `monto_cif_usd`, `costo_logistico_usd`).
4. **`mart` (Data Mart de Indicadores):**
   - Contiene las 5 tablas de indicadores agregados calculadas por `dag_03_indicadores_mart`.
5. **`public.etl_audit` (Gobernanza y Trazabilidad):**
   - Almacena métricas de cada corrida: `dag_id`, `run_id`, `filas_leidas`, `filas_cargadas`, `porcentaje_nulos`, `duracion_seg`, `estado` (`SUCCESS` o `FAILED`) y detalle de errores en caso de fallo (`on_failure_callback`).

---

## 3. FUENTES SELECCIONADAS DE DATOS ABIERTOS (`datos.gob.cl`)

| Flujo | Recurso API | Resource ID Activo | Registros Totales | Formato API |
| :--- | :--- | :--- | :--- | :--- |
| **Importaciones** | Registro de Importaciones de Aduanas | `c2211d9e-4c8e-4ef4-84d5-87039c515584` | 32.117+ | CKAN DataStore (JSON) |
| **Exportaciones** | Registro de Exportaciones de Aduanas | `f6fe6014-e353-4bc6-bd5d-762511a4dd57` | 55.331+ | CKAN DataStore (JSON) |

**Campos de Cruce entre Fuentes:**
1. **Aduana de Control:** Código y nombre de aduana en territorio nacional.
2. **País de Contraparte:** País de procedencia (importación) o destino (exportación).
3. **Vía de Transporte:** Modo logístico de internación o embarque.
4. **Período Temporal:** Año y mes de aceptación de la operación.

---

## 4. GUÍA DE INSTALACIÓN Y PUESTA EN MARCHA (DOCKER)

### Paso 1: Configurar Variables de Entorno
Copiar la plantilla de variables de entorno:
```bash
cp .env.example .env
```
*(El archivo `.env` ya incluye los puertos y contraseñas por defecto preconfigurados para pruebas locales).*

### Paso 2: Levantar Contenedores con Docker Compose
```bash
docker compose up -d --build
```
Esto levantará:
- `aduanas-postgres-dw` (PostgreSQL 15 expuesto en puerto host `5433` para evitar conflicto con servicios nativos de Windows, y puerto interno `5432` de Docker con esquemas inicializados automáticamente).
- `aduanas-airflow-init` (Inicializa la base de datos de Airflow y crea el usuario `admin`).
- `aduanas-airflow-webserver` (Interfaz Web de Airflow en `http://localhost:8080`).
- `aduanas-airflow-scheduler` (Planificador y ejecutor de tareas).

> **Nota para conexión desde DBeaver en Windows:** Conectar a `localhost:5433`, Base de datos: `aduanas_dw`, Usuario: `postgres`, Contraseña: `aduanas_dw_secure_pass`.

### Paso 3: Inicializar Conexiones y Variables en Airflow
Ejecutar el script de inicialización automatizada:
```bash
docker exec -it aduanas-airflow-webserver python /opt/airflow/scripts/init_airflow_resources.py
```
Este script crea:
- Conexión **`postgres_dw`**: `postgresql://postgres:aduanas_dw_secure_pass@postgres:5432/aduanas_dw`.
- Variables de la API: `aduanas_api_base_url`, `aduanas_resource_id_importaciones`, `aduanas_resource_id_exportaciones`, `aduanas_api_page_limit` (500), `aduanas_api_max_pages` (5).

> 📘 **Guía Maestra de Presentación y Examen Oral:** Consulta [GUIA_PRESENTACION_Y_DEFENSA.md](GUIA_PRESENTACION_Y_DEFENSA.md) para el desglose de los 8 minutos de exposición, protocolo de falla en vivo y banco de preguntas individuales.

---

## 5. EJECUCIÓN Y OPERACIÓN DE LOS DAGS

Accede a la interfaz web de Airflow en:
👉 **URL:** [http://localhost:8080](http://localhost:8080)  
👉 **Usuario:** `admin` | **Contraseña:** `admin`

### Flujo de Ejecución:
1. Activar y disparar manualmente **`dag_01_etl_importaciones`**:
   - Ejecuta `validar_api` (HTTP 200 y validación JSON).
   - Extrae lotes paginados hacia `raw.importaciones_raw`.
   - Limpia, normaliza e inserta en `staging.stg_operaciones_aduanas`.
   - Carga idempotentemente en `dw.dim_*` y `dw.fact_comercio_exterior`.
   - Registra auditoría `SUCCESS` en `public.etl_audit`.
   - Dispara automáticamente mediante `TriggerDagRunOperator` el DAG **`dag_03_indicadores_mart`**.
2. Activar y disparar **`dag_02_etl_exportaciones`**:
   - Sigue el mismo ciclo para el flujo de exportaciones, disparando al finalizar `dag_03_indicadores_mart`.
3. Comprobar en PostgreSQL que los datos se poblaron en todas las capas.

### Prueba de Idempotencia:
Vuelve a ejecutar `dag_01_etl_importaciones` una segunda o tercera vez.
Verifica mediante la siguiente consulta SQL que el número de hechos no se duplica:
```sql
SELECT tipo_flujo, COUNT(*) AS total_hechos, COUNT(DISTINCT codigo_operacion) AS operaciones_unicas
FROM dw.fact_comercio_exterior
GROUP BY tipo_flujo;
```
*(El resultado muestra que `total_hechos = operaciones_unicas` demostrando idempotencia total).*

---

## 6. LOS 5 INDICADORES DE GESTIÓN (KPIS EN DATA MART)

Cada indicador en el esquema `mart.*` incluye explícitamente en cada registro su documentación completa según pauta:

1. **`mart.kpi_01_balanza_comercial_aduana` (Balanza Comercial y Cobertura por Aduana y Mes):**
   - **Indicador:** Balanza Comercial y Cobertura por Aduana.
   - **Objetivo / Decisión:** Medir el superávit o déficit comercial (FOB - CIF) por aduana para priorizar inversiones en infraestructura y agilización aduanera.
   - **Fuente:** API `datos.gob.cl` - Aduanas (Importaciones y Exportaciones).
   - **Periodicidad:** MENSUAL.
   - **Fórmula:** $\text{Saldo USD} = \sum \text{FOB Export} - \sum \text{CIF Import}$; $\text{Tasa Cobertura (\%)} = (\frac{\sum \text{FOB Export}}{\sum \text{CIF Import}}) \times 100$.
   - **Meta / Umbral:** Tasa de Cobertura agregada $\ge 100.0\%$ (Superávit comercial).

2. **`mart.kpi_02_incidencia_costo_logistico` (% de Flete + Seguro sobre Valor CIF de Importación):**
   - **Indicador:** Incidencia del Costo Logístico sobre Valor CIF.
   - **Objetivo / Decisión:** Evaluar el sobrecosto de internación por transporte internacional y seguro para negociar acuerdos y reducir costos arancelarios/logísticos.
   - **Fuente:** API `datos.gob.cl` - Aduanas (Importaciones DIN).
   - **Periodicidad:** MENSUAL.
   - **Fórmula:** $\frac{\sum (\text{monto\_flete\_usd} + \text{monto\_seguro\_usd})}{\sum \text{monto\_cif\_usd}} \times 100$.
   - **Meta / Umbral:** $\le 10.0\%$ en vía marítima y $\le 18.0\%$ en vía aérea.

3. **`mart.kpi_03_concentracion_carga_aduanera` (Índice de Concentración Logística por Aduana):**
   - **Indicador:** Índice de Concentración Logística por Aduana.
   - **Objetivo / Decisión:** Detectar cuellos de botella operacionales y dependencia crítica en puertos para activar planes de contingencia logística.
   - **Fuente:** API `datos.gob.cl` - Aduanas (Importaciones y Exportaciones).
   - **Periodicidad:** MENSUAL.
   - **Fórmula:** $\frac{\sum \text{Toneladas Aduana } i}{\sum \text{Toneladas Nacionales}} \times 100$.
   - **Meta / Umbral:** Concentración $\le 40.0\%$ (Alerta si $> 40.0\%$).

4. **`mart.kpi_04_valor_agregado_tonelada` (Valor Promedio USD por Tonelada por Socio Comercial):**
   - **Indicador:** Densidad de Valor Económico por Tonelada.
   - **Objetivo / Decisión:** Comparar el valor económico generado por tonelada transada para evaluar si Chile exporta materias primas de bajo valor o bienes manufacturados con alto valor agregado.
   - **Fuente:** API `datos.gob.cl` - Aduanas (Comercio Exterior).
   - **Periodicidad:** MENSUAL.
   - **Fórmula:** $\frac{\sum \text{monto\_fob\_usd}}{\sum \text{peso\_toneladas}}$ por país y flujo.
   - **Meta / Umbral:** Exportaciones $\ge 1.200 \text{ USD/Ton}$.

5. **`mart.kpi_05_calidad_y_eficiencia_etl` (Gobernanza y Rendimiento del Pipeline):**
   - **Indicador:** Gobernanza, Calidad de Datos y Eficiencia ETL.
   - **Objetivo / Decisión:** Monitorear la calidad de los datos gubernamentales de origen, el porcentaje de nulos y la estabilidad operativa de las ejecuciones en Airflow.
   - **Fuente:** Metadatos de Auditoría (`public.etl_audit`).
   - **Periodicidad:** POR EJECUCIÓN (DIARIO).
   - **Fórmula:** Tasa de retención $(\frac{\sum \text{filas\_cargadas}}{\sum \text{filas\_leidas}}) \times 100$, promedio de % nulos y tasa de éxito de los DAGs.
   - **Meta / Umbral:** Tasa de éxito $\ge 95.0\%$, nulos críticos $< 5.0\%$, retención $\ge 95.0\%$.

---

## 7. PROTOCOLO DE DEMOSTRACIÓN DE FALLA CONTROLADA EN VIVO (8 MINUTOS)

Para cumplir con el requerimiento de presentación grupal frente al docente:

1. **Provocar la Falla en Vivo:**
   Cambiar la URL base de la API en Airflow a un endpoint inexistente:
   ```bash
   docker exec -it aduanas-airflow-webserver airflow variables set aduanas_api_base_url "https://datos.gob.cl/api/3/action/endpoint_invalido"
   ```
2. **Disparar `dag_01_etl_importaciones`:**
   - La tarea `validar_api` fallará inmediatamente al no obtener código HTTP 200.
   - En la interfaz web de Airflow se apreciarán los **2 reintentos automáticos** programados cada **15 segundos** (`retry_delay=15s`).
3. **Verificar el Registro de Falla en la Auditoría:**
   Consultar la tabla `public.etl_audit` en PostgreSQL:
   ```sql
   SELECT id_audit, dag_id, estado, duracion_seg, mensaje_error
   FROM public.etl_audit
   ORDER BY id_audit DESC LIMIT 1;
   ```
   *(Se observará el registro con `estado = 'FAILED'` capturado por `on_failure_callback`).*
4. **Demostrar el Aislamiento de Fallas:**
   Ejecutar `dag_02_etl_exportaciones`:
   - Mostrar que `dag_02` se ejecuta de forma independiente y finaliza en `SUCCESS`, disparando `dag_03_indicadores_mart` sin ser detenido por la caída de `dag_01`.
5. **Restaurar el Servicio:**
   Restaurar la URL válida de la API:
   ```bash
   docker exec -it aduanas-airflow-webserver airflow variables set aduanas_api_base_url "https://datos.gob.cl/api/3/action/datastore_search"
   ```
   Volver a disparar `dag_01_etl_importaciones` y comprobar que finaliza exitosamente en `SUCCESS`.

---

## 8. ESTRATEGIA DE DATOS, RIESGOS Y ESCALABILIDAD

- **Gobernanza y Calidad:** Se registra el porcentaje de celdas nulas o no informadas en cada lote de extracción. Se descartan o corrigen valores numéricos anómalos o negativos.
- **Riesgos y Mitigación:**
  - *Límites de peticiones (Rate limits):* Se implementó retardo controlado (`time.sleep(0.5)`) entre llamadas de paginación.
  - *Cambios de esquema en origen:* La capa `raw` en formato JSONB garantiza resiliencia ante adición o supresión de columnas en la API gubernamental.
  - *Sensibilidad de datos:* No se manejan datos personales sensibles (RUTs o personas naturales). Los campos identificatorios se anonimizan.
- **Escalabilidad Horizontal:** La arquitectura desacoplada permite incorporar nuevas fuentes del sector público (ej. Comisión para el Mercado Financiero, Banco Central de Chile) agregando nuevos DAGs independientes sin modificar el pipeline existente.
