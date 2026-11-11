# FLUJO DE EJECUCIÓN Y CICLO DE VIDA DEL DATO (ETL ADUANAS DE CHILE)
## Arquitectura de 4 Capas, Orquestación Desacoplada y Data Mart Dimensional
### Asignatura: INF-522 Gestión Informática II | Evaluación 2
**Firma Consultora:** DataCustoms Analytics SpA  
**Integrantes:** Leonel Ayala, Luis Barrios, Leonardo Mellado, Alan Troncoso, Héctor Valenzuela  
**Docentes:** Prof. Natalia Morales, Prof. Sebastián Farías  

---

## 1. Visión General del Pipeline End-to-End

El ciclo de vida del dato sigue un flujo estrictamente **unidireccional, reproducible, auditable e idempotente**, transitando a través de 4 capas de persistencia en PostgreSQL bajo la orquestación desacoplada de Apache Airflow 2.9:

```
 [Portal Oficial datos.gob.cl]
        │   (Peticiones HTTP GET paginadas con limit/offset)
        ▼
 ┌───────────────────────────────────────────────────────────────┐
 │ FASE 1: EXTRACCIÓN Y RATE LIMITING                            │
 │ • Validación previa (limit=1, timeout 15s)                    │
 │ • Captura y retroceso exponencial ante HTTP 429 (Retry-After)  │
 │ • Paginación en bloques de 500 registros con pausas de 0.5s   │
 │ • Archivo responsable: src/extract.py                         │
 └───────────────────────────────┬───────────────────────────────┘
                                 │
                                 ▼ (JSONB Crudo 1:1)
 ┌───────────────────────────────────────────────────────────────┐
 │ CAPA RAW (TRANSITORIA EN POSTGRESQL)                         │
 │ • raw.importaciones_raw / raw.exportaciones_raw               │
 │ • TRUNCATE al inicio de cada ejecución                        │
 │ • Almacenamiento íntegro del payload + metadatos forenses     │
 └───────────────────────────────┬───────────────────────────────┘
                                 │
                                 ▼ (Lectura en Memoria)
 ┌───────────────────────────────────────────────────────────────┐
 │ FASE 2: DEPURACIÓN, NORMALIZACIÓN Y QUALITY GATE              │
 │ • Circuit Breaker: Aborta si celdas nulas > 25.0% o 0 filas   │
 │ • Filtrado y descarte de registros sin fecha contable         │
 │ • Códigos de país deterministas con algoritmo CRC32           │
 │ • Marcado booleano de valores atípicos (es_atipico = TRUE)    │
 │ • Marcado de datos estimados (flete, seguro, peso, aduana)    │
 │ • Archivo responsable: src/transform.py                       │
 └───────────────────────────────┬───────────────────────────────┘
                                 │
                                 ▼ (Filas Limpias y Validadas)
 ┌───────────────────────────────────────────────────────────────┐
 │ CAPA STAGING (TRANSITORIA EN POSTGRESQL)                     │
 │ • staging.stg_operaciones_aduanas                             │
 │ • DELETE WHERE tipo_flujo al iniciar el lote                  │
 │ • Tipado estricto (NUMERIC, DATE, INT, BOOLEAN)               │
 └───────────────────────────────┬───────────────────────────────┘
                                 │
                                 ▼ (Carga Dimensional Kimball)
 ┌───────────────────────────────────────────────────────────────┐
 │ FASE 3: CARGA DIMENSIONAL IDEMPOTENTE                         │
 │ • Poblado de Dimensiones con ON CONFLICT DO NOTHING           │
 │ • Carga de Hechos con ON CONFLICT (codigo_operacion)          │
 │ • Integridad Referencial: ON DELETE RESTRICT                  │
 │ • Archivo responsable: src/load.py                            │
 └───────────────────────────────┬───────────────────────────────┘
                                 │
                                 ▼ (Persistencia Permanente)
 ┌───────────────────────────────────────────────────────────────┐
 │ CAPA DW: MODELO DIMENSIONAL ESTRELLA                         │
 │ • 4 Dimensiones conformadas: Tiempo, Aduana, País, Vía        │
 │ • 1 Fact Central: dw.fact_comercio_exterior                   │
 │ • Coexistencia de tiempos: fecha de negocio vs fecha de carga │
 └───────────────────────────────┬───────────────────────────────┘
                                 │
                                 ▼ (TriggerDagRunOperator)
 ┌───────────────────────────────────────────────────────────────┐
 │ FASE 4: CÁLCULO ANALÍTICO DEL DATA MART (SQL)                 │
 │ • Recálculo de los 5 KPIs de negocio en PostgreSQL            │
 │ • Archivo responsable: sql/02_indicadores_mart.sql            │
 │ • Orquestador: dags/dag_03_indicadores_mart.py                │
 └───────────────────────────────┬───────────────────────────────┘
                                 │
                                 ▼ (Tablas Agregadas de Negocio)
 ┌───────────────────────────────────────────────────────────────┐
 │ CAPA MART: INDICADORES ESTRATÉGICOS                          │
 │ • mart.kpi_01_balanza_comercial_aduana                        │
 │ • mart.kpi_02_incidencia_costo_logistico                      │
 │ • mart.kpi_03_concentracion_carga_aduanera                    │
 │ • mart.kpi_04_valor_agregado_tonelada                         │
 │ • mart.kpi_05_participacion_socios_comerciales                │
 └───────────────────────────────┬───────────────────────────────┘
                                 │
                                 ▼
 ┌───────────────────────────────────────────────────────────────┐
 │ FASE 5: AUDITORÍA Y OBSERVABILIDAD TRANSVERSAL                │
 │ • Inserción automática en public.etl_audit                    │
 │ • Registro de duración, filas, nulos, atípicos y fallas       │
 │ • Archivo responsable: src/audit.py                           │
 └───────────────────────────────┬───────────────────────────────┘
                                 │
                                 ▼
 ┌───────────────────────────────────────────────────────────────┐
 │ FASE 6: REPORTERÍA VISUAL Y DASHBOARD EN VIVO                 │
 │ • Generación de reporte HTML interactivo (Chart.js)           │
 │ • Archivo: scripts/generar_dashboard_html.py                  │
 └───────────────────────────────────────────────────────────────┘
```

---

## 2. Descripción Paso a Paso de Cada Fase del Flujo

---

### Fase 0: Inicialización y Gobernanza de Infraestructura
- **Archivos que intervienen:** `docker-compose.yml`, `.env`, `sql/01_ddl_esquemas.sql`, `scripts/init_airflow_resources.py`.
- **Qué ocurre paso a paso:**
  1. Docker Compose levanta los contenedores: `aduanas-postgres-dw` (mapeado al puerto host 5433 para evitar colisiones con instancias nativas de Windows), `aduanas-airflow-init`, `aduanas-airflow-webserver` y `aduanas-airflow-scheduler`.
  2. En el primer arranque, PostgreSQL ejecuta automáticamente `sql/01_ddl_esquemas.sql`, construyendo los 4 esquemas (`raw`, `staging`, `dw`, `mart`), las tablas relacionales con sus restricciones `ON DELETE RESTRICT` y la tabla `public.etl_audit`.
  3. El script `scripts/init_airflow_resources.py` conecta a la API interna de Airflow y provisiona la conexión `postgres_dw` (cifrada con `AIRFLOW_FERNET_KEY`) y las variables de configuración por fuente (`aduanas_api_url_*`, límites de paginación y umbrales de calidad).

---

### Fase 1: Validación y Extracción Paginada (Raw Ingestion)
- **Archivos que intervienen:** `dags/dag_01_etl_importaciones.py` (o `dag_02`), `src/extract.py`, `src/db.py`.
- **Qué ocurre paso a paso:**
  1. **Tarea `validar_api`:** Realiza un sondeo HTTP ultraliviano con `limit=1` y `timeout=15s` hacia el endpoint configurado en la variable de Airflow.
     - Si el endpoint responde HTTP 200 y `success=true`, la tarea finaliza exitosamente y da paso a la extracción.
     - Si la URL es inválida o el portal estatal está caído (como en la simulación de falla controlada), captura la excepción y ejecuta hasta 2 reintentos con intervalo de 15 segundos. Si la falla persiste, aborta el DAG y gatilla el callback de falla sin haber tocado la base de datos.
  2. **Tarea `extraer_a_raw`:**
     - Vacía la tabla de destino (`TRUNCATE TABLE raw.importaciones_raw`) para asegurar la transitoriedad y no acumular duplicados.
     - Inicia un bucle de paginación solicitando bloques de 500 registros (`aduanas_api_page_limit`).
     - Entre cada llamada HTTP, ejecuta una pausa de 0.5s para no gatillar bloqueos de IP por *Rate Limiting*.
     - **Control HTTP 429 (Too Many Requests):** Si el servidor de datos abiertos responde con código 429 por exceso de concurrencia, el código captura el evento, lee la cabecera `Retry-After` o aplica un algoritmo de retroceso exponencial (`backoff = 2^intento * 2s`), esperando pacientemente la liberación del servidor.
     - Inserta los registros en lotes mediante `execute_values`, guardando el JSON crudo en una columna de tipo nativo `JSONB` junto con el `run_id`, el número de página y la fecha de extracción.
     - Publica en XCom el conteo de `filas_leidas` (2.500) y el `total_disponible_api` (32.117 en DIN o 55.331 en DUS).

---

### Fase 2: Depuración, Normalización y Quality Gate (Staging)
- **Archivos que intervienen:** `dags/dag_01_etl_importaciones.py` (o `dag_02`), `src/transform.py`.
- **Qué ocurre paso a paso:**
  1. **Tarea `transformar_a_staging`:**
     - Extrae los registros JSONB de la capa `raw` transitoria y los carga en un DataFrame de pandas.
     - **Circuit Breaker (Disyuntor de Calidad):** Mide la proporción de celdas vacías sobre el lote. Si supera el 25.0% o si no existen filas útiles, lanza un `ValueError` inmediato, deteniendo el flujo para evitar contaminar el Data Warehouse.
     - Limpia la capa intermedia ejecutando `DELETE FROM staging.stg_operaciones_aduanas WHERE tipo_flujo = ...`.
     - **Conversión de Tipos:** Realiza casting estricto de textos numéricos (ej. montos con comas o puntos decimales) a `NUMERIC(18,2)` y valida fechas contables. Si un registro carece de año o mes legítimo, se descarta y se suma a `registros_rechazados`.
     - **Homologación de Aduana y País:** Mapea las regiones de exportación a las aduanas oficiales chilenas. En importaciones, dado que el catálogo público no desagrega el puerto físico, asigna `0: SIN INFORMACION` y activa `aduana_estimada = TRUE`. Para los países, genera códigos deterministas estables mediante el algoritmo hash CRC32 (`zlib.crc32`), impidiendo la creación de países duplicados como ocurría anteriormente con Paraguay.
     - **Detección de Valores Atípicos:** Evalúa el peso bruto frente al percentil 99.5 de referencia (> 1.000 t) o razones monetarias atípicas (< $0.01 USD/kg o > $100.000 USD/kg). Las declaraciones se conservan para no alterar la contabilidad, pero se marcan con la bandera `es_atipico = TRUE`.
     - **Banderas de Estimación:** Al no publicar la fuente pública los valores de flete y seguro individuales, aplica el estándar internacional proporcional (FOB 91%, flete 8%, seguro 1%) y activa obligatoriamente la bandera `montos_estimados = TRUE`. Si falta el peso, se estima y se activa `peso_estimado = TRUE`.
     - Inserta los datos validados en `staging.stg_operaciones_aduanas` y publica en XCom las métricas de calidad (`porcentaje_nulos`, `registros_atipicos`, `registros_rechazados`).

---

### Fase 3: Carga Dimensional Idempotente (Data Warehouse Kimball)
- **Archivos que intervienen:** `dags/dag_01_etl_importaciones.py` (o `dag_02`), `src/load.py`.
- **Qué ocurre paso a paso:**
  1. **Tarea `cargar_a_dw`:**
     - Lee los registros limpios de `staging.stg_operaciones_aduanas`.
     - Puebla las 4 dimensiones conformadas con cláusulas que impiden colisiones:
       - `dw.dim_tiempo`: Inserta las fechas contables con su jerarquía (`id_tiempo`, año, mes, nombre_mes, trimestre, semestre) con `ON CONFLICT (fecha) DO NOTHING`.
       - `dw.dim_aduana`, `dw.dim_pais`, `dw.dim_via_transporte`: Inserta nuevos catálogos con `ON CONFLICT DO NOTHING`.
     - Puebla la tabla de hechos central `dw.fact_comercio_exterior` cruzando las claves foráneas con las dimensiones.
     - **Idempotencia Estricta:** La inserción en la tabla de hechos utiliza la clave natural única `codigo_operacion`:
       ```sql
       ON CONFLICT (codigo_operacion) DO UPDATE SET
           monto_fob_usd = EXCLUDED.monto_fob_usd,
           monto_cif_usd = EXCLUDED.monto_cif_usd,
           costo_logistico_usd = EXCLUDED.costo_logistico_usd,
           fecha_carga_dw = CURRENT_TIMESTAMP;
       ```
       Si se vuelve a ejecutar el pipeline sobre el mismo periodo, el conteo total de filas no aumenta ni en una sola unidad; se actualizan las métricas y se renueva la fecha de carga técnica.
     - **Integridad Referencial Estricta:** Si alguien intentara eliminar una dimensión en `dw.dim_*` que esté siendo utilizada por un hecho, PostgreSQL aborta la operación gracias a la política `ON DELETE RESTRICT` definida en el DDL.
     - Envía por XCom el total de `filas_cargadas` (2.500 por flujo).

---

### Fase 4: Auditoría Forense y Enlace entre DAGs
- **Archivos que intervienen:** `dags/dag_01_etl_importaciones.py` (o `dag_02`), `src/audit.py`.
- **Qué ocurre paso a paso:**
  1. **Tarea `auditar_ejecucion`:** Extrae las métricas de XCom (`filas_leidas`, `filas_cargadas`, `porcentaje_nulos`, `registros_atipicos`, `registros_rechazados`) y calcula el tiempo transcurrido desde el inicio del DAG. Inserta una fila en `public.etl_audit` con `estado = 'SUCCESS'` y un mensaje de auditoría formateado.
  2. **Tarea `gatillar_mart_indicadores`:** Ejecuta un `TriggerDagRunOperator`, ordenándole a Airflow que inicie de inmediato la ejecución de `dag_03_indicadores_mart` para recalcular los indicadores de negocio.

---

### Fase 5: Recálculo Analítico del Data Mart (SQL)
- **Archivos que intervienen:** `dags/dag_03_indicadores_mart.py`, `sql/02_indicadores_mart.sql`.
- **Qué ocurre paso a paso:**
  1. **Tarea `recalcular_kpis_mart`:** Lee y ejecuta de forma transaccional el archivo `sql/02_indicadores_mart.sql` sobre PostgreSQL, recalculando de forma agregada las 5 tablas del esquema `mart`:
     - **KPI 01 (`mart.kpi_01_balanza_comercial_aduana`):** Calcula el saldo comercial (FOB - CIF) por aduana y mes. Si una aduana no registra importaciones (CIF = 0), asigna cobertura `NULL`, resolviendo la distorsión matemática que antes forzaba 100%.
     - **KPI 02 (`mart.kpi_02_incidencia_costo_logistico`):** Evalúa el ratio de flete + seguro sobre el valor CIF agrupado por vía de transporte.
     - **KPI 03 (`mart.kpi_03_concentracion_carga_aduanera`):** Cuantifica las toneladas físicas por puerto y evalúa el porcentaje nacional frente al umbral del 40%, excluyendo la aduana 0 del ranking físico.
     - **KPI 04 (`mart.kpi_04_valor_agregado_tonelada`):** Calcula la relación USD FOB / Tonelada métrica por socio comercial para clasificar exportaciones de alto valor versus materias primas brutas.
     - **KPI 05 (`mart.kpi_05_participacion_socios_comerciales`):** Calcula la cuota de intercambio sobre el total nacional por socio comercial, confirmando que ningún país supera el umbral de sobreexposición del 35%.
  2. **Tarea `registrar_auditoria_kpis`:** Inserta un registro en `public.etl_audit` confirmando la actualización exitosa de las 5 tablas analíticas.
  3. **Tarea `generar_dashboard_html_en_vivo`:** Invoca a `scripts/generar_dashboard_html.py`, el cual consulta las tablas `mart.*` y `dw.fact_comercio_exterior` y regenera automáticamente el archivo `reporte_estadisticas_aduanas.html` con gráficos dinámicos de Chart.js.

---

## 3. Resumen de Estados y Manejo de Contingencias

| Situación Operacional | Comportamiento del Sistema | Resultado en Base de Datos |
| :--- | :--- | :--- |
| **Flujo Normal (API Disponible)** | Validación exitosa, extracción en 5 páginas, limpieza, carga en DW y cálculo de 5 KPIs. | Filas cargadas en `dw.fact_comercio_exterior` y registro `SUCCESS` en `public.etl_audit`. |
| **API Caída / Error HTTP 400-500** | La tarea `validar_api` intercepta la falla, realiza 2 reintentos cada 15s y se detiene. | No se inserta basura en `raw` ni `dw`. Se inserta registro `FAILED` con traceback forense en `public.etl_audit`. |
| **API con HTTP 429 (Saturación)** | Detecta código 429, lee la cabecera `Retry-After` o duerme con backoff exponencial y reintenta. | Extracción exitosa sin abortar el proceso ni saturar el servidor público. |
| **Carga de Datos Degradados (> 25% Nulos)** | El Circuit Breaker de `src/transform.py` detiene la ejecución inmediatamente. | El Data Warehouse queda intacto; la falla queda documentada en auditoría. |
| **Re-ejecución / Reprocesamiento (Idempotencia)** | La cláusula `ON CONFLICT (codigo_operacion) DO UPDATE` actualiza métricas y timestamp. | Cero filas duplicadas; los KPIs reflejan la información actualizada. |

---

## 4. Trazabilidad de Limitaciones de Origen y Reglas Metodológicas

Para garantizar total transparencia ante los evaluadores del proyecto, el pipeline formaliza tres limitaciones estructurales de la API pública de Aduanas de Chile (`datos.gob.cl`):

1. **Ausencia de Aduana de Ingreso en Importaciones:**
   - **Causa:** El recurso `c2211d9e-4c8e-4ef4-84d5-87039c515584` (Declaración de Ingreso DIN 2026) solo publica 9 atributos consolidados a nivel nacional.
   - **Tratamiento en Pipeline:** Se asigna `cod_aduana = 0` (`0: SIN INFORMACION`) y se marca `aduana_estimada = TRUE`. Esto previene sesgar artificialmente puertos marítimos específicos y permite aislar operaciones consolidadas en el Data Mart.

2. **Cálculo de Flete y Seguro (Incidencia 9.00% en KPI 02):**
   - **Causa:** La API pública entrega el valor `USD CIF` pero no desglosa los componentes de flete marítimo/terrestre ni primas de seguro internacional.
   - **Tratamiento en Pipeline:** `src/transform.py` implementa el estándar internacional de comercio exterior de la CEPAL/Naciones Unidas ($91\%$ FOB, $8\%$ Flete, $1\%$ Seguro). Como consecuencia matemática:
     $$\text{Costo Logístico} = \text{Flete (8\%)} + \text{Seguro (1\%)} = 9\% \text{ del CIF}$$
     $$\text{Incidencia (\%)} = \frac{9\% \times \text{CIF}}{\text{CIF}} \times 100 = 9.00\%$$
   - **Gobernanza:** Cada fila resultante se persiste en `dw.fact_comercio_exterior` con la bandera `montos_estimados = TRUE`, transparentando que se trata de una estimación normativa y no de una factura de flete auditada.

3. **Manejo de Cobertura Indefinida en Puertos Exportadores (KPI 01):**
   - **Causa:** Puertos como Talcahuano registran $111.6M USD FOB de exportación en la muestra, pero $0 CIF de importación debido a la centralización nacional de la DIN.
   - **Tratamiento en Pipeline:** En `sql/02_indicadores_mart.sql`, la fórmula de cobertura aplica `CASE WHEN total_cif > 0 THEN (total_fob / total_cif) * 100 ELSE NULL END`. Se devuelve `NULL` (no evaluable / indefinido), erradicando el error de versiones preliminares que asignaban un arbitrario $100\%$.


