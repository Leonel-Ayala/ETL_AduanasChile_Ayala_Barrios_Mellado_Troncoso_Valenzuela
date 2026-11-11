# GUÍA TÉCNICA DE ARCHIVOS Y COMPONENTES DEL SISTEMA ETL
## Proyecto: Arquitectura de Ingeniería de Datos y Data Mart - Aduanas de Chile
### Asignatura: INF-522 Gestión Informática II | Evaluación 2
**Firma Consultora:** DataCustoms Analytics SpA  
**Integrantes:** Leonel Ayala, Luis Barrios, Leonardo Mellado, Alan Troncoso, Héctor Valenzuela  
**Docentes Evaluadores:** Prof. Natalia Morales, Prof. Sebastián Farías  

---

## 1. Introducción y Mapa Arquitectural

El proyecto implementa una arquitectura desacoplada, modular y resiliente de ingeniería de datos en **Apache Airflow 2.9 (Docker)** y **PostgreSQL 15**, estructurada bajo la metodología dimensional de **Ralph Kimball**.

El árbol de directorios del repositorio se organiza estrictamente por responsabilidades:

```text
ETL_AduanasChile_Ayala_Barrios_Mellado_Troncoso_Valenzuela/
├── dags/                           <- Definición de DAGs orquestadores en Airflow
│   ├── dag_01_etl_importaciones.py <- Pipeline independiente de Importaciones (DIN)
│   ├── dag_02_etl_exportaciones.py <- Pipeline independiente de Exportaciones (DUS)
│   └── dag_03_indicadores_mart.py  <- Pipeline analítico de KPIs SQL en Data Mart
├── src/                            <- Lógica de negocio y módulos de ingeniería
│   ├── __init__.py                 <- Inicializador de paquete Python
│   ├── db.py                       <- Conexión centralizada segura a PostgreSQL
│   ├── extract.py                  <- Validación HTTP, paginación, rate limiting y 429 backoff
│   ├── transform.py                <- Tipado, Circuit Breaker, atípicos, nulos y CRC32
│   ├── load.py                     <- Carga dimensional Kimball con ON CONFLICT DO UPDATE
│   └── audit.py                    <- Registro de auditoría forense y callbacks en public.etl_audit
├── sql/                            <- Scripts DDL y consultas de cálculo de KPIs
│   ├── 01_ddl_esquemas.sql         <- Creación de esquemas, tablas, PKs, FKs RESTRICT e índices
│   └── 02_indicadores_mart.sql     <- Cálculo SQL idempotente de los 5 KPIs del Data Mart
├── scripts/                        <- Utilidades de inicialización y reportería
│   ├── init_airflow_resources.py   <- Provisionamiento seguro de conexiones y variables Airflow
│   ├── descubrir_endpoints_aduanas.py <- Validador y descubridor de endpoints CKAN
│   ├── generar_dashboard_html.py   <- Generador dinámico del dashboard visual con Chart.js
│   └── reporte_estadisticas_aduanas.html <- Reporte visual generado
├── docker-compose.yml              <- Ecosistema multicontenedor (Webserver, Scheduler, DB)
├── Dockerfile                      <- Imagen personalizada con dependencias Python y OS
├── requirements.txt                <- Librerías Python necesarias
├── .env.example                    <- Plantilla de variables de entorno sin credenciales expuestas
├── .env                            <- Variables de entorno locales activas (puerto 5433, Fernet)
└── README.md                       <- Documentación técnica del proyecto y guía de reproducción
```

---

## 2. Detalle de Archivos: Responsabilidades, Entradas, Salidas e Impacto en el Flujo

---

### Módulo: `src/db.py`
- **Propósito:** Centralizar en un único punto de acceso todas las conexiones a PostgreSQL, garantizando **cero contraseñas expuestas en código**.
- **Cómo opera:**
  1. Si se ejecuta dentro del ecosistema de Airflow, detecta `PostgresHook` y solicita la conexión segura preconfigurada `postgres_conn_id="postgres_dw"`, cuyas credenciales están cifradas simétricamente con `AIRFLOW_FERNET_KEY`.
  2. Si se ejecuta en modo standalone fuera de Airflow, utiliza `psycopg2` leyendo obligatoriamente las variables de entorno `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD` y `POSTGRES_DB`. Si falta alguna, lanza un `RuntimeError` y prohíbe la ejecución con valores por defecto.
- **Entradas:** Conexión `postgres_dw` de Airflow o variables de entorno del sistema operativo.
- **Salidas:** Objeto de conexión nativo de PostgreSQL (`psycopg2.extensions.connection`).
- **Influencia en el flujo:** Es el cimiento de seguridad del proyecto. Si este archivo falla, ningún DAG ni script puede interactuar con la base de datos.

---

### Módulo: `src/extract.py`
- **Propósito:** Conectarse programáticamente a la API CKAN DataStore de `datos.gob.cl` y persistir los registros crudos en la capa `raw` de PostgreSQL.
- **Funciones principales:**
  - `validar_api_disponible(tipo_flujo, **context)`: Ejecuta una consulta ultraliviana (`limit=1`, `timeout=15s`) al endpoint para verificar código HTTP 200 y `success=true`. Si el endpoint no responde o devuelve error (como en la simulación de caída), lanza una excepción inmediata disparando la política de reintentos de Airflow sin corromper la base de datos.
  - `extraer_datos_api_a_raw(tipo_flujo, **context)`: 
    1. Lee las variables parametrizadas en Airflow (`aduanas_api_url_*`, `aduanas_resource_id_*`, `aduanas_api_page_limit=500`, `aduanas_api_max_pages=5`).
    2. Aplica **Rate Limiting Preventivo** con pausas técnicas de 0.5s entre llamadas para no saturar los servidores del Estado.
    3. Implementa **Manejo Avanzado de HTTP 429 (Too Many Requests)**: Lee la cabecera `Retry-After` enviada por el servidor o aplica retroceso exponencial (`backoff * 2^intento`).
    4. Trunca la tabla transitoria correspondiente (`raw.importaciones_raw` o `raw.exportaciones_raw`) para no acumular basura de ejecuciones anteriores.
    5. Inserta los registros en lotes usando `execute_values`, preservando el payload JSONB íntegro junto con metadatos: `run_id`, `offset_pagina`, `resource_id` y `fecha_extraccion`.
    6. Envía métricas por XCom: total de filas leídas (`filas_leidas`) y total disponible en la API (`total_disponible_api`).
- **Entradas:** Variables de Airflow y respuesta JSON de la API CKAN.
- **Salidas:** Filas JSONB en `raw.importaciones_raw` o `raw.exportaciones_raw`, y claves XCom para la auditoría.
- **Influencia en el flujo:** Garantiza que la ingestión sea tolerante a fallas de red, respete los límites de uso del servidor público y preserve la evidencia forense cruda 1:1.

---

### Módulo: `src/transform.py`
- **Propósito:** Transformar el payload JSONB crudo de la capa `raw`, validar la calidad mediante un *Circuit Breaker*, tipar campos, homologar nomenclaturas y persistir en `staging.stg_operaciones_aduanas`.
- **Reglas clave implementadas:**
  1. **Circuit Breaker / Quality Gate:** Calcula el porcentaje de celdas nulas o vacías sobre el lote crudo. Si supera el umbral parametrizado (`aduanas_max_pct_nulos_permitido = 25.0%`) o si la cantidad de registros procesables es 0, detiene el pipeline inmediatamente con un `ValueError`, protegiendo el Data Warehouse de datos corruptos.
  2. **Validación Estricta de Fechas:** Valida año y mes numéricos legítimos. Si una declaración carece de fecha válida, se rechaza y se contabiliza en `registros_rechazados`.
  3. **Homologación de Aduanas:** En exportaciones mapea la región oficial a la aduana nacional (Talcahuano, San Antonio, Metropolitana, Valparaíso, etc.). En importaciones, dado que el dataset de datos.gob.cl no desagrega el puerto de entrada, asigna `0: SIN INFORMACION` y marca `aduana_estimada = TRUE`, eliminando cualquier asignación ficticia por turnos.
  4. **Códigos de País Deterministas (Hash CRC32):** Para países fuera del catálogo base, calcula un código numérico estable mediante `zlib.crc32(nombre.encode('utf-8'))`, garantizando que socios como Paraguay reciban siempre el mismo identificador único en todas las corridas.
  5. **Detección de Valores Atípicos (Outliers):** Identifica registros con peso unitario superior al percentil 99.5 (> 1.000 toneladas) o relaciones monetarias inconsistentes (< $0.01 o > $100.000 USD/kg). Las filas se conservan para no alterar la contabilidad pero se marcan con la bandera booleana `es_atipico = TRUE`.
  6. **Tratamiento Declarado de Estimaciones:** Al no desglosar la API de importaciones el flete y seguro, aplica el coeficiente internacional estándar CIF (FOB 91%, flete 8%, seguro 1%) y marca `montos_estimados = TRUE`. Si falta el peso físico, se estima por densidad monetaria y se marca `peso_estimado = TRUE`.
- **Entradas:** Tabla `raw.importaciones_raw` o `raw.exportaciones_raw`.
- **Salidas:** Registros limpios en `staging.stg_operaciones_aduanas`, y métricas de nulos, atípicos y rechazados en XCom.
- **Influencia en el flujo:** Es el filtro de calidad del ecosistema; impide la entrada de basura analítica y etiqueta formalmente las estimaciones.

---

### Módulo: `src/load.py`
- **Propósito:** Poblar de forma idempotente las 4 dimensiones conformadas y la tabla de hechos central en el esquema `dw`.
- **Mecanismo:**
  1. Puebla `dw.dim_tiempo` con la jerarquía temporal (año, mes, nombre de mes, trimestre, semestre) usando `ON CONFLICT (fecha) DO NOTHING`.
  2. Puebla `dw.dim_aduana`, `dw.dim_pais` y `dw.dim_via_transporte` asegurando identificadores únicos.
  3. Realiza la carga dimensional sobre `dw.fact_comercio_exterior` cruzando las claves subrogadas mediante `JOIN` sobre las dimensiones.
  4. **Idempotencia Estricta:** Aplica `ON CONFLICT (codigo_operacion) DO UPDATE SET monto_fob_usd = EXCLUDED.monto_fob_usd, monto_cif_usd = EXCLUDED.monto_cif_usd, costo_logistico_usd = EXCLUDED.costo_logistico_usd, fecha_carga_dw = CURRENT_TIMESTAMP`. Re-ejecutar el pipeline 100 veces actualiza las filas pero jamás duplica registros.
  5. Mapea fielmente las banderas booleanas de auditoría: `montos_estimados`, `peso_estimado`, `aduana_estimada`, `es_atipico`.
- **Entradas:** `staging.stg_operaciones_aduanas`.
- **Salidas:** `dw.dim_*` y `dw.fact_comercio_exterior`.
- **Influencia en el flujo:** Garantiza la persistencia histórica en el modelo estrella Kimball y la estricta integridad referencial con `ON DELETE RESTRICT`.

---

### Módulo: `src/audit.py`
- **Propósito:** Centralizar la observabilidad, métricas de calidad y registro forense de fallas en la tabla `public.etl_audit`.
- **Funciones:**
  - `registrar_auditoria_exito(fuente, **context)`: Se ejecuta al final de cada pipeline exitoso. Extrae las métricas de XCom (`filas_leidas`, `filas_cargadas`, `porcentaje_nulos`, `registros_atipicos`, `registros_rechazados`), calcula la duración exacta en segundos e inserta un registro con `estado = 'SUCCESS'` y un mensaje descriptivo auditado.
  - `registrar_falla_callback(context)`: Callback automático de Airflow (`on_failure_callback`). Se dispara cuando una tarea agota sus reintentos. Captura el `dag_id`, `run_id`, `logical_date` y el traceback exacto de la excepción, insertando una fila forense en `public.etl_audit` con `estado = 'FAILED'`.
- **Entradas:** Contexto de ejecución de Airflow (`ti`, `dag_run`, `exception`).
- **Salidas:** Registros auditables en `public.etl_audit`.
- **Influencia en el flujo:** Provee trazabilidad total ante auditorías y permite demostrar en vivo la tolerancia a fallas.

---

### DAG: `dags/dag_01_etl_importaciones.py`
- **Propósito:** Orquestar el flujo end-to-end de Declaraciones de Ingreso (DIN) de Aduanas de Chile.
- **Secuencia de Tareas:**
  `validar_api` -> `extraer_a_raw` -> `transformar_a_staging` -> `cargar_a_dw` -> `auditar_ejecucion` -> `gatillar_mart_indicadores`.
- **Gatillador:** Al culminar exitosamente, utiliza `TriggerDagRunOperator` para ejecutar automáticamente `dag_03_indicadores_mart`.

---

### DAG: `dags/dag_02_etl_exportaciones.py`
- **Propósito:** Orquestar el flujo end-to-end de Declaraciones Únicas de Salida (DUS) de Aduanas de Chile.
- **Secuencia de Tareas:**
  `validar_api` -> `extraer_a_raw` -> `transformar_a_staging` -> `cargar_a_dw` -> `auditar_ejecucion` -> `gatillar_mart_indicadores`.
- **Aislamiento:** Es 100% independiente de `dag_01`. Si la API de importaciones cae, exportaciones sigue operando sin interrupción.

---

### DAG: `dags/dag_03_indicadores_mart.py`
- **Propósito:** Ejecutar el recálculo analítico de los 5 KPIs de negocio sobre el esquema `mart` en PostgreSQL y regenerar el reporte visual.
- **Secuencia de Tareas:**
  `recalcular_kpis_mart` -> `registrar_auditoria_kpis` -> `generar_dashboard_html_en_vivo`.
- **Características:** No depende de internet ni de APIs externas; trabaja 100% en SQL sobre los datos ya cargados en el DW.

---

### Script DDL: `sql/01_ddl_esquemas.sql`
- **Propósito:** Script idempotente que define la estructura relacional completa de la base de datos `aduanas_dw`.
- **Estructura que crea:**
  - Esquemas: `raw`, `staging`, `dw`, `mart`.
  - Tablas transitorias: `raw.importaciones_raw`, `raw.exportaciones_raw`, `staging.stg_operaciones_aduanas`.
  - Dimensiones Kimball: `dw.dim_tiempo`, `dw.dim_aduana`, `dw.dim_pais`, `dw.dim_via_transporte`.
  - Tabla de Hechos: `dw.fact_comercio_exterior` con claves foráneas protegidas mediante `ON DELETE RESTRICT`.
  - Tablas de Data Mart: `mart.kpi_01` a `mart.kpi_05` con restricciones de unicidad para soporte de inserciones idempotentes.
  - Tabla de Auditoría: `public.etl_audit`.

---

### Script SQL: `sql/02_indicadores_mart.sql`
- **Propósito:** Contiene las consultas de cálculo analítico de los 5 KPIs del Data Mart ejecutadas por el DAG 03.
- **Cálculos que realiza:**
  - KPI 01: Balanza Comercial mensual por aduana (FOB - CIF) y Cobertura (con manejo de cobertura `NULL` cuando CIF = 0).
  - KPI 02: Incidencia del Costo Logístico sobre Valor CIF (Flete + Seguro / CIF) por modo de transporte.
  - KPI 03: Concentración de Carga Aduanera (% de toneladas por aduana, excluyendo aduana 0 del ranking físico).
  - KPI 04: Densidad de Valor Económico por Tonelada (USD/Ton) por país socio.
  - KPI 05: Participación y Dependencia por Socio Comercial (% del intercambio nacional total).

---

### Script: `scripts/generar_dashboard_html.py`
- **Propósito:** Consultar en vivo las tablas pre-agregadas en el Data Mart (`mart.*`) y las métricas nacionales de `dw.fact_comercio_exterior`, generando un reporte visual HTML autocontenido con gráficos dinámicos de Chart.js.
- **Mejoras implementadas:**
  - Agrupa las aduanas oficiales chilenas en el KPI 01 mostrando claramente los puertos reales del país (Talcahuano, San Antonio, Valparaíso, Metropolitana, etc.) y una fila explicativa para operaciones centralizadas.
  - Muestra la incidencia logística del KPI 02 a nivel nacional consolidado por vía de transporte (Marítima y Terrestre).
  - Consulta los totales nacionales directamente desde la tabla de hechos, garantizando concordancia con las cifras consolidadas.
  - Reemplazo de `print` por logging estructurado de Python.

---

### Script: `scripts/init_airflow_resources.py`
- **Propósito:** Inicializar de forma automatizada las conexiones y variables dentro de Apache Airflow tras levantar los contenedores Docker.
- **Acciones:**
  - Crea la conexión `postgres_dw` leyendo las credenciales desde las variables de entorno del contenedor (sin contraseñas hardcodeadas).
  - Crea las variables independientes por fuente: `aduanas_api_url_importaciones`, `aduanas_api_url_exportaciones`, `aduanas_resource_id_importaciones`, `aduanas_resource_id_exportaciones`, `aduanas_api_page_limit` (500) y `aduanas_api_max_pages` (5).

---

## 3. Matriz de Trazabilidad entre Archivos y Requisitos de Pauta

| Requisito de la Rúbrica E2 | Archivos Responsables en el Repositorio | Mecanismo Técnico |
| :--- | :--- | :--- |
| **Consumo exclusivo vía API REST** | `src/extract.py`, `dags/dag_01`, `dags/dag_02` | Peticiones HTTP GET estructuradas a CKAN DataStore (`limit`, `offset`). |
| **Límite de uso y tasa de consultas** | `src/extract.py` | Rate limiting de 0.5s y manejo de HTTP 429 con cabecera `Retry-After` y retroceso exponencial. |
| **Tratamiento de nulos y atípicos** | `src/transform.py`, `sql/01_ddl_esquemas.sql` | Circuit Breaker (umbral 25%), marcas `es_atipico` y banderas de estimación booleanas. |
| **Modelo dimensional en estrella** | `sql/01_ddl_esquemas.sql`, `src/load.py` | 4 Dimensiones conformadas + 1 Fact central con `ON DELETE RESTRICT`. |
| **Idempotencia y contraste histórico** | `src/load.py`, `sql/02_indicadores_mart.sql` | `ON CONFLICT (codigo_operacion) DO UPDATE` y separación de tiempos (`fecha` vs `fecha_carga_dw`). |
| **Desacoplamiento de DAGs** | `dags/dag_01`, `dag_02`, `dag_03` | 3 DAGs independientes vinculados por `TriggerDagRunOperator`. |
| **Cero contraseñas expuestas** | `src/db.py`, `docker-compose.yml`, `.env.example` | Conexión centralizada, variables de entorno sin defaults y cifrado Fernet activo. |
| **Tolerancia a fallas y auditoría** | `src/audit.py`, `src/extract.py`, `dags/*` | Políticas de 2 reintentos con 15s de espera, `on_failure_callback` y tabla `public.etl_audit`. |
| **Data Mart con 5 KPIs de negocio** | `sql/02_indicadores_mart.sql`, `dags/dag_03` | 5 tablas dedicadas en esquema `mart` con fórmulas agregadas en SQL. |

---

## 4. Preguntas Técnicas Frecuentes y Justificaciones de Auditoría (Defensa E2)

Esta sección reúne las justificaciones metodológicas ante preguntas desafiantes de la comisión evaluadora sobre los datos, garantizando que el equipo hable con una sola voz técnica sustentada en la base de datos real:

### 1. ¿Por qué la API pública de importaciones no trae aduana de ingreso y la de exportaciones sí?
- **Realidad de la fuente oficial:** En el portal [datos.gob.cl](https://datos.gob.cl), el Servicio Nacional de Aduanas publica dos datasets con granularidades distintas:
  - **Exportaciones (`f6fe6014-...`):** Incluye el campo `Region origen` (ej. Biobío, Valparaíso, Metropolitana), lo que permite homologar con certeza a aduanas físicas chilenas (Talcahuano, Valparaíso, Metropolitana, San Antonio, etc.).
  - **Importaciones (`c2211d9e-...`):** Es una tabla resumida a nivel nacional (DIN) que sólo contiene 9 columnas: `_id`, `Mes`, `Anio`, `Unidad`, `USD CIF`, `Volumen`, `Producto`, `Pais origen`, `Codigo producto`. No incluye aduana de ingreso (`ADU`), flete, seguro ni valor FOB.
- **Decisión de Arquitectura y Gobernanza:** En lugar de inventar puertos ficticios (lo que violaría el principio de veracidad del dato), el pipeline asigna honestamente la categoría formal `0: SIN INFORMACION` y marca la bandera booleana `aduana_estimada = TRUE`. Esto permite a los analistas filtrar o separar las declaraciones con asignación directa de aquellas consolidadas nacionalmente.

### 2. ¿Por qué el KPI 02 (Incidencia Costo Logístico sobre Valor CIF) da exactamente 9.00%?
- **Origen de la cifra:** Como la API de importaciones entrega únicamente el valor `USD CIF` sin desagregar flete ni seguro, el módulo `src/transform.py` aplica el estándar internacional de comercio exterior de la CEPAL y Naciones Unidas:
  - $\text{FOB} = 91\% \times \text{CIF}$
  - $\text{Flete} = 8\% \times \text{CIF}$
  - $\text{Seguro} = 1\% \times \text{CIF}$
  - $\text{Costo Logístico} = \text{Flete} + \text{Seguro} = 8\% + 1\% = 9\% \times \text{CIF}$
- **Cálculo del KPI:** $\frac{\text{Costo Logístico}}{\text{CIF}} \times 100 = \frac{0.09 \times \text{CIF}}{\text{CIF}} \times 100 = 9.00\%$.
- **Gobernanza:** Esta estimación no se oculta; se registra formalmente en `dw.fact_comercio_exterior` con la bandera `montos_estimados = TRUE`.

### 3. ¿Por qué antes el gráfico de KPI 02 mostraba 6 barras con una barra morada aérea de 14.2%?
- **Causa raíz:** En la versión preliminar del script `scripts/generar_dashboard_html.py`, cuando no se conectaba a la base de datos o se probaba en entornos locales, la función `obtener_datos_fallback()` inyectaba datos simulados hardcodeados (Metropolitana Aérea 14.2%, San Antonio 6.79%, Valparaíso 6.76%, etc.).
- **Estado actual:** El dashboard actual consulta en vivo y de forma 100% verídica la base de datos PostgreSQL (`mart.kpi_02_incidencia_costo_logistico`), reflejando la realidad matemática del Data Mart sin datos ficticios en memoria.

### 4. ¿Por qué en KPI 02 no aparece la vía aérea en las importaciones actuales?
- **Regla de asignación de vías:** En `src/transform.py`, las operaciones de importación asignan vía de transporte según país de origen (países limítrofes a Terrestre, ultramar a Marítima). La asignación a vía Aérea requería aduana Metropolitana (`cod_aduana == 48`). Como las importaciones llegan centralizadas bajo `cod_aduana = 0` (Sin Información), quedan distribuidas entre Marítima y Terrestre, lo que honestamente reporta 0 registros aéreos identificables en esta muestra abierta.

### 5. ¿Por qué en KPI 01 Talcahuano muestra Tasa de Cobertura `NULL` en vez de 100%?
- **Corrección matemática:** La tasa de cobertura se define como: $\frac{\text{Exportaciones FOB}}{\text{Importaciones CIF}} \times 100$.
- En puertos puramente exportadores dentro de la muestra (como Talcahuano, que tiene $111.6M FOB y $0 CIF importado), dividir por cero es matemáticamente indefinido.
- El código antiguo forzaba artificialmente un `100%` con un `CASE WHEN importaciones = 0 THEN 100`. En la versión final se corrigió en `sql/02_indicadores_mart.sql` para devolver `NULL`, ya que un puerto con cero importaciones no tiene cobertura financiera local, sino que es un nodo netamente de salida.

### 6. ¿Por qué antes San Antonio tenía el 35.8% de carga y ahora Talcahuano lidera con el 25.41%?
- **Eliminación de sesgo artificial:** El código preliminar incluía una regla ficticia por turnos (`cod_aduana = 34 if idx % 2 == 0 else 18`), asignando de forma alternada la mitad de los registros sin aduana a San Antonio.
- Al erradicar dicha imputación artificial en `src/transform.py`, agrupar los registros sin puerto en la aduana 0 y calcular el ranking físico real sobre aduanas conocidas en `mart.kpi_03_concentracion_carga_aduanera`, Talcahuano emerge legítimamente como el nodo físico de mayor volumen con 322.145 toneladas reales verificadas.


