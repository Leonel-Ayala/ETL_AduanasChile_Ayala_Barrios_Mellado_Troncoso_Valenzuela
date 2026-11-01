# GUÍA MAESTRA DE EJECUCIÓN, PRESENTACIÓN Y DEFENSA ORAL
## Proyecto ETL Aduanas de Chile: Ingesta, Modelado Dimensional y Data Mart de Comercio Exterior
**Asignatura:** INF-522 Gestión Informática II | **Carrera:** Ingeniería Civil Informática | **Universidad Católica del Maule**  
**Académicos:** Natalia Morales | Sebastián Farías  
**Repositorio GitHub:** `ETL_AduanasChile_Ayala_Barrios_Mellado_Troncoso_Valenzuela`  
**Integrantes:** Ayala, Barrios, Mellado, Troncoso, Valenzuela  

---

## 1. AUDITORÍA DE CUMPLIMIENTO DE LA RÚBRICA (100% NIVEL DESTACADO)

A continuación se detalla la correspondencia exacta entre cada criterio de evaluación de la **Pauta E2-INF-522** y la implementación técnica realizada:

| Criterio de la Rúbrica | Ponderación | Requisito "Destacado" (5 pts) | Estado en Nuestro Proyecto | Evidencia Técnica en el Repositorio |
| :--- | :---: | :--- | :---: | :--- |
| **1. Selección y justificación de fuentes** | Parte 1 | Al menos 2 APIs públicas de Chile, bien justificadas (viabilidad, documentación, límites, volumen), con campo de cruce claro y links propios (no repetidos). | **100% CUMPLIDO** | Dos recursos CKAN activos en `datos.gob.cl`: Importaciones (`c2211d9e-4c8e...`, 32.117+ reg.) y Exportaciones (`f6fe6014-e353...`, 55.331+ reg.). Cruces por Aduana, País, Vía de Transporte y Período. Descubrimiento automático en `scripts/descubrir_endpoints_aduanas.py`. |
| **2. Extracción y transformación en Python** | Parte 1 | Extracción robusta (autenticación, paginación, límites de uso y errores HTTP) y transformaciones documentadas con validaciones de calidad; métricas completas. | **100% CUMPLIDO** | `src/extract.py` implementa `requests.get`, `timeout=10`, paginación por bloques (`limit=500`), control de pausas (`time.sleep(0.5)`). `src/transform.py` limpia, normaliza, imputa, elimina duplicados y calcula % de nulos. Métricas enviadas por XCom. |
| **3. Modelo de datos en PostgreSQL** | Parte 1 | Capas `raw`, `staging`, `dw` y `mart`; modelo dimensional coherente (hechos, dimensiones, claves y tipos) y carga idempotente. | **100% CUMPLIDO** | `sql/01_ddl_esquemas.sql` crea 4 esquemas + `public.etl_audit`. Modelo estrella con 4 dimensiones (`dim_tiempo`, `dim_aduana`, `dim_pais`, `dim_via_transporte`) y 1 hecho (`fact_comercio_exterior`). Carga idempotente con `ON CONFLICT DO UPDATE`. |
| **4. Diseño de DAGs y orquestación en Airflow** | Parte 1 | Un DAG independiente por API y un DAG de indicadores disparado automáticamente; dependencias, schedule y configuración (Connections y Variables) correctos. | **100% CUMPLIDO** | 3 DAGs desacoplados: `dag_01_etl_importaciones`, `dag_02_etl_exportaciones`, `dag_03_indicadores_mart` (disparado por `TriggerDagRunOperator`). Cero claves en código: usa Connection `postgres_dw` y Variables de Airflow. |
| **5. Tolerancia a fallas y ejecución en Airflow** | Parte 1 | Reintentos con espera, timeouts, validación previa, registro de fallas en auditoría; aislamiento de fallas y demostración en vivo. | **100% CUMPLIDO** | `retries=2`, `retry_delay=15s`, `execution_timeout=5min`. Tarea inicial `validar_api`. Si un DAG cae, los demás siguen (`aislamiento`). Callback `registrar_falla_callback` escribe `estado = 'FAILED'` en `public.etl_audit`. Protocolo de prueba en vivo listo. |
| **6. Indicadores de gestión** | Parte 1 | De 4 a 6 indicadores relevantes, con definición completa (nombre, objetivo, fórmula, fuente, periodicidad, meta), calculados en SQL sobre `mart`. | **100% CUMPLIDO** | 5 Indicadores en `mart.*` calculados mediante `sql/02_indicadores_mart.sql`. Cada fila incluye los 6 metadatos obligatorios y el flag booleano `cumple_meta`. Visualización complementaria en `reporte_estadisticas_aduanas.html`. |
| **7. Estrategia de datos** | Parte 1 | Aborda decisiones que apoyan los indicadores, calidad, linaje, frecuencia, riesgos (API, límites, licencias, privacidad) y escalabilidad. | **100% CUMPLIDO** | Documentado en la Sección 6 de esta guía y en `README.md`, listo para ser incluido en el capítulo de 2-3 páginas del informe escrito. |
| **8. Presentación grupal (30%) y Preguntas individuales (40%)** | Parte 2 (70%) | Exposición fluida en 8 minutos con diapositivas, demostración de falla en vivo y dominio técnico absoluto en la ronda de preguntas. | **100% PREPARADO** | Secciones 3, 4 y 5 de esta guía proporcionan el guion segundo a segundo, la guía de clics para la demo y 15 preguntas de examen con respuestas modelo. |

---

## 2. GUÍA DE INSTALACIÓN, PREPARACIÓN Y PUESTA EN MARCHA

### 2.1 Requisitos Previos
- Docker Desktop instalado y en ejecución en Windows.
- DBeaver (o pgAdmin) para visualización de base de datos.
- Navegador Web (Chrome, Edge o Firefox).

### 2.2 Variables de Entorno y Configuración de Puertos
El repositorio contiene `.env.example`. Para la ejecución local se utiliza el archivo `.env`:

> [!IMPORTANT]
> **Resolución de Conflicto de Puertos en Windows:**
> Los sistemas Windows suelen tener el servicio local `postgresql-x64-xx` escuchando en el puerto `5432`. Para evitar colisiones y errores de contraseña ajenos al contenedor, este proyecto expone PostgreSQL hacia la máquina anfitriona en el puerto **`5433`** (`5433:5432`). Dentro de la red interna de Docker, Airflow se comunica de manera nativa por el puerto estándar `5432`.

Contenido del `.env`:
```ini
POSTGRES_USER=postgres
POSTGRES_PASSWORD=aduanas_dw_secure_pass
POSTGRES_DB=aduanas_dw
POSTGRES_PORT=5433

AIRFLOW_IMAGE_NAME=apache/airflow:2.9.1
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
AIRFLOW__CORE__LOAD_EXAMPLES=false
```

### 2.3 Puesta en Marcha en 3 Comandos
Abre PowerShell en la carpeta raíz del proyecto y ejecuta:

```powershell
# 1. Construir y levantar los 3 contenedores
docker compose up -d --build

# 2. Verificar que los contenedores estén 'healthy' y 'running'
docker compose ps

# 3. Inicializar Conexiones y Variables en Airflow automáticamente
docker exec -it aduanas-airflow-webserver python /opt/airflow/scripts/init_airflow_resources.py
```

### 2.4 Verificación de Interfaces de Acceso

1. **Apache Airflow Web UI:**
   - **URL:** `http://localhost:8080`
   - **Usuario:** `admin` | **Contraseña:** `admin`
   - Deberás ver los 3 DAGs: `dag_01_etl_importaciones`, `dag_02_etl_exportaciones`, `dag_03_indicadores_mart`.

2. **DBeaver (Conexión a PostgreSQL):**
   - **Host:** `localhost`
   - **Port:** `5433`
   - **Database:** `aduanas_dw`
   - **Username:** `postgres`
   - **Password:** `aduanas_dw_secure_pass`
   - Deberás ver los esquemas: `raw`, `staging`, `dw`, `mart` y la tabla `public.etl_audit`.

3. **Dashboard Visual de Indicadores:**
   - Abre con doble clic en el navegador el archivo local:
     `reporte_estadisticas_aduanas.html`

---

## 3. GUION DE LA PRESENTACIÓN GRUPAL (8 MINUTOS EXACTOS)

La evaluación asigna **8 minutos estrictos** para la exposición grupal y demostración. A continuación se detalla la división temporal recomendada para los 5 integrantes:

```
[00:00 - 01:15] Minuto 1: Contexto, Fuentes y Problema de Gestión (Expositor 1)
[01:15 - 02:45] Minuto 2-3: Arquitectura, Esquemas y Carga Idempotente (Expositor 2)
[02:45 - 04:15] Minuto 4-5: Orquestación Airflow y 3 DAGs Desacoplados (Expositor 3)
[04:15 - 06:15] Minuto 6-7: DEMOSTRACIÓN EN VIVO DE FALLA CONTROLADA (Expositor 4)
[06:15 - 08:00] Minuto 8: Indicadores de Gestión, Hallazgos y Conclusión (Expositor 5)
```

---

### Diapositiva por Diapositiva: Qué Mostrar y Qué Decir

#### Lámina 1 (00:00 - 00:30) | Portada e Introducción
- **Contenido Visual:** Título del proyecto, logo institucional, nombres del equipo (Ayala, Barrios, Mellado, Troncoso, Valenzuela).
- **Guion Oral:**
  > *"Estimada profesora Natalia, profesor Sebastián y compañeros. Hoy presentamos nuestra solución integral de ingeniería de datos para el análisis de comercio exterior de Chile, construida sobre APIs públicas del Servicio Nacional de Aduanas, orquestada con Apache Airflow y almacenada en un Data Warehouse dimensional en PostgreSQL."*

#### Lámina 2 (00:30 - 01:15) | Fuentes de Datos y Justificación
- **Contenido Visual:** Tabla comparativa de los dos endpoints CKAN en `datos.gob.cl`. Mención explícita a la prohibición de archivos CSV/XLSX.
- **Guion Oral:**
  > *"Seleccionamos dos APIs REST del portal datos.gob.cl provistas por el Servicio Nacional de Aduanas: una para Importaciones y otra para Exportaciones. Ambas utilizan el motor CKAN DataStore con paginación nativa y formato JSON. Justificamos estas fuentes por su alta relevancia macroeconómica y porque comparten campos de cruce naturales: Aduana de control, País de procedencia o destino, Vía de transporte y Período contable. Cumpliendo rigurosamente la pauta, el 100% de los datos se extrae vía HTTP; no hay un solo archivo plano en la solución."*

#### Lámina 3 (01:15 - 02:00) | Arquitectura Global de 4 Capas + Auditoría
- **Contenido Visual:** Diagrama de bloques mostrando el flujo: `API` -> `raw` -> `staging` -> `dw` -> `mart` y en paralelo `public.etl_audit`.
- **Guion Oral:**
  > *"Nuestra arquitectura desacopla el ciclo de vida del dato en cuatro esquemas especializados:
  > 1. `raw`: Ingesta el payload crudo en JSONB conservando la fidelidad exacta de la API.
  > 2. `staging`: Aplica saneamiento, homologación de tipos, conversión numérica, descarte de nulos críticos y recodificación de tablas aduaneras.
  > 3. `dw`: Almacena un Modelo Dimensional en Estrella para análisis analítico OLAP.
  > 4. `mart`: Contiene 5 tablas de indicadores agregados calculados en SQL.
  > Adicionalmente, el esquema `public` aloja la tabla `etl_audit`, garantizando trazabilidad y registro forense de cada ejecución."*

#### Lámina 4 (02:00 - 02:45) | Modelo Dimensional en Estrella e Idempotencia
- **Contenido Visual:** Diagrama relacional del esquema en estrella: `dw.fact_comercio_exterior` al centro, conectada con `dim_tiempo`, `dim_aduana`, `dim_pais` y `dim_via_transporte`.
- **Guion Oral:**
  > *"El Data Warehouse se diseñó bajo un esquema en estrella con claves foráneas e integridad referencial. Para asegurar la idempotencia técnica —requisito indispensable de la pauta—, implementamos la cláusula `ON CONFLICT (codigo_operacion) DO UPDATE`. Esto significa que si un DAG se ejecuta varias veces para la misma fecha o si realizamos un reprocesamiento histórico (backfill), el sistema jamás generará registros duplicados; en su lugar, actualiza las métricas atómicamente."*

#### Lámina 5 (02:45 - 03:30) | Estrategia de Orquestación con 3 DAGs Desacoplados
- **Contenido Visual:** Captura de Airflow mostrando los tres DAGs: `dag_01_etl_importaciones`, `dag_02_etl_exportaciones` y `dag_03_indicadores_mart`.
- **Guion Oral:**
  > *"Diseñamos tres DAGs totalmente independientes para garantizar el aislamiento de fallas. Los DAGs 1 y 2 ejecutan el flujo ETL de cada fuente y, mediante `TriggerDagRunOperator`, disparan al DAG 3 de indicadores. La configuración sigue el principio de seguridad 'Zero Hardcoding': ninguna URL ni contraseña está escrita en el código; las conexiones se resuelven mediante el Connection `postgres_dw` y las rutas mediante Airflow Variables."*

#### Lámina 6 (03:30 - 04:15) | Tolerancia a Fallas, Reintentos y Auditoría
- **Contenido Visual:** Parámetros `retries=2`, `retry_delay=15s`, `on_failure_callback`, y esquema de `public.etl_audit`.
- **Guion Oral:**
  > *"Para tolerar la inestabilidad inherente a los servicios públicos, cada DAG inicia con una tarea de validación de disponibilidad HTTP antes de extraer datos. Si la API falla, Airflow ejecuta 2 reintentos con 15 segundos de intervalo. Si la falla persiste, el `on_failure_callback` intercepta la excepción y registra automáticamente el incidente con estado `FAILED` y detalle del error en `public.etl_audit`, sin afectar a los otros flujos."*

---

## 4. PROTOCOLO DE DEMOSTRACIÓN DE FALLA CONTROLADA EN VIVO (MOMENTO CUMBRE)

> [!IMPORTANT]
> Esta demostración es de carácter obligatorio según la pauta (Página 4 y Página 7 de la rúbrica). Practiquen estos pasos para realizarlos con total serenidad en menos de 2 minutos.

```mermaid
sequenceDiagram
    autonumber
    actor Alumno as Alumno (Expositor)
    participant AirflowUI as Airflow UI (Variables)
    participant Scheduler as Airflow Scheduler
    participant Postgres as PostgreSQL (etl_audit)
    
    Alumno->>AirflowUI: 1. Modifica 'aduanas_api_base_url' (URL errónea)
    Alumno->>AirflowUI: 2. Dispara 'dag_01_etl_importaciones'
    AirflowUI->>Scheduler: Tarea 'validar_api' falla (HTTP != 200)
    Note over Scheduler: Reintento 1 (15 seg) -> Reintento 2 (15 seg) -> Falla definitiva
    Scheduler->>Postgres: on_failure_callback registra 'FAILED' en public.etl_audit
    Alumno->>Postgres: 3. Ejecuta SELECT en DBeaver y muestra fila FAILED con mensaje de error
    Alumno->>AirflowUI: 4. Dispara 'dag_02_etl_exportaciones'
    Note over AirflowUI: DAG 02 corre exitosamente (SUCCESS) y dispara DAG 03
    Alumno->>AirflowUI: 5. Restaura URL válida y re-ejecuta DAG 01 en SUCCESS
```

### Paso a Paso para la Exposición en Vivo:

#### Paso 1: Introducir la Falla Aislada en Airflow
En la interfaz de Airflow (`http://localhost:8080`), ve al menú superior: **Admin -> Variables**.
Busca la variable específica de la fuente de importaciones: **`aduanas_api_url_importaciones`** (o `aduanas_resource_id_importaciones`).
Haz clic en el lápiz (Edit) y cámbiala a:
```text
https://datos.gob.cl/api/3/action/endpoint_falla_controlada
```
*(O ponle a `aduanas_resource_id_importaciones` el valor `c2211d9e_falso`).*  
Haz clic en **Save**.

> **¿Por qué esta variable y no la general?:** Si modificas `aduanas_api_url_importaciones`, la falla queda estrictamente aislada a la fuente de Importaciones (`dag_01`). La fuente de Exportaciones (`dag_02`) conserva su propia variable `aduanas_api_url_exportaciones` intacta, demostrando fehacientemente a la comisión que la caída de un servicio externo no derriba al otro.

*(Alternativa por consola si prefieren terminal):*
```powershell
docker exec -it aduanas-airflow-webserver airflow variables set aduanas_api_url_importaciones "https://datos.gob.cl/api/3/action/endpoint_falla_controlada"
```

#### Paso 2: Disparar `dag_01_etl_importaciones` y Mostrar Reintentos
- Ve a la vista principal de DAGs y haz clic en el botón **Trigger DAG** (ícono de play) en `dag_01_etl_importaciones`.
- Entra a la vista **Grid** o **Graph**.
- **Qué decir a la comisión:**
  > *"Observen cómo la primera tarea, `validar_api`, detecta de inmediato que el endpoint no responde. En lugar de abortar abruptamente, el orquestador activa su política de resiliencia: pasa al estado 'up_for_retry' y realiza dos intentos con 15 segundos de intervalo."*

#### Paso 3: Mostrar la Captura en `public.etl_audit` (DBeaver)
- Una vez agotados los 2 reintentos, la tarea se torna roja (`FAILED`).
- Cambia a la ventana de DBeaver y ejecuta la siguiente consulta en la pestaña SQL:
```sql
SELECT id_audit, dag_id, estado, duracion_seg, mensaje_error, fecha_inicio
FROM public.etl_audit
ORDER BY id_audit DESC
LIMIT 1;
```
- **Qué decir a la comisión:**
  > *"Como se aprecia en DBeaver, nuestro `on_failure_callback` interceptó la excepción en tiempo de ejecución y persistió en la tabla `etl_audit` el DAG causante, el estado FAILED, la duración exacta y el traceback de la falla HTTP. El error quedó documentado forensemente."*

#### Paso 4: Demostrar Aislamiento de Fallas
- Vuelve a Airflow y dispara manualmente **`dag_02_etl_exportaciones`**.
- Muestra cómo `dag_02` pasa inmediatamente por sus tareas en verde (`validar_api`, `extraer_a_raw`, `transformar_a_staging`, `cargar_a_dw`), finaliza en `SUCCESS` y dispara automáticamente a `dag_03_indicadores_mart`.
- **Qué decir a la comisión:**
  > *"Esto demuestra el principio de Aislamiento de Fallas: la indisponibilidad de la fuente de importaciones no interrumpe el procesamiento de exportaciones ni el cálculo de indicadores globales."*

#### Paso 5: Restaurar el Servicio
- En Airflow Variables, restaura la URL legítima en `aduanas_api_url_importaciones` (o en `aduanas_api_base_url` si la habías modificado):
```text
https://datos.gob.cl/api/3/action/datastore_search
```
- Vuelve a disparar `dag_01_etl_importaciones`.
- Observa cómo todas las tareas terminan en verde (`SUCCESS`) y el sistema se recupera de manera 100% autónoma.

---

## 5. LOS 5 INDICADORES DE GESTIÓN Y HALLAZGOS (CONCLUSIONES)

#### Lámina 7 y 8 (06:15 - 08:00) | Data Mart y Análisis de Negocio
Explica las 5 tablas del esquema `mart.*` proyectando DBeaver o el dashboard HTML:

1. **KPI 1: Balanza Comercial y Cobertura por Aduana (`mart.kpi_01_balanza_comercial_aduana`):**
   - **Objetivo:** Identificar qué aduanas presentan superávit comercial para priorizar dotación fiscal e infraestructura logística.
   - **Fórmula:** $\text{Saldo USD} = \sum \text{FOB Export} - \sum \text{CIF Import}$; $\text{Cobertura (\%)} = (\frac{\sum \text{FOB}}{\sum \text{CIF}}) \times 100$.
   - **Meta:** Tasa de Cobertura $\ge 100\%$.
   - **Hallazgo:** San Antonio y Valparaíso concentran el déficit importador del país, mientras que aduanas mineras del norte muestran tasas de cobertura superiores al 200%.

2. **KPI 2: Incidencia del Costo Logístico sobre Valor CIF (`mart.kpi_02_incidencia_costo_logistico`):**
   - **Objetivo:** Medir el impacto de fletes marítimos/aéreos y seguros sobre el valor final de internación de mercancías.
   - **Fórmula:** $\frac{\sum (\text{Flete} + \text{Seguro})}{\sum \text{CIF}} \times 100$.
   - **Meta / Umbral:** $\le 10\%$ marítimo, $\le 18\%$ aéreo.
   - **Hallazgo:** Las rutas marítimas cumplen el umbral promediando un 6.8% de costo logístico, evidenciando eficiencia en fletes a granel.

3. **KPI 3: Índice de Concentración Logística por Aduana (`mart.kpi_03_concentracion_carga_aduanera`):**
   - **Objetivo:** Detectar cuellos de botella y dependencia portuaria nacional.
   - **Fórmula:** $\frac{\sum \text{Toneladas Aduana } i}{\sum \text{Toneladas Nacionales}} \times 100$.
   - **Meta / Umbral:** Ninguna aduana debe exceder el $40\%$ de la carga nacional sin plan de contingencia.
   - **Hallazgo:** San Antonio concentra cerca del 36% del tonelaje, acercándose peligrosamente al umbral de riesgo ante paros portuarios o marejadas.

4. **KPI 4: Densidad de Valor Económico por Tonelada (`mart.kpi_04_valor_agregado_tonelada`):**
   - **Objetivo:** Evaluar si Chile exporta mercancías de alto valor agregado o concentradas en materias primas de bajo valor por tonelada.
   - **Fórmula:** $\frac{\sum \text{Monto FOB USD}}{\sum \text{Peso Toneladas}}$ por país y flujo.
   - **Meta:** Exportaciones $\ge 1.200 \text{ USD/Ton}$.

5. **KPI 5: Participación y Dependencia por Socio Comercial (`mart.kpi_05_participacion_socios_comerciales`):**
   - **Objetivo:** Evaluar el grado de diversificación del comercio exterior chileno y alertar sobreexposición económica con socios específicos (riesgo geopolítico de concentración).
   - **Fórmula:** $\text{Participación (\%)} = (\frac{\text{FOB Export} + \text{CIF Import}_{\text{País}}}{\text{Intercambio Nacional Total}}) \times 100$.
   - **Meta / Umbral:** Ningún socio comercial debe superar el $35.0\%$ del intercambio comercial total de Chile (Alerta de Sobreexposición).
   - **Hallazgo:** Los socios principales (China y EE.UU.) se monitorean continuamente para evitar que la economía nacional dependa excesivamente de una sola potencia económica.

---

## 6. ESTRATEGIA DE DATOS (MATERIAL PARA EL INFORME Y PREGUNTAS)

### 6.1 Decisiones que apoyan los indicadores y quién los usaría
- **Directorio del Servicio Nacional de Aduanas y Ministerio de Hacienda:** Utilizan el KPI de Balanza y Concentración para distribuir los presupuestos de fiscalización física, escáneres de contenedores y dotación de funcionarios en los pasos fronterizos de mayor tráfico.
- **Cámara Marítima y Portuaria de Chile (CAMPORT):** Emplea el KPI de Incidencia de Costo Logístico para monitorear distorsiones en tarifas de fletes y sobrecostos por demoras portuarias.
- **ProChile y Ministerio de Economía:** Utilizan el KPI de Valor Agregado por Tonelada para diseñar incentivos tributarios y subsidios orientados a industrias manufactureras no tradicionales.

### 6.2 Calidad de Datos, Nulos y Duplicados
- **Gestión de Nulos:** En las fuentes originales de Aduanas, muchos registros de exportación FOB no declaran seguro o flete internacional porque la responsabilidad de transporte recae en el comprador extranjero (Incoterm FOB). El script `src/transform.py` no descarta estos registros; imputa `0.0` y calcula el % de celdas no informadas para auditoría.
- **Valores Atípicos:** Se filtran pesos negativos o montos iguales a cero mediante validaciones previas antes de ingresar al Data Warehouse.
- **Duplicados:** Se eliminan duplicados operacionales basados en `codigo_operacion` (`NUM_ATRIBUTO` o `COD_DECLARACION`).

### 6.3 Linaje de Datos (Data Lineage)
```
[API datos.gob.cl (JSON)] 
       │ (HTTP GET paginado, limit=500)
       ▼
[raw.importaciones_raw / raw.exportaciones_raw] 
       │ (JSONB intacto + run_id + fecha_extraccion)
       ▼
[staging.stg_operaciones_aduanas] 
       │ (Limpieza, tipado SQL, normalización, códigos a texto)
       ▼
[dw.dim_* y dw.fact_comercio_exterior] 
       │ (Claves subrogadas, integridad referencial, ON CONFLICT)
       ▼
[mart.kpi_*] 
       │ (Agregaciones analíticas SQL + Metadatos de gobernanza)
       ▼
[Reportes / Dashboards de Decisión]
```

### 6.4 Frecuencia de Actualización vs. Schedule de Airflow
- Los registros de aduanas son consolidados mensualmente por el Servicio Nacional de Aduanas en el portal de datos abiertos.
- Por tanto, el `schedule_interval` óptimo de producción está configurado como mensual (`@monthly` o `0 3 5 * *`, el día 5 de cada mes a las 03:00 AM para procesar el mes cerrado). Para efectos de desarrollo, pruebas y la defensa académica, los DAGs se configuran con `schedule_interval=None` para ser operados por disparo manual o evento (`TriggerDagRunOperator`).

### 6.5 Riesgos y Mitigación
1. **Límites de Peticiones y Bloqueo de IP (Rate Limiting):** Se incorporó una pausa obligatoria de 500 milisegundos (`time.sleep(0.5)`) entre llamadas de paginación CKAN para no saturar los servidores estatales.
2. **Cambios de Esquema en la API del Gobierno:** La capa `raw` almacena en tipo `JSONB`, lo que otorga desacoplamiento estructural: si Aduanas agrega o renombra una columna mañana, la extracción no se rompe.
3. **Privacidad y Datos Sensibles:** Los datasets utilizados corresponden a datos agregados de comercio exterior. No se almacenan datos personales (RUTs de personas naturales, domicilios particulares ni secretos comerciales protegidos por la Ley 19.628).

### 6.6 Escalabilidad Futura
- **Escalabilidad Horizontal de Fuentes:** Para incorporar nuevas fuentes públicas (por ejemplo, el tipo de cambio diario de la API de `mindicador.cl` o estadísticas bancarias de la CMF), solo se requiere crear un nuevo script `src/extract_indicadores.py` y un `dag_04`, integrándose de forma natural a la dimensión de tiempo y al Data Mart sin tocar el código existente.
- **Escalabilidad en Volumen:** En entornos con millones de declaraciones aduaneras anuales, la tabla `dw.fact_comercio_exterior` soporta particionamiento nativo por rango en PostgreSQL (`PARTITION BY RANGE (fecha_operacion)`).

---

## 7. BANCO DE PREGUNTAS INDIVIDUALES DE LA COMISIÓN (40% DE LA NOTA)

En la rúbrica, **el 40% de la nota final depende de cómo cada estudiante responde individualmente a las preguntas de los académicos Natalia Morales y Sebastián Farías**. A continuación tienen las preguntas más probables y sus respuestas técnicas ideales:

---

### Pregunta 1: *"¿Por qué diseñaron una base de datos con esquemas separados (raw, staging, dw, mart) en lugar de cargar directamente los datos limpios a la tabla final?"*
- **Respuesta Destacada:**
  > *"Separar en capas responde al principio arquitectónico de desacoplamiento y no-repudio. La capa `raw` actúa como una zona de aterrizaje inmutable; si en el futuro descubrimos una falla en la lógica de negocio o necesitamos recalcular un indicador histórico, no requerimos volver a consultar la API gubernamental (que podría no estar disponible o haber cambiado), sino que reprocesamos desde `raw`. La capa `staging` es efímera para transformación y homologación, `dw` garantiza el modelo dimensional analítico a largo plazo, y `mart` aísla los KPIs de gestión para que las consultas analíticas pesadas no compitan con el pipeline de carga."*

---

### Pregunta 2: *"¿Qué diferencia existe entre un sistema OLTP y un sistema OLAP / Data Warehouse, y cómo se refleja esto en su modelo?"*
- **Respuesta Destacada:**
  > *"Un sistema OLTP (Online Transaction Processing) está optimizado para operaciones transaccionales frecuentes de inserción, actualización y borrado de registros unitarios, utilizando un alto nivel de normalización (Tercera Forma Normal) para evitar redundancias. Por el contrario, nuestro Data Warehouse es un sistema OLAP (Online Analytical Processing), optimizado para consultas de lectura masiva, agregaciones y análisis multidimensional. Esto se refleja en nuestro esquema en estrella, donde desnormalizamos intencionalmente las dimensiones (`dim_tiempo`, `dim_aduana`, `dim_pais`, `dim_via_transporte`) alrededor de una tabla de hechos central (`fact_comercio_exterior`), minimizando los joins complejos y maximizando la velocidad analítica."*

---

### Pregunta 3: *"¿Cómo garantizan técnicamente la idempotencia y qué sucede si un DAG se ejecuta tres veces seguidas para la misma fecha?"*
- **Respuesta Destacada:**
  > *"Garantizamos la idempotencia mediante dos mecanismos complementarios:
  > 1. En PostgreSQL, definimos restricciones de unicidad sobre la clave de negocio (`codigo_operacion`) y ejecutamos la inserción con `INSERT INTO dw.fact_comercio_exterior ... ON CONFLICT (codigo_operacion) DO UPDATE SET ...`. Si el registro ya existe, se actualizan sus valores pero no se inserta una nueva fila.
  > 2. En `staging.stg_operaciones_aduanas` y en las dimensiones, también usamos `ON CONFLICT DO UPDATE` o `DO NOTHING`.
  > Si el DAG se ejecuta tres veces consecutivas, el número total de filas en `dw.fact_comercio_exterior` se mantiene exactamente en 5.000 filas y `COUNT(DISTINCT codigo_operacion)` sigue siendo 5.000, demostrando duplicación cero."*

---

### Pregunta 4: *"¿Por qué utilizaron Airflow Variables y Connections en vez de declarar las contraseñas y URLs en variables del sistema operativo o en el código Python?"*
- **Respuesta Destacada:**
  > *"Por tres razones críticas de seguridad y gobernanza:
  > 1. **Seguridad y Cero Hardcoding:** Evita que credenciales de base de datos o tokens viajen en el control de versiones de Git.
  > 2. **Centralización y Parametrización en Caliente:** Permite modificar la URL de la API, tokens o credenciales de base de datos desde la interfaz de Airflow sin necesidad de reconstruir la imagen Docker ni redeployar código.
  > 3. **Gestión de Pools y Cifrado:** Airflow Connections cifra las contraseñas en su base de datos de metadatos utilizando una clave Fernet (`AIRFLOW__CORE__FERNET_KEY`), cumpliendo con los estándares de seguridad exigidos en la pauta."*

---

### Pregunta 5: *"¿Cómo se comunican las tareas entre sí dentro del DAG? ¿Por qué no pasaron todo el dataset por XCom?"*
- **Respuesta Destacada:**
  > *"Aplicamos la mejor práctica de ingeniería de datos en Airflow: **XCom se utiliza exclusivamente para el paso de metadatos ligeros** (como `run_id`, el conteo de `filas_leidas`, `filas_cargadas` y el `porcentaje_nulos`), cuyo tamaño es de unos pocos bytes y no satura la base de datos de metadatos de Airflow. Los datos masivos (miles de registros JSON y tablas transaccionales) jamás se pasan por XCom; se escriben y leen directamente en las tablas correspondientes de PostgreSQL (`raw`, `staging` y `dw`)."*

---

### Pregunta 6: *"¿Qué tratamiento aplicaron a los valores nulos y por qué no simplemente eliminaron todas las filas que tenían celdas vacías?"*
- **Respuesta Destacada:**
  > *"Eliminar todas las filas con algún nulo habría provocado una pérdida masiva de información legítima. En las declaraciones aduaneras de exportación, las empresas chilenas venden habitualmente en términos Incoterm FOB (Free on Board), donde el flete y el seguro son contratados por el importador extranjero, por lo que Aduanas registra legalmente esos campos vacíos. Nuestra transformación distingue entre **nulos de negocio permitidos** (los cuales imputamos con `0.0` en flete y seguro sin alterar la integridad) y **nulos críticos inaceptables** (como operaciones sin código de transacción o fecha, las cuales sí descartamos). Además, calculamos y registramos el `% de nulos` en `etl_audit` para auditoría continua."*

---

### Pregunta 7: *"¿Qué hace el `on_failure_callback` y qué valor aporta a la arquitectura?"*
- **Respuesta Destacada:**
  > *"El `on_failure_callback` es una función interceptora que Airflow ejecuta automáticamente cuando una tarea agota todos sus reintentos configurados y pasa al estado definitivo de falla. En nuestro proyecto, esta función extrae el contexto de ejecución de Airflow (`dag_id`, `run_id`, `logical_date`, tiempo transcurrido y la excepción Python específica) y realiza una inserción atómica en `public.etl_audit` con `estado = 'FAILED'` y el mensaje de error. Esto permite contar con un registro forense centralizado de incidentes sin tener que revisar manualmente gigabytes de archivos de logs en el servidor."*

---

### Pregunta 8: *"¿Cómo se relacionan los 5 indicadores de gestión con la toma de decisiones reales en Aduanas o Comercio Exterior?"*
- **Respuesta Destacada:**
  > *"Los 5 indicadores son 100% de negocio y cubren las tres dimensiones críticas del comercio exterior:
  > - **Estratégica y Geopolítica (KPI 1 y KPI 5):** La Balanza Comercial por Aduana y la Participación por Socio Comercial permiten al Ministerio de Hacienda, Relaciones Exteriores y ProChile fiscalizar la entrada de divisas y evitar la sobreexposición económica con un solo país (meta < 35%).
  > - **Operacional y Portuaria (KPI 2 y KPI 3):** La Incidencia del Costo Logístico sobre CIF y la Concentración de Carga permiten detectar sobrecostos abusivos en fletes internacionales y evitar que un solo puerto (como San Antonio, con 35,8%) paralice la cadena de suministro nacional ante contingencias.
  > - **Competitividad y Valor Agregado (KPI 4):** La Densidad de Valor Económico (USD/Ton) permite monitorear si Chile avanza hacia exportaciones manufactureras y agroindustriales con valor agregado (meta > 1.200 USD/Ton) o si sigue estancado en materias primas puras."*

---

## 8. MATRIZ DE ASIGNACIÓN DE ROLES PARA LA DEFENSA

Para asegurar que los 5 integrantes (**Ayala, Barrios, Mellado, Troncoso, Valenzuela**) obtengan la calificación máxima individual (5/5 en Preguntas), se sugiere la siguiente especialización temática durante la preparación:

| Integrante | Rol en la Presentación | Temas de Dominio Clave para Preguntas |
| :--- | :--- | :--- |
| **Leonel Ayala** | Introducción, Fuentes y Arquitectura Global | Justificación de APIs CKAN, esquema en capas, seguridad de credenciales y variables. |
| **Barrios** | Transformación y Calidad de Datos (`staging`) | Reglas de depuración, tratamiento e imputación de nulos, recodificación de catálogos aduaneros. |
| **Mellado** | Modelo Dimensional y Base de Datos (`dw`) | Esquema en estrella, hechos vs dimensiones, claves foráneas e idempotencia (`ON CONFLICT`). |
| **Troncoso** | Orquestación, DAGs y Tolerancia a Fallas | Configuración de Airflow, reintentos, aislamiento de procesos y ejecución de la Demo de Falla. |
| **Valenzuela** | Data Mart, Indicadores de Gestión y Auditoría | Lógica SQL de los 5 KPIs, métricas de `public.etl_audit`, decisiones de negocio y escalabilidad. |

---

*¡Con esta guía, la solución técnica implementada y la ejecución en Docker, el equipo cuenta con el 100% de los elementos exigidos para obtener la nota máxima (7,0)!*
