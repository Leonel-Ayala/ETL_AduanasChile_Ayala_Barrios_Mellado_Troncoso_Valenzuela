# GUION ESTRATÉGICO DE DEFENSA - PRESENTACIÓN EVALUACIÓN 2
## Presentación Oficial: "ETL Aduanas Chile – Presentación E2"
### Asignatura: INF-522 Gestión Informática II | Ingeniería Civil Informática
**Firma Consultora:** DataCustoms Analytics SpA  
**Integrantes:** Leonel Ayala, Luis Barrios, Leonardo Mellado, Alan Troncoso, Héctor Valenzuela  
**Docentes:** Prof. Natalia Morales, Prof. Sebastián Farías  
**Formato de Evaluación:** **10 minutos máximos de exposición** (Slides 01 a 11) + **5 minutos estrictos de preguntas de los docentes** (Slide 12).

---

## 1. Cronograma de Exposición (Control Estricto de 10 Minutos)

Para asegurar que la presentación termine en **9 minutos y 30 segundos** (dejando un margen de seguridad de 30 segundos antes del límite de 10 minutos), el tiempo por diapositiva está estrictamente acotado:

| Diapositiva | Título de la Diapositiva | Integrante Asignado | Tiempo Límite | Enfoque Principal |
| :---: | :--- | :--- | :---: | :--- |
| **01** | Carátula e Introducción Institucional | **Leonel Ayala** | **0:45 min** | Presentación de consultora, problema país y enfoque empresarial. |
| **02** | Problema de Negocio y Fuentes Oficiales | **Luis Barrios** | **0:50 min** | Justificación de APIs CKAN y aclaración de la DIN sin puerto. |
| **03** | Arquitectura: Cómo Fluyen los Datos | **Alan Troncoso** | **0:50 min** | 4 capas, transitoriedad de raw/staging y contraste en DW. |
| **04** | Modelo de Datos: Las Preguntas que Responde | **Leonardo Mellado** | **0:55 min** | Modelo estrella Kimball, granularidad y ON DELETE RESTRICT. |
| **05** | ETL: Cómo Aseguramos la Calidad del Dato | **Héctor Valenzuela** | **0:55 min** | Circuit Breaker (25%), CRC32 y banderas booleanas. |
| **06** | DAGs: Automatización con Apache Airflow | **Luis Barrios** | **0:50 min** | 3 DAGs desacoplados, Fernet, variables y cero contraseñas. |
| **07** | Tolerancia a Fallas · Demo en Vivo | **Leonel Ayala** | **1:00 min** | Protocolo de falla: aislamiento, reintentos y auditoría forense. |
| **08** | Indicadores de Gestión (5 KPIs) | **Leonel Ayala** | **1:10 min** | Fórmulas: Cobertura NULL en Talcahuano y 9.00% CEPAL en KPI 02. |
| **09** | Hallazgos de Negocio Clave | **Leonardo Mellado** | **0:50 min** | Talcahuano #1 real (25.4%) y corrección de turnos ficticios. |
| **10** | Estrategia de Datos y Gobernanza | **Héctor Valenzuela** | **0:45 min** | CC BY 4.0, perfiles decisores y escalabilidad a 50M filas. |
| **11** | Conclusiones y Cierre | **Alan Troncoso** | **0:40 min** | Lecciones aprendidas, síntesis técnica y pase a preguntas. |
| **--** | **TOTAL TIEMPO DE EXPOSICIÓN** | **Equipo Completo** | **9:35 min** | **Margen de 25 segundos antes del corte de 10:00.** |
| **12** | **Ronda de Preguntas de la Comisión** | **Todo el Equipo** | **5:00 min** | **Banco Maestro de Preguntas Trampa y Defensa Técnica.** |

---

## 2. Guion Slide por Slide (Con Respuestas Blindadas Integradas)

---

### Diapositiva 01: Carátula e Introducción
- **Expositor:** **Leonel Ayala**
- **Tiempo asignado:** **0:45 minutos (45 segundos)**.

#### 🎙️ Qué decir (Palabra por palabra):
> *"Muy buenos días profesores Natalia Morales y Sebastián Farías. Junto a mis compañeros Luis Barrios, Leonardo Mellado, Alan Troncoso y Héctor Valenzuela, en representación de nuestra firma consultora DataCustoms Analytics SpA, presentamos la defensa técnica de la Evaluación 2: 'ETL de Comercio Exterior para el Servicio Nacional de Aduanas de Chile'.*  
> *El comercio exterior representa más del 56% del PIB nacional. Sin embargo, históricamente las áreas analíticas del Estado dependían de planillas Excel fragmentadas, sin trazabilidad ni validaciones automáticas. Asumimos el reto de diseñar una plataforma de nivel empresarial en Apache Airflow y PostgreSQL que ingesta automáticamente datos abiertos del Estado, aplica disyuntores automáticos de calidad y centraliza la inteligencia en un Data Mart dimensional con cero contraseñas en código y desacoplamiento absoluto de fallas."*

#### 💡 Blindaje en la Diapositiva (Evita Preguntas):
- Al mencionar que operamos como consultora técnica con enfoque empresarial, dejamos claro desde el segundo uno por qué no es un script casero, justificando la seguridad y los estándares de producción.

---

### Diapositiva 02: Problema de Negocio y Fuentes Oficiales
- **Expositor:** **Luis Barrios**
- **Tiempo asignado:** **0:50 minutos (50 segundos)**.

#### 🎙️ Qué decir:
> *"Identificamos tres interrogantes de negocio críticas: ¿Por qué aduanas entra y sale la carga física del país?, ¿Dónde se genera superávit o déficit comercial?, y ¿Qué tan expuesta está la economía a socios clave?*  
> *Cumpliendo estrictamente la pauta de consumir exclusivamente APIs REST, nos conectamos a dos endpoints oficiales e independientes en datos.gob.cl bajo CKAN DataStore: Importaciones DIN (32.000 registros) y Exportaciones DUS (55.000).*  
> *Ambos datasets comparten 4 ejes naturales: aduana, país, vía de transporte y mes contable.*  
> *Un hallazgo metodológico clave de origen es que la API pública de importaciones publica 9 columnas a nivel país sin desglosar el puerto de entrada. En lugar de inventar puertos ficticios, nuestro pipeline asigna transparentemente la categoría '0: SIN INFORMACION' y activa la bandera booleana `aduana_estimada = TRUE`, asegurando total integridad ética de los datos."*

#### 💡 Blindaje en la Diapositiva (Evita Preguntas):
- Luis desactiva de inmediato la pregunta de "¿Por qué las importaciones dicen SIN INFORMACIÓN?". El profesor ve que el equipo ya lo sabe, lo investigó en la API y lo resolvió con gobernanza.

---

### Diapositiva 03: Arquitectura: Cómo Fluyen los Datos
- **Expositor:** **Alan Troncoso**
- **Tiempo asignado:** **0:50 minutos (50 segundos)**.

#### 🎙️ Qué decir:
> *"La arquitectura fue diseñada en 4 capas desacopladas sobre PostgreSQL, orquestadas por Apache Airflow 2.9:*  
> *1. Extracción: Ingesta directa de APIs CKAN sin archivos intermedios, almacenando payloads JSONB crudos 1:1 en la capa Raw con metadatos de auditoría.*  
> *2. Staging: Normaliza textos, tipa campos y aplica reglas de negocio.*  
> *Un estándar de diseño fundamental es la transitoriedad de Raw y Staging: ambas se limpian en cada corrida con TRUNCATE y DELETE por flujo, evitando saturar el almacenamiento de Docker.*  
> *¿Dónde radica la persistencia histórica? Exclusivamente en el Data Warehouse (`dw`), donde la clave natural única `codigo_operacion` junto a `ON CONFLICT DO UPDATE` garantiza idempotencia matemática: re-ejecutar el pipeline 100 veces actualiza cifras pero jamás duplica registros. Finalmente, el Data Mart agrega los 5 KPIs de negocio, mientras `public.etl_audit` registra cada segundo de ejecución."*

#### 💡 Blindaje en la Diapositiva (Evita Preguntas):
- Alan desactiva la duda: *"Si limpian raw y staging, ¿dónde contrasta la historia?"*. La respuesta ya quedó dicha: en `dw.fact_comercio_exterior` gobernado por `dim_tiempo.fecha` e idempotencia.

---

### Diapositiva 04: Modelo de Datos: Las Preguntas que Responde
- **Expositor:** **Leonardo Mellado**
- **Tiempo asignado:** **0:55 minutos (55 segundos)**.

#### 🎙️ Qué decir:
> *"El modelo del Data Warehouse sigue rigurosamente el esquema en estrella (Star Schema) de Ralph Kimball, optimizado para consultas analíticas de alta velocidad:*  
> *Contamos con 4 dimensiones conformadas: `dim_tiempo` con jerarquía de año, mes, trimestre y semestre; `dim_aduana` con zona geográfica; `dim_pais` con continente; y `dim_via_transporte` con el modo de traslado.*  
> *Al centro, la tabla de hechos `dw.fact_comercio_exterior` consolida tanto compras como ventas con métricas monetarias (FOB, CIF, flete, seguro) y físicas (toneladas).*  
> *Dos decisiones de alto nivel técnico sustentan el modelo: primero, todas las claves foráneas tienen `ON DELETE RESTRICT`, blindando la base contra borrados accidentales en cascada. Y segundo, incorporamos 4 banderas booleanas de auditoría: `montos_estimados`, `peso_estimado`, `aduana_estimada` y `es_atipico`, permitiendo a los analistas separar mediciones directas de estimaciones normativas con un simple filtro SQL."*

#### 💡 Blindaje en la Diapositiva (Evita Preguntas):
- Leonardo justifica por qué `RESTRICT` (protege la historia) y por qué estrella (rapidez de JOINs analíticos), evitando preguntas teóricas sobre copos de nieve o borrados huérfanos.

---

### Diapositiva 05: ETL: Cómo Aseguramos la Calidad del Dato
- **Expositor:** **Héctor Valenzuela**
- **Tiempo asignado:** **0:55 minutos (55 segundos)**.

#### 🎙️ Qué decir:
> *"La calidad de datos en nuestra solución es un control activo de ingeniería estructurado en tres fases:*  
> *1. En Extracción: Paginamos bloques de 500 registros con rate limiting de 0.5 segundos y manejo de HTTP 429 con retroceso exponencial.*  
> *2. En Limpieza: Implementamos un Circuit Breaker o disyuntor de calidad: si el lote crudo supera el 25% de celdas vacías o registros nulos, el pipeline se aborta inmediatamente antes de tocar Staging o DW.*  
> *Para los países resolvimos la duplicidad de socios como Paraguay implementando códigos deterministas mediante hash CRC32.*  
> *3. En Carga: Los valores atípicos (outliers) sobre el percentil 99.5 de peso no se eliminan para no descuadrar la contabilidad nacional, sino que se etiquetan con `es_atipico = TRUE`. En nuestra última corrida logramos 0% de celdas vacías en campos requeridos, 0 rechazos, y etiquetamos formalmente las estimaciones de aduana y flete en la metadata."*

#### 💡 Blindaje en la Diapositiva (Evita Preguntas):
- Héctor aclara por qué no se borran los outliers (descuadraría balances de aduanas) y por qué se usa CRC32 (solución definitiva a duplicados de países).

---

### Diapositiva 06: DAGs: Automatización con Apache Airflow
- **Expositor:** **Luis Barrios**
- **Tiempo asignado:** **0:50 minutos (50 segundos)**.

#### 🎙️ Qué decir:
> *"La orquestación fue construida en Apache Airflow 2.9 desacoplando el flujo en 3 DAGs autónomos:*  
> *`dag_01_etl_importaciones`, `dag_02_etl_exportaciones`, y `dag_03_indicadores_mart`.*  
> *El desacoplamiento es total: dag_01 y dag_02 son independientes. Si la API de importaciones falla, exportaciones sigue procesándose y, al terminar, gatilla dag_03 mediante `TriggerDagRunOperator`.*  
> *En producción la frecuencia está calendarizada de forma mensual (`0 3 5 * *`), pero para efectos evaluativos se ejecuta a demanda.*  
> *Bajo directrices de ciberseguridad cumplimos la regla estricta de cero credenciales expuestas: la conexión `postgres_dw` utiliza cifrado criptográfico Fernet (`AIRFLOW_FERNET_KEY`), las URLs se gestionan mediante Variables de Airflow y el acceso centralizado en `src/db.py` no posee contraseñas hardcodeadas."*

#### 💡 Blindaje en la Diapositiva (Evita Preguntas):
- Luis demuestra que no hay passwords en Git y que un fallo en un endpoint externo no bloquea al otro pipeline.

---

### Diapositiva 07: Tolerancia a Fallas · Demo en Vivo
- **Expositor:** **Leonel Ayala**
- **Tiempo asignado:** **1:00 minuto (60 segundos)**.

#### 🎙️ Qué decir:
> *"Para certificar la resiliencia operativa, diseñamos una prueba de falla controlada en vivo:*  
> *Paso 1: Modificamos la variable `aduanas_api_url_importaciones` apuntándola a una ruta rota (`/apimala`).*  
> *Paso 2: Ejecutamos `dag_01`. La tarea `validar_api` actúa como gatekeeper: detecta el error HTTP, ejecuta 2 reintentos con 15 segundos de pausa y falla de forma segura antes de realizar TRUNCATE o insertar datos basura.*  
> *Paso 3: El callback `on_failure_callback` registra automáticamente el error forense en `public.etl_audit`.*  
> *Paso 4: Demostramos el desacoplamiento ejecutando `dag_02_etl_exportaciones`, el cual finaliza al 100% SUCCESS y gatilla los indicadores de dag_03.*  
> *Paso 5: Restauramos la URL válida, re-ejecutamos y el pipeline se recupera íntegramente de forma idempotente."*

#### 💡 Blindaje en la Diapositiva (Evita Preguntas):
- Leonel deja claro que la falla no rompió staging ni dw porque el gatekeeper valida antes de abrir transacciones.

---

### Diapositiva 08: Indicadores de Gestión (5 KPIs)
- **Expositor:** **Leonel Ayala**
- **Tiempo asignado:** **1:10 minutos (70 segundos)**.

#### 🎙️ Qué decir:
> *"El Data Mart implementa 5 indicadores estratégicos ejecutados en PostgreSQL:*  
> *1. Balanza por Aduana: Mide el saldo FOB menos CIF y la tasa de cobertura. En puertos puramente exportadores en esta muestra como Talcahuano, la cobertura es matemáticamente `NULL`, ya que dividir por cero importaciones es indefinido, corrigiendo el 100% artificial del código preliminar.*  
> *2. Incidencia del Costo Logístico: Mide flete y seguro sobre el valor CIF. Cumple la meta con un 9.00% exacto en modo marítimo y terrestre, producto de aplicar el estándar internacional de la CEPAL y ONU (8% flete + 1% seguro), transparentado bajo `montos_estimados = TRUE`.*  
> *3. Concentración de Carga: Monitorea toneladas fiscalizadas por aduana. Talcahuano lidera con un 25,41% nacional, cumpliendo el umbral de control (ninguna aduana supera el 40%, descartando congestión).*  
> *4. Densidad Económica: Revela la brecha estructural de Chile: vendemos a China commodities a 804 USD/t, mientras que a Estados Unidos exportamos manufactura y agroalimentos a 2.236 USD/t.*  
> *5. Socios Comerciales: Monitorea dependencia. China lidera con 17,57%, muy por debajo de la alerta de sobreexposicin del 35%."*

#### 💡 Blindaje en la Diapositiva (Evita Preguntas):
- Leonel explica directamente el 9.00% de la CEPAL y la cobertura NULL de Talcahuano, desactivando por completo las dos preguntas más probables de los profesores.

---

### Diapositiva 09: Hallazgos de Negocio Clave
- **Expositor:** **Leonardo Mellado**
- **Tiempo asignado:** **0:50 minutos (50 segundos)**.

#### 🎙️ Qué decir:
> *"Los hallazgos empíricos sobre las 5.000 operaciones cargadas revelan datos estratégicos:*  
> *Primero, Talcahuano es el verdadero nodo de volumen físico del país en este dataset, fiscalizando 322.145 toneladas (25,41% nacional), impulsado por la industria forestal y pesquera del Biobío. Aclaramos que en versiones iniciales del código se asignaba artificialmente el 50% de carga a San Antonio por índice de fila; al eliminar esa regla artificial y calcular sobre datos limpios, Talcahuano emerge legítimamente como el líder.*  
> *Segundo, la canasta chilena está diversificada: China 17,6%, Estados Unidos 12,5% y Brasil 12,1%, alejando riesgos de dependencia.*  
> *Tercero, evidenciamos que los envíos a Norteamérica y Mercosur triplican el valor agregado por tonelada respecto a Asia.*  
> *Y cuarto, confirmamos con auditoría que el 100% de las declaraciones de importación abiertas no incluyen aduana ni flete desagregado, información de alto valor para mejorar las APIs de Aduanas."*

#### 💡 Blindaje en la Diapositiva (Evita Preguntas):
- Leonardo desactiva de inmediato la duda: *"¿Por qué en el informe previo decían que San Antonio tenía 35.8% y ahora Talcahuano tiene 25.4%?"*. Explica que se quitó el sesgo artificial de código antiguo y se reflejó la realidad física.

---

### Diapositiva 10: Estrategia de Datos y Gobernanza
- **Expositor:** **Héctor Valenzuela**
- **Tiempo asignado:** **0:45 minutos (45 segundos)**.

#### 🎙️ Qué decir:
> *"Nuestra estrategia de datos garantiza valor sostenido en el tiempo:*  
> *Definimos roles para tres decisores: Directivos de Aduanas (monitoreo de congestión y balanza), ProChile (promoción de exportaciones con valor agregado) y el Ministerio de Hacienda (fiscalización arancelaria y dependencia).*  
> *En licenciamiento operamos bajo Creative Commons Reconocimiento (CC BY 4.0), conforme a la política de Datos Abiertos del Gobierno.*  
> *Respecto a escalabilidad, el modelo está preparado para crecer a 50 millones de registros históricos aplicando particionamiento declarativo por rango de año en PostgreSQL (`PARTITION BY RANGE`), ingestas incrementales con el parámetro `filters` de CKAN y workers distribuidos en Celery."*

#### 💡 Blindaje en la Diapositiva (Evita Preguntas):
- Héctor anticipa cómo escala el sistema a millones de registros y formaliza la gobernanza legal de los datos públicos.

---

### Diapositiva 11: Conclusiones y Cierre
- **Expositor:** **Alan Troncoso**
- **Tiempo asignado:** **0:40 minutos (40 segundos)**.

#### 🎙️ Qué decir:
> *"Para cerrar nuestra exposición, sintetizamos tres grandes lecciones de ingeniería:*  
> *1. Para el Negocio: Es viable generar inteligencia estratégica en tiempo real conectándose directamente a los datos abiertos del Estado.*  
> *2. Para TI: La arquitectura desacoplada demuestra que la caída de un servicio externo no debe paralizar los procesos del Estado; cada pipeline debe tener aislamiento, reintentos y persistencia idempotente.*  
> *3. En Datos: Ante la ausencia de un dato en origen, la regla de oro no es inventarlo, sino etiquetarlo transparentemente en la metadata y aplicar controles disyuntores de calidad.*  
> *Con esto concluimos la presentación en 9 minutos y medio y damos paso inmediato a la ronda de preguntas."*

---

## 3. Diapositiva 12: Gran Banco de Preguntas Trampa de la Comisión (Ronda de 5 Minutos)

> ⚠️ **REGLA DE ORO DE LA DEFENSA:** En evaluaciones universitarias, los docentes (**Prof. Sebastián Farías** y **Prof. Natalia Morales**) suelen apuntar a **cualquier integrante al azar** (*"A ver Luis, respóndame usted..."*, *"Héctor, ¿por qué aquí usaron RESTRICT y no CASCADE?"*, *"Leonel, ¿por qué la cobertura dio NULL?"*) para verificar si hubo trabajo real en equipo o si uno solo hizo el proyecto.  
> **Por ende, los 5 integrantes (Leonel, Luis, Alan, Leonardo y Héctor) DEBEN dominar las 16 respuestas.**  
> Si la pregunta es dirigida con nombre y apellido, responde el aludido con seguridad. Si la pregunta es abierta al grupo, toma la palabra el vocero sugerido inmediatamente para no dejar silencios incómodos. Cualquier compañero puede complementar diciendo: *"Complementando lo que menciona mi compañero..."*.

---

### 3.1. Cheat Sheet de Emergencia (16 Respuestas Clave en 1 Frase)

Esta tabla es la guía rápida para repasar antes de entrar a defender. Si te preguntan sobre cualquiera de estos temas, di la **frase clave** y nombra el **archivo/tabla de respaldo**:

| N° | Pregunta del Docente | Respuesta Clave en 1 Frase | Concepto / Respaldo Técnico | Vocero si es abierta |
| :---: | :--- | :--- | :--- | :---: |
| **01** | ¿Por qué consultora externa y no script interno? | Aplicamos estándares enterprise: cero claves en código, desacoplamiento y auditoría forense. | Estándar de entrega profesional. | **Leonel** |
| **02** | ¿Dónde está la contraseña de la base de datos en Git? | En ninguna línea; está cifrada con la llave Fernet en Airflow y leída por variables de entorno. | `src/db.py`, `AIRFLOW_FERNET_KEY`. | **Leonel** |
| **03** | Si falla la validación de API, ¿se borra raw o staging? | No, la tarea `validar_api` actúa como gatekeeper antes del TRUNCATE; las tablas quedan intactas. | `dag_01_etl_importaciones.py`. | **Luis** |
| **04** | Si un DAG falla y el otro no, ¿se calculan KPIs a medias? | Sí, para dar datos frescos, pero `etl_audit` registra la falla; en producción se usa `ExternalTaskSensor`. | `TriggerDagRunOperator`, `public.etl_audit`. | **Alan** |
| **05** | ¿Cómo demuestran en vivo que el código es idempotente? | Ejecutamos el DAG dos veces y `COUNT(DISTINCT codigo_operacion)` da exactamente 2.500; no duplica nada. | `ON CONFLICT DO UPDATE` en `src/load.py`. | **Luis** |
| **06** | ¿Por qué modelo estrella y no copo de nieve (Snowflake)? | Priorizamos velocidad de lectura analítica y simplicidad SQL de KPIs sin hacer JOINs redundantes. | Star Schema de Ralph Kimball. | **Leonardo** |
| **07** | ¿Por qué `ON DELETE RESTRICT` y no `CASCADE` en FKs? | Porque la integridad histórica es sagrada; `CASCADE` borraría miles de hechos si alguien borra una dimensión. | `sql/01_ddl_esquemas.sql`. | **Leonardo** |
| **08** | Si borran raw y staging, ¿dónde contrasta la historia? | En `dw.fact_comercio_exterior` gobernado por la dimensión tiempo y la clave única de operación. | `dim_tiempo.fecha`, persistencia en DW. | **Alan** |
| **09** | ¿Cómo escalaría este Data Warehouse a 50M de filas? | Con particionamiento declarativo por rango de año en PostgreSQL (`PARTITION BY RANGE`) y workers Celery. | Escalabilidad PostgreSQL + Airflow. | **Héctor** |
| **10** | ¿Dónde está el Circuit Breaker del 25% y qué excepción lanza? | En `src/transform.py` (líneas 182-200); si las celdas vacías superan el 25%, lanza un `ValueError`. | Circuit Breaker / Quality Gate. | **Héctor** |
| **11** | ¿Por qué marcar valores atípicos en vez de borrarlos? | Porque son embarques mineros reales de alto tonelaje; borrarlos descuadraría la contabilidad nacional. | Flag booleano `es_atipico = TRUE`. | **Héctor** |
| **12** | ¿Por qué importaciones no trae aduana y exportaciones sí? | En datos.gob.cl, la DIN es un resumen nacional de 9 columnas; no inventamos datos y pusimos aduana 0. | `aduana_estimada = TRUE`. | **Luis** |
| **13** | ¿Por qué KPI 02 da 9.00% exacto y antes salía 14.2%? | El 14.2% era mock data en memoria del script viejo; el 9.00% es Postgres real con estándar CEPAL (8% flete + 1% seguro). | `montos_estimados = TRUE`, estándar CEPAL. | **Leonel** |
| **14** | ¿Por qué no aparece vía aérea en el gráfico del KPI 02? | Vía aérea requería aduana Metropolitana (48); al llegar importaciones con aduana 0, se asignaron a marítima y terrestre. | Regla de inferencia en `src/transform.py`. | **Alan** |
| **15** | ¿Por qué Talcahuano tenía antes 100% de cobertura y ahora NULL? | Porque Talcahuano solo exportó y tuvo $0 CIF; dividir por cero es indefinido (`NULL`), no 100% de superávit. | Corrección en `sql/02_indicadores_mart.sql`. | **Alan** |
| **16** | ¿Por qué San Antonio era 35.8% y ahora Talcahuano tiene 25.4%? | El código viejo asignaba artificialmente la mitad a San Antonio con `idx % 2 == 0`; al borrar el sesgo, Talcahuano lidera real. | Ranking físico en `mart.kpi_03`. | **Leonardo** |

---

### 3.2. Detalle Exhaustivo de Respuestas Técnicas (Para Profundizar)

Cualquiera de los 5 integrantes debe estar listo para responder el desarrollo técnico de las siguientes preguntas si el profesor exige mayor detalle:

### BLOQUE A: Arquitectura, Orquestación y Seguridad (Prof. Sebastián Farías)

#### Pregunta 01: "¿Por qué decidieron estructurar la solución como una consultora externa y no como un script interno de Aduanas?"
- **Preguntada a:** **Cualquiera del equipo** (Vocero si es abierta: **Leonel Ayala**).
- **Respuesta exacta:**
  > *"Porque plantearlo como consultoría técnica nos obligó a aplicar estándares de nivel enterprise: cero contraseñas en código, desacoplamiento absoluto de fallas entre DAGs, trazabilidad forense en `public.etl_audit` y aislamiento de capas en PostgreSQL, garantizando una entrega auditable y reproducible."*

#### Pregunta 02: "¿Dónde está la contraseña de la base de datos en su repositorio?"
- **Preguntada a:** **Cualquiera del equipo** (Vocero si es abierta: **Leonel Ayala**).
- **Respuesta exacta:**
  > *"En ninguna línea de código del repositorio. En `src/db.py` la conexión se solicita mediante el hook oficial de Airflow o leyendo variables de entorno en tiempo de ejecución. El archivo `.env.example` solo trae la plantilla sin claves, y en Airflow la credencial está cifrada con la llave criptográfica Fernet (`AIRFLOW_FERNET_KEY`)."*

#### Pregunta 03: "Si el dag_01 falló en la tarea de validación de API, ¿qué pasó con las tablas raw y staging?"
- **Preguntada a:** **Cualquiera del equipo** (Vocero si es abierta: **Luis Barrios**).
- **Respuesta exacta:**
  > *"Quedaron exactamente intactas. Al ubicar la tarea `validar_api` como un gatekeeper antes de la extracción y el truncado, la falla se intercepta en la capa de transporte, evitando que se ejecute el TRUNCATE o que se inserte basura a medio procesar en la base de datos."*

#### Pregunta 04: "Veo que dag_03 se dispara automáticamente. ¿Qué pasa si dag_01 termina bien pero dag_02 falla? ¿Se calculan los KPIs con datos a medias?"
- **Preguntada a:** **Cualquiera del equipo** (Vocero si es abierta: **Alan Troncoso**).
- **Respuesta exacta:**
  > *"En el diseño actual, el `TriggerDagRunOperator` dispara el dag_03 al finalizar exitosamente cualquiera de los dos flujos para asegurar que los analistas dispongan de la información más fresca disponible, quedando explícitamente registrado en `public.etl_audit` que exportaciones falló. En una fase productiva corporativa, se puede condicionar el disparo a un operador sensor (`ExternalTaskSensor`) que exija el éxito conjunto de ambos DAGs antes de gatillar el Data Mart."*

#### Pregunta 05: "¿Cómo me demuestran en vivo que su código es idempotente si ejecuto el DAG dos veces seguidas?"
- **Preguntada a:** **Cualquiera del equipo** (Vocero si es abierta: **Luis Barrios** o **Alan Troncoso**).
- **Respuesta exacta:**
  > *"Muy simple, profesor: en DBeaver ejecutamos `SELECT tipo_flujo, COUNT(*) AS hechos, COUNT(DISTINCT codigo_operacion) AS unicos FROM dw.fact_comercio_exterior GROUP BY tipo_flujo;`. En ambas ejecuciones el conteo da exactamente 2.500 hechos y 2.500 operaciones únicas por flujo. Gracias a la cláusula `ON CONFLICT (codigo_operacion) DO UPDATE` de `src/load.py`, PostgreSQL actualiza los montos y el timestamp, pero no duplica ni una sola fila."*

---

### BLOQUE B: Modelado Dimensional, Kimball y SQL (Prof. Natalia Morales)

#### Pregunta 06: "¿Por qué usaron un modelo estrella (Star Schema) y no un modelo copo de nieve (Snowflake)?"
- **Preguntada a:** **Cualquiera del equipo** (Vocero si es abierta: **Leonardo Mellado**).
- **Respuesta exacta:**
  > *"Porque en un Data Mart de comercio exterior priorizamos la velocidad de lectura analítica y la simplicidad en las consultas SQL de los KPIs. El modelo estrella desnormaliza las jerarquías dentro de cada dimensión (como año, mes y trimestre en `dim_tiempo`), reduciendo drásticamente la cantidad de JOINs necesarios y optimizando el rendimiento."*

#### Pregunta 07: "¿Por qué la tabla de hechos tiene claves foráneas con `ON DELETE RESTRICT` y no `CASCADE`?"
- **Preguntada a:** **Cualquiera del equipo** (Vocero si es abierta: **Leonardo Mellado**).
- **Respuesta exacta:**
  > *"Porque en un Data Warehouse analítico, la integridad de los hechos históricos es sagrada. Si usáramos `CASCADE`, al borrar por error una aduana en `dim_aduana` se eliminarían automáticamente miles de hechos de exportación e importación, destruyendo la contabilidad histórica. `RESTRICT` prohíbe taxativamente la eliminación de dimensiones que tengan transacciones referenciadas."*

#### Pregunta 08: "Si limpian Raw y Staging en cada corrida, ¿dónde se contrastan los datos nuevos contra la historia?"
- **Preguntada a:** **Cualquiera del equipo** (Vocero si es abierta: **Alan Troncoso**).
- **Respuesta exacta:**
  > *"El contraste histórico ocurre en la capa analítica `dw.fact_comercio_exterior`. Allí conviven todos los periodos históricos gobernados por la dimensión de tiempo (`dim_tiempo.fecha`), mientras que la clave natural única `codigo_operacion` con la cláusula `ON CONFLICT DO UPDATE` garantiza que si re-ejecutamos un mes pasado, se actualicen las métricas sin duplicar jamás registros."*

#### Pregunta 09: "¿Cómo escalaría este Data Warehouse si Aduanas nos entregara 50 millones de registros de los últimos 10 años?"
- **Preguntada a:** **Cualquiera del equipo** (Vocero si es abierta: **Héctor Valenzuela**).
- **Respuesta exacta:**
  > *"En PostgreSQL aplicaríamos particionamiento declarativo sobre `dw.fact_comercio_exterior` particionando por rango anual de `fecha_operacion` (`PARTITION BY RANGE`). En Airflow configuraríamos un pool de conexiones con workers distribuidos en Celery o Kubernetes, y en la extracción utilizaríamos el parámetro `filters` de CKAN para hacer ingestas incrementales mensuales por rango de fecha contable."*

---

### BLOQUE C: Calidad del Dato, Banderas de Auditoría y KPIs (Ambos Docentes)

#### Pregunta 10: "Ustedes mencionan un Circuit Breaker del 25% de nulos. ¿En qué parte del código está implementado y qué excepción lanza?"
- **Preguntada a:** **Cualquiera del equipo** (Vocero si es abierta: **Héctor Valenzuela**).
- **Respuesta exacta:**
  > *"Está implementado en el módulo `src/transform.py` entre las líneas 182 y 200. Lee el DataFrame crudo de pandas, cuantifica celdas vacías y nulas frente al total de celdas y, si supera el valor de la variable de Airflow `aduanas_max_pct_nulos_permitido` (25.0%), lanza un `ValueError` con el mensaje `[CIRCUIT BREAKER] Violación de Quality Gate`. La tarea falla, no inserta nada en staging ni DW y dispara el callback de auditoría."*

#### Pregunta 11: "¿Por qué marcaron los valores atípicos con un booleano en lugar de eliminarlos en el proceso de limpieza?"
- **Preguntada a:** **Cualquiera del equipo** (Vocero si es abierta: **Héctor Valenzuela**).
- **Respuesta exacta:**
  > *"Porque en finanzas públicas y comercio exterior, una operación de 2.000 toneladas de concentrado de cobre no es un error de digitación; es un embarque minero extraordinario. Si lo hubiésemos borrado, habríamos falseado la recaudación fiscal y el saldo de la balanza comercial. Al marcarlo con `es_atipico = TRUE`, preservamos la integridad contable total en el DW y permitimos que las consultas analíticas del Data Mart decidan si incluirlo o aislarlo."*

#### Pregunta 12: "¿Por qué la API pública de importaciones de datos.gob.cl no trae aduana de ingreso mientras que la de exportaciones sí?"
- **Preguntada a:** **Cualquiera del equipo** (Vocero si es abierta: **Luis Barrios**).
- **Respuesta exacta:**
  > *"Porque en el portal datos.gob.cl, el dataset de exportaciones (DUS) fue publicado con el atributo `Region origen`, permitiendo homologar a aduanas locales. En cambio, el dataset abierto de importaciones (DIN) corresponde a una tabla agregada a nivel país de 9 campos por producto y país de origen, sin desglosar el puerto de ingreso. Por ende, mapeamos a '0: SIN INFORMACION' y marcamos `aduana_estimada = TRUE` para no inventar datos falsos."*

#### Pregunta 13: "¿Por qué el KPI 02 (Incidencia Costo Logístico) da exactamente 9.00% y por qué antes salía una barra morada de 14.2% con 6 aduanas?"
- **Preguntada a:** **Cualquiera del equipo** (Vocero si es abierta: **Leonel Ayala** o **Alan Troncoso**).
- **Respuesta exacta:**
  > *"En el código preliminar, el dashboard mostraba datos simulados hardcodeados provenientes de una función fallback en memoria. En la versión actual, el reporte consulta 100% en vivo a PostgreSQL. Como la API pública entrega USD CIF sin fletes ni seguros desglosados, `src/transform.py` aplica el estándar de la CEPAL y Naciones Unidas: 8% flete + 1% seguro = 9% de costo logístico sobre CIF. Matemáticamente (9% / CIF) * 100 resulta en 9.00% exacto, y se registra con `montos_estimados = TRUE`."*

#### Pregunta 14: "¿Por qué en el gráfico del KPI 02 no aparece la vía de transporte aérea?"
- **Preguntada a:** **Cualquiera del equipo** (Vocero si es abierta: **Alan Troncoso**).
- **Respuesta exacta:**
  > *"Porque en las reglas de inferencia de `src/transform.py`, la vía aérea requería explícitamente estar vinculada a la aduana Metropolitana (código 48). Al no traer aduana la API de importaciones y asignarse a la categoría 0, los registros se distribuyeron por país fronterizo a vía Terrestre y los de ultramar a vía Marítima, dejando la vía aérea con cero operaciones identificables en esta muestra abierta."*

#### Pregunta 15: "¿Por qué Talcahuano tenía antes un 100% de cobertura y ahora tiene NULL?"
- **Preguntada a:** **Cualquiera del equipo** (Vocero si es abierta: **Alan Troncoso**).
- **Respuesta exacta:**
  > *"Porque en el código antiguo existía un `CASE WHEN importaciones = 0 THEN 100` que forzaba artificialmente un 100% de cobertura. Matemáticamente la tasa de cobertura es `(Exportaciones FOB / Importaciones CIF) * 100`. En puertos como Talcahuano que solo exportaron y registraron $0 de importación en la muestra, dividir por cero es matemáticamente indefinido (`NULL`), corrigiendo un falso superávit artificial."*

#### Pregunta 16: "¿Por qué en el primer informe decían que San Antonio concentraba el 35,8% de carga y ahora Talcahuano tiene el 25,4%?"
- **Preguntada a:** **Cualquiera del equipo** (Vocero si es abierta: **Leonardo Mellado**).
- **Respuesta exacta:**
  > *"Porque en la versión preliminar existía una regla por turnos (`idx % 2 == 0`) que asignaba de forma ficticia la mitad de los registros sin aduana a San Antonio. En la versión final eliminamos esa regla artificial, agrupamos los registros sin puerto en 'SIN INFORMACION' y dejamos que el ranking de carga en `mart.kpi_03` refleje la realidad física de las declaraciones. Por eso Talcahuano emerge legítimamente con sus 322.145 toneladas reales comprobadas."*

---

## 4. Dinámica de Trabajo en Equipo durante los 5 Minutos de Preguntas

Para proyectar la máxima cohesión y evitar titubeos ante la comisión:

1. **Si la pregunta es dirigida con nombre y apellido:**  
   El integrante aludido responde con tranquilidad y firmeza usando la frase clave del Cheat Sheet. No mires a tus compañeros buscando rescate; confía en la respuesta directa que tienes en la tabla.
2. **Si la pregunta es abierta al grupo (*"¿Quién me explica por qué...?"*):**  
   El vocero sugerido toma la palabra de inmediato (en menos de 1 segundo). Si otro integrante se siente con más energía o dominio en ese segundo, puede tomar la palabra sin dudar. Lo crucial es **cero segundos de silencio**.
3. **La Técnica del Complemento Profesional:**  
   Cuando un compañero termina de dar la respuesta clave, otro integrante puede intervenir brevemente diciendo:  
   *"Complementando lo que señala mi compañero..."* y aportar un detalle adicional (por ejemplo, el nombre de la tabla `dw.fact_comercio_exterior` o la bandera `montos_estimados = TRUE`). Esto demuestra a los profesores que **los 5 integrantes construyeron juntos el sistema y todos manejan la totalidad del proyecto**.

