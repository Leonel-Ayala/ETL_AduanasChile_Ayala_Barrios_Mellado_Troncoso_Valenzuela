# Correcciones al repositorio `ETL_AduanasChile_Ayala_Barrios_Mellado_Troncoso_Valenzuela`

Revisión hecha el 04-11-2026 sobre el commit `28f9b89` (rama `main`), contrastando:

- la pauta **E2 INF-522** (rúbrica, entregables y restricciones),
- el **informe** en PDF (25 páginas),
- el **código** del repositorio.

Las correcciones están ordenadas por prioridad. Cada una indica **archivo, línea, problema, cambio exacto y cómo verificarlo**.

---

## 0. Resumen y prioridades

| # | Corrección | Prioridad | Por qué |
|---|---|---|---|
| 1 | Quitar la contraseña escrita en el código y en el README | 🔴 Crítica | La pauta lo prohíbe ("No se deben incluir contraseñas ni claves… en el código ni en la carpeta entregada") |
| 2 | Corregir la asignación de aduana "por turnos" | 🔴 Crítica | Los KPI 1 y 3 salen en parte de una regla artificial, no de los datos |
| 3 | Reemplazar `hash()` en el código de país | 🔴 Crítica | Genera países duplicados (PARAGUAY sale dos veces en el KPI 5) |
| 4 | Quitar "CHINA" como país por defecto | 🔴 Crítica | Infla los KPI 4 y 5 de China |
| 5 | README: la demo de falla usa la Variable equivocada | 🔴 Crítica | Siguiendo el README, el DAG **no falla** |
| 6 | KPI 1: cobertura = 100 % cuando no hay importaciones | 🟠 Alta | Talcahuano aparece con 100 % con CIF = 0 |
| 7 | Declarar y marcar los datos estimados (flete, seguro, peso) | 🟠 Alta | El KPI 2 sale 9,00 % parejo por la imputación |
| 8 | Fernet key vacía | 🟠 Alta | El informe dice que las credenciales van "cifradas" y no es así |
| 9 | Puerto 5432 vs. 5433 | 🟡 Media | El README y el `.env.example` se contradicen |
| 10 | Unificar los scripts de inicialización (`.sh` y `.py`) | 🟡 Media | El `.sh` no crea las Variables por fuente |
| 11 | User-Agent `AntigravityETL` | 🟡 Media | Delata la herramienta de IA usada |
| 12 | Backfill: implementarlo o dejar de afirmarlo | 🟡 Media | La pauta lo pide y el código no filtra por fecha |
| 13 | Limpiar los archivos que no van en la entrega | 🟡 Media | Script que genera el informe y la PPT, guías internas, pauta |
| 14 | Generar las evidencias que faltan en el informe | 🟠 Alta | Faltan el diagrama de dependencias, los logs y las consultas SQL |
| 15 | Ajustar el informe para que calce con el código | 🟠 Alta | Schedule, hallazgos, RESTRICT, cifrado, licencia |

> **Orden recomendado:** 1 → 5, 8 → 11, 13 (código y configuración) → 2, 3, 4, 6, 7 (datos) → **recargar todo desde cero** (sección 16) → 14 (evidencias) → 15 (informe).

---

## 1. 🔴 Contraseña escrita en el código

### Problema

`aduanas_dw_secure_pass` aparece como valor por defecto en 8 archivos de código y 2 veces en el README:

| Archivo | Línea |
|---|---|
| `src/extract.py` | 51 |
| `src/transform.py` | 134 |
| `src/load.py` | 33 |
| `src/audit.py` | 30 |
| `dags/dag_03_indicadores_mart.py` | 58 |
| `scripts/init_airflow_resources.py` | 31 |
| `scripts/init_airflow_resources.sh` | 16 |
| `scripts/generar_dashboard_visual.py` | 16 |
| `README.md` | 99 y 107 |

Además, el README (sección 1) afirma: *"Cero Claves Expuestas: No existen contraseñas…"*. Esto **contradice el propio código** y es lo primero que un evaluador encontraría con un `grep`.

### Cambio

**a) Centralizar la conexión en un solo módulo.** Hoy hay cinco copias de `_obtener_conexion_postgres()`. Creen `src/db.py`:

```python
"""Conexión única a PostgreSQL: Airflow Connection o variables de entorno (sin valores por defecto)."""
import os

try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except ImportError:
    PostgresHook = None


def obtener_conexion():
    if PostgresHook:
        return PostgresHook(postgres_conn_id="postgres_dw").get_conn()

    import psycopg2
    faltantes = [v for v in ("POSTGRES_HOST", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB")
                 if not os.getenv(v)]
    if faltantes:
        raise RuntimeError(f"Faltan variables de entorno: {', '.join(faltantes)}. Revise su archivo .env")

    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )
```

**b) Reemplazar** cada `_obtener_conexion_postgres()` (en `extract.py`, `transform.py`, `load.py`, `audit.py`) y el bloque `psycopg2.connect(...)` de `dag_03_indicadores_mart.py` por:

```python
from src.db import obtener_conexion
...
conn = obtener_conexion()
```

En `scripts/generar_dashboard_visual.py`, si se mantiene, hacer lo mismo o al menos cambiar a `password=os.environ["POSTGRES_PASSWORD"]`.

**c) `scripts/init_airflow_resources.py`, línea 31:**

```python
# antes
password=os.getenv("POSTGRES_PASSWORD", "aduanas_dw_secure_pass"),
# después
password=os.environ["POSTGRES_PASSWORD"],
```

**d) `scripts/init_airflow_resources.sh`, línea 16** (si se conserva, ver sección 10):

```bash
--conn-password "${POSTGRES_PASSWORD:?Defina POSTGRES_PASSWORD en el archivo .env}" \
```

**e) README, líneas 99 y 107:** no escribir la contraseña. Por ejemplo:

```markdown
> **DBeaver:** `localhost:5433`, base `aduanas_dw`, usuario y contraseña los definidos en su `.env`.
```

```markdown
- Conexión **`postgres_dw`**, creada con los valores de `POSTGRES_USER`, `POSTGRES_PASSWORD` y `POSTGRES_DB` del `.env`.
```

**f) README, sección 4, paso 1:** borrar la frase *"El archivo `.env` ya incluye los puertos y contraseñas por defecto preconfigurados"* (es falsa: `.env.example` trae `cambiar_por_contrasena_segura`). Reemplazarla por:

```markdown
Edite `.env` y defina una contraseña propia en `POSTGRES_PASSWORD` antes de levantar los contenedores.
```

### Verificar

```bash
grep -rn "aduanas_dw_secure_pass" . --exclude-dir=.git
# Debe devolver 0 resultados.
```

> ⚠️ La contraseña sigue en el **historial de git** (commits anteriores). Para una evaluación basta con que no esté en la versión entregada (`.zip`). Si quieren borrarla también del historial: `git filter-repo --replace-text` y luego un force push. Solo háganlo si nadie más tiene clones con trabajo pendiente.

---

## 2. 🔴 Aduana asignada "por turnos" cuando falta el dato

### Problema

`src/transform.py`, líneas 226–231:

```python
if not cod_aduana:
    cod_aduana = MAPA_REGION_A_ADUANA.get(id_region) or MAPA_REGION_A_ADUANA.get(reg_nombre)

if not cod_aduana:
    # Asignación probabilística y balanceada según el flujo
    cod_aduana = 34 if (idx % 2 == 0) else (33 if (idx % 3 == 0) else 48)
```

Cuando el registro no trae aduana ni región, se asigna San Antonio, Valparaíso o Metropolitana **según la posición de la fila**. Eso **fabrica** la distribución por aduana. El 34,95 % de San Antonio en el KPI 3 y parte del déficit del KPI 1 pueden ser consecuencia de esta regla. Un profesor que lea el código lo detecta de inmediato.

### Cambio

**a) Agregar una aduana "sin información"** en `MAPA_ADUANAS` (cerca de la línea 33):

```python
MAPA_ADUANAS = {
    0: {"nombre": "SIN INFORMACION", "zona": "SIN INFORMACION"},
    34: {"nombre": "SAN ANTONIO", "zona": "CENTRO"},
    # ... resto igual
}
```

**b) Reemplazar las líneas 229–231:**

```python
if not cod_aduana:
    cod_aduana = 0          # la fuente no informa aduana ni región: no se inventa
    aduana_estimada = True
else:
    aduana_estimada = False  # viene en el dato o se derivó de la región de origen
```

(Si quieren distinguir "derivada de región" de "dato directo", usen tres valores: `'DIRECTA'`, `'DERIVADA_REGION'`, `'SIN_INFO'`.)

**c) KPI 3** (`sql/02_indicadores_mart.sql`): excluir la aduana 0 del ranking o mostrarla aparte, y **reportar qué % de toneladas quedó sin aduana**:

```sql
-- % de toneladas sin aduana informada (dato para el informe)
SELECT ROUND(100.0 * SUM(f.peso_toneladas) FILTER (WHERE a.codigo_aduana = 0)
             / NULLIF(SUM(f.peso_toneladas), 0), 2) AS pct_toneladas_sin_aduana
FROM dw.fact_comercio_exterior f
JOIN dw.dim_aduana a ON a.id_aduana = f.id_aduana;
```

### Vía de transporte (líneas 244–251)

La vía se **infiere**: Argentina, Perú y Bolivia como terrestre; aduana 48 como aérea; todo lo demás marítimo. Es defendible como regla de negocio, pero **tiene que estar declarado** en el informe y, en lo posible, marcado igual que la aduana (`via_estimada = True`).

### Verificar

```sql
SELECT a.nombre_aduana, COUNT(*) FROM dw.fact_comercio_exterior f
JOIN dw.dim_aduana a USING (id_aduana) GROUP BY 1 ORDER BY 2 DESC;
-- Debe aparecer 'SIN INFORMACION' con la cantidad real de registros sin aduana,
-- y la proporción San Antonio / Valparaíso / Metropolitana ya no debe seguir el patrón 1/2 - 1/6 - 1/3.
```

---

## 3. 🔴 Código de país con `hash()` (países duplicados)

### Problema

`src/transform.py`, línea 210:

```python
info_pais = MAPA_PAISES.get(nombre_pais_raw, {"codigo": 400 + (hash(nombre_pais_raw) % 500), ...})
```

En Python 3, `hash()` de un texto **cambia en cada proceso** (aleatorización `PYTHONHASHSEED`). Cada ejecución de Airflow asigna entonces otro código al mismo país. Como `dw.dim_pais` tiene `UNIQUE (codigo_pais)`, se crea una fila nueva con el mismo nombre. Por eso **PARAGUAY aparece dos veces** en la tabla del KPI 5 del informe, con cifras casi iguales. Esto también rompe la afirmación de "consistencia entre fuentes".

### Cambio

```python
import zlib

def _codigo_pais_estable(nombre: str) -> int:
    """Código determinístico (igual en todas las ejecuciones) para países fuera del diccionario."""
    return 1000 + zlib.crc32(nombre.encode("utf-8")) % 900000

# línea 210
info_pais = MAPA_PAISES.get(
    nombre_pais_raw,
    {"codigo": _codigo_pais_estable(nombre_pais_raw), "nombre": nombre_pais_raw, "region": "SIN CLASIFICAR"},
)
```

Opcional, pero mejor: ampliar `MAPA_PAISES` con los países que aparecen en los datos. Se pueden listar con:

```sql
SELECT DISTINCT COALESCE(payload->>'Pais origen', payload->>'Pais destino') AS pais
FROM raw.importaciones_raw
UNION
SELECT DISTINCT COALESCE(payload->>'Pais origen', payload->>'Pais destino')
FROM raw.exportaciones_raw
ORDER BY 1;
```

### Limpieza necesaria

Las filas duplicadas que ya existen **no se arreglan solas**. Hay que vaciar el DW y recargar (sección 16).

### Verificar

```sql
SELECT nombre_pais, COUNT(*) FROM dw.dim_pais GROUP BY 1 HAVING COUNT(*) > 1;
-- Debe devolver 0 filas.
```

---

## 4. 🔴 "CHINA" como país por defecto

### Problema

`src/transform.py`, línea 209:

```python
nombre_pais_raw = _limpiar_texto(row.get("Pais origen") or ... or "CHINA")
```

Todo registro **sin país** se carga como China. Esto infla el KPI 5 (China es el mayor socio, 10,03 %) y el KPI 4 (804 USD/t).

### Cambio

```python
nombre_pais_raw = _limpiar_texto(
    row.get("Pais origen") or row.get("Pais destino") or row.get("PA_ORIG") or row.get("PA_ADQ")
) or "SIN INFORMACION"
```

Y en `MAPA_PAISES`:

```python
"SIN INFORMACION": {"codigo": 0, "nombre": "SIN INFORMACION", "region": "SIN INFORMACION"},
```

En los KPI 4 y 5, excluir `codigo_pais = 0` del ranking y reportar cuántos registros quedaron sin país.

### Otros valores por defecto que conviene revisar (líneas 201–204)

```python
anio = int(row.get("Anio") or row.get("ANO_REG") or datetime.now().year)   # año actual si falta
mes = int(row.get("Mes") or 1)                                              # enero si falta
if mes < 1 or mes > 12:
    mes = 1
```

Un registro sin fecha queda en **enero del año en curso** y distorsiona la serie mensual. Mejor descartarlo y contarlo como rechazado. Así además cumplen lo que dice el informe: *"filtro automático… declaraciones sin fecha válida"*.

```python
anio = row.get("Anio") or row.get("ANO_REG")
mes = row.get("Mes")
try:
    anio, mes = int(anio), int(mes)
    if not 1 <= mes <= 12:
        raise ValueError
except (TypeError, ValueError):
    registros_rechazados += 1
    continue
```

Envíen `registros_rechazados` por XCom y regístrenlo en `etl_audit`: es la métrica de "registros con errores" que pide la pauta.

---

## 5. 🔴 README: la demo de falla no falla

### Problema

`README.md`, líneas 197 y 216, indica cambiar **`aduanas_api_base_url`**. Pero `src/extract.py` (líneas 62–68) lee primero **`aduanas_api_url_importaciones`**, que `init_airflow_resources.py` sí crea. Con eso `aduanas_api_base_url` queda como simple valor de respaldo y **cambiarla no provoca ninguna falla**. El informe (sección 4.4) sí usa la Variable correcta.

### Cambio

README, sección 7, paso 1:

```bash
docker exec -it aduanas-airflow-webserver airflow variables set \
  aduanas_api_url_importaciones "https://datos.gob.cl/api/3/action/apimala"
```

Paso 5:

```bash
docker exec -it aduanas-airflow-webserver airflow variables set \
  aduanas_api_url_importaciones "https://datos.gob.cl/api/3/action/datastore_search"
```

README, línea 108: listar **todas** las Variables que crea el script:

```markdown
- Variables: `aduanas_api_url_importaciones`, `aduanas_api_url_exportaciones`,
  `aduanas_resource_id_importaciones`, `aduanas_resource_id_exportaciones`,
  `aduanas_api_page_limit` (500), `aduanas_api_max_pages` (5), `aduanas_api_token` (vacía).
```

### Verificar

Ejecutar exactamente los pasos del README en un entorno limpio y confirmar que `validar_api` termina en `failed` después de 2 reintentos.

---

## 6. 🟠 KPI 1: 100 % de cobertura cuando no hay importaciones

### Problema

`sql/02_indicadores_mart.sql`, líneas 33–38:

```sql
CASE
    WHEN ... CIF > 0 THEN ROUND(FOB / CIF * 100, 2)
    ELSE 100.00
END AS tasa_cobertura_pct,
```

Si una aduana no tiene importaciones (CIF = 0), la cobertura queda en **100 %**. Esa cobertura no está definida (división por cero). Así aparece Talcahuano en el informe: 100 %, "SUPERÁVIT" y CIF $0.

### Cambio

```sql
CASE
    WHEN SUM(CASE WHEN f.tipo_flujo = 'IMPORTACION' THEN f.monto_cif_usd ELSE 0 END) > 0 THEN
        ROUND(SUM(CASE WHEN f.tipo_flujo = 'EXPORTACION' THEN f.monto_fob_usd ELSE 0 END)
              / SUM(CASE WHEN f.tipo_flujo = 'IMPORTACION' THEN f.monto_cif_usd ELSE 0 END) * 100.0, 2)
    ELSE NULL      -- sin importaciones: cobertura no definida
END AS tasa_cobertura_pct,
CASE
    WHEN SUM(CASE WHEN f.tipo_flujo = 'IMPORTACION' THEN f.monto_cif_usd ELSE 0 END) = 0 THEN NULL
    WHEN SUM(CASE WHEN f.tipo_flujo = 'EXPORTACION' THEN f.monto_fob_usd ELSE 0 END)
       >= SUM(CASE WHEN f.tipo_flujo = 'IMPORTACION' THEN f.monto_cif_usd ELSE 0 END) THEN TRUE
    ELSE FALSE
END AS cumple_meta,
```

Revisar en el DDL que `tasa_cobertura_pct` y `cumple_meta` del KPI 1 **acepten NULL** (sin `NOT NULL`).

### Mejora de presentación

El KPI 1 agrupa por aduana, año y mes. En el informe la tabla muestra "SAN ANTONIO" 6 veces **sin la columna de mes**, y parece un error. Al sacar la captura, incluir `anio` y `mes`, o agregar una vista anual.

---

## 7. 🟠 Datos estimados: flete, seguro y peso

### Problema

`src/transform.py`:

- **Líneas 270–274:** si la importación trae CIF pero no FOB, se estima `FOB = 91 %`, `flete = 8 %` y `seguro = 1 %` del CIF. Por eso el KPI 2 da **exactamente 9,00 %** en todas las vías y aduanas.
- **Línea 290:** si falta el peso, se estima `peso = max(10, FOB / 5)`, es decir, 5 USD por kg. Esto **fija la densidad de valor** del KPI 4 en torno a 5.000 USD/t para esos registros (Italia 4.836 y Alemania 4.275 están sospechosamente cerca).

Ninguna de estas estimaciones aparece en el informe.

### Primero: verificar qué trae realmente la API

```sql
-- ¿La fuente de importaciones trae flete y seguro?
SELECT COUNT(*) AS total,
       COUNT(*) FILTER (WHERE payload ? 'FLETE')  AS con_flete,
       COUNT(*) FILTER (WHERE payload ? 'SEGURO') AS con_seguro,
       COUNT(*) FILTER (WHERE payload ? 'USD FOB' OR payload ? 'FOB') AS con_fob
FROM raw.importaciones_raw;

-- ¿Y el peso?
SELECT COUNT(*) AS total,
       COUNT(*) FILTER (WHERE COALESCE(payload->>'Volumen', payload->>'TOT_PESO', '') <> '') AS con_peso
FROM raw.exportaciones_raw;
```

### Cambio

**a) Marcar las filas estimadas.** Agregar en `staging.stg_operaciones_aduanas` y en `dw.fact_comercio_exterior` (`sql/01_ddl_esquemas.sql`):

```sql
montos_estimados BOOLEAN NOT NULL DEFAULT FALSE,
peso_estimado    BOOLEAN NOT NULL DEFAULT FALSE,
aduana_estimada  BOOLEAN NOT NULL DEFAULT FALSE,
```

En `transform.py`, poner `montos_estimados = True` dentro del `if` de la línea 270, `peso_estimado = True` en la línea 290 y `aduana_estimada` como en la sección 2. Incluir las tres columnas en el INSERT de staging (líneas 330–354) y en el INSERT del hecho en `src/load.py` (líneas 160–197, en las columnas, el SELECT y el `DO UPDATE SET`).

**b) Usar la marca en los KPIs.**

- **KPI 2:** calcular solo con `WHERE NOT f.montos_estimados`. Si la consulta del punto anterior muestra que la API **nunca** trae flete ni seguro, el KPI 2 no se puede medir con esta fuente. En ese caso: **reemplazarlo** por otro KPI calculable (por ejemplo, la participación de cada vía de transporte en el valor CIF) o mantenerlo **declarado como estimación** en el informe.
- **KPI 4:** calcular con `WHERE NOT f.peso_estimado`, o reportar en el informe qué % de toneladas son estimadas.

**c) Agregar una medida de calidad al mart o al informe:**

```sql
SELECT tipo_flujo,
       ROUND(100.0 * AVG(montos_estimados::int), 2) AS pct_montos_estimados,
       ROUND(100.0 * AVG(peso_estimado::int), 2)    AS pct_peso_estimado,
       ROUND(100.0 * AVG(aduana_estimada::int), 2)  AS pct_aduana_estimada
FROM dw.fact_comercio_exterior
GROUP BY tipo_flujo;
```

Esto suma puntos en "validaciones de calidad" y "estrategia de datos: calidad".

---

## 8. 🟠 Fernet key vacía

### Problema

`docker-compose.yml`, línea 12: `AIRFLOW__CORE__FERNET_KEY: ''`. Sin Fernet key, Airflow guarda las contraseñas de las Connections **en texto plano** en su base de metadatos. El informe (sección 4.2) afirma: *"Gestiona de forma cifrada…"*.

### Cambio

**a) Generar una clave** (una vez):

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

**b) `.env.example`:**

```dotenv
# Genere una con: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
AIRFLOW_FERNET_KEY=
```

**c) `docker-compose.yml`, línea 12:**

```yaml
AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY:?Defina AIRFLOW_FERNET_KEY en .env}
```

**d) README, paso 1:** agregar la instrucción para generar la clave.

> Si ya existe la Connection creada sin clave, después de configurar la Fernet key hay que **volver a crearla** (re-ejecutar `init_airflow_resources.py`, que la borra y la crea de nuevo) para que quede cifrada.

---

## 9. 🟡 Puerto 5432 vs. 5433

### Problema

- `README.md` (líneas 94 y 99) e informe (desafío 1) dicen que PostgreSQL queda en **5433** en el host.
- `.env.example`, línea 10: `POSTGRES_PORT=5432`.
- `docker-compose.yml`, línea 43: `"${POSTGRES_PORT:-5432}:5432"`.

Quien siga el README queda en 5432 y se encuentra con el mismo conflicto que ustedes ya resolvieron.

### Cambio

`.env.example`:

```dotenv
# Puerto del HOST para conectarse desde DBeaver (5433 evita choque con un PostgreSQL local).
# Dentro de Docker los contenedores siempre usan 5432.
POSTGRES_PORT=5433
```

Ojo: `docker-compose.yml`, línea 22, define `POSTGRES_PORT: 5432` para los contenedores de Airflow, y eso **está bien** (es el puerto interno). Dejen un comentario para que no se confunda con el del host.

---

## 10. 🟡 Dos scripts de inicialización distintos

### Problema

| Variable | `init_airflow_resources.py` | `init_airflow_resources.sh` |
|---|---|---|
| `aduanas_api_url_importaciones` | ✅ | ❌ |
| `aduanas_api_url_exportaciones` | ✅ | ❌ |
| las demás | ✅ | ✅ |

Si alguien usa el `.sh`, las fuentes quedan **sin Variable propia** y la falla deja de estar aislada (es justamente el "desafío 4" del informe).

### Cambio

Opción recomendada: **borrar `scripts/init_airflow_resources.sh`** y dejar solo el `.py`, que es el que cita el README.

Si prefieren conservar el `.sh`, agregar:

```bash
airflow variables set 'aduanas_api_url_importaciones' 'https://datos.gob.cl/api/3/action/datastore_search'
airflow variables set 'aduanas_api_url_exportaciones' 'https://datos.gob.cl/api/3/action/datastore_search'
```

---

## 11. 🟡 User-Agent `AntigravityETL/1.0`

### Problema

`src/extract.py` (líneas 74 y 128) y `scripts/descubrir_endpoints_aduanas.py` (línea 11) usan:

```python
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AntigravityETL/1.0"}
```

"Antigravity" es el nombre de un editor de código con IA. Delata la herramienta y no aporta nada. Además, hacerse pasar por un navegador (`Mozilla/5.0…`) no es necesario para esta API.

### Cambio

Definirlo una sola vez, por ejemplo en `src/extract.py`:

```python
USER_AGENT = "ETL-Aduanas-UCM-INF522/1.0"
headers = {"User-Agent": USER_AGENT}
```

### Verificar

```bash
grep -rni "antigravity" . --exclude-dir=.git   # 0 resultados
```

---

## 12. 🟡 Backfill (recargar un rango de fechas pasado)

### Problema

La pauta pide: *"poder recargar un rango de fechas pasado"*. El README y el informe dicen que hay backfill "mediante `logical_date`". Pero `extraer_datos_api_a_raw` **siempre trae los primeros 2.500 registros** (offset 0…2.000), sin importar la fecha. Re-ejecutar un día pasado no recarga ese periodo: solo repite la misma muestra (sin duplicar, eso sí).

### Opción A: implementarlo (recomendado si hay tiempo)

CKAN `datastore_search` acepta `filters` como JSON. Si los datos traen columnas de año y mes, se puede filtrar por el mes de `logical_date`:

```python
import json

def extraer_datos_api_a_raw(tipo_flujo, **context):
    ...
    fecha = context["data_interval_start"]          # pendulum.DateTime
    params = {
        "resource_id": resource_id,
        "limit": page_limit,
        "offset": offset,
        "filters": json.dumps({"Anio": str(fecha.year), "Mes": str(fecha.month)}),
    }
```

> Verifiquen antes los **nombres exactos y el tipo** de las columnas de año y mes en el JSON (`SELECT payload FROM raw.importaciones_raw LIMIT 1;`). El código usa `Anio`/`ANO_REG` y `Mes`.

Con eso el backfill funciona así:

```bash
airflow dags backfill dag_01_etl_importaciones -s 2026-01-01 -e 2026-03-31
```

Para que tenga sentido, el schedule debería ser **mensual** (`"0 3 5 * *"`, el día 5 a las 03:00, como dice el informe) y `catchup` puede quedar en `False` (el backfill se lanza a mano).

### Opción B: no implementarlo y dejar de afirmarlo

Quitar "backfill mediante logical_date" del README (sección 1) y del informe (sección 6.4), y escribir algo honesto:

> "Re-ejecutar un run pasado no duplica datos (ON CONFLICT). La extracción por rango de fechas mediante el parámetro `filters` de CKAN queda como mejora."

---

## 13. 🟡 Archivos que no van en la entrega

| Archivo | Acción | Motivo |
|---|---|---|
| `scripts/generar_entregables_informe_ppt.py` | **Eliminar** | Genera automáticamente el informe y la PPT. Con el art. 67 (copia o contenido "reproducido de algún medio") es un riesgo innecesario |
| `GUIA_PRESENTACION_Y_DEFENSA.md` | Eliminar del repo (guardarlo aparte) | Notas internas; además repite la contraseña (líneas 44 y 81) y afirma cosas falsas sobre el Fernet (línea 335) |
| `PLAN_DE_IMPLEMENTACION.md` | Eliminar del repo | Notas internas; tiene marcas `[cite: …]` de un asistente de IA |
| `PAUTA E2-INF-522.pdf` | Eliminar | Es material de los profesores |
| `PRESENTACION_DEFENSA_ETL_ADUANAS_CHILE.pptx` | Reemplazar por la presentación final | Es la versión antigua, con datos que contradicen el código |
| `INFORME_TECNICO_ETL_ADUANAS_CHILE.docx` | Reemplazar por el **PDF** final | La pauta exige el informe en PDF |
| `scripts/generar_dashboard_visual.py` | Opcional: mantener, sin contraseña | Genera las capturas de los KPIs; si se queda, debe usar `src/db.py` |

Agregar a `.gitignore`, por si acaso:

```gitignore
*.docx
reporte_estadisticas_aduanas.html
```

---

## 14. 🟠 Evidencias que faltan en el informe (generarlas después de corregir y recargar)

La rúbrica del informe pide para el nivel "Destacado": *"evidencia completa (capturas de Airflow, logs, consultas)"* y *"diagrama de dependencias"*.

| Evidencia | Cómo obtenerla |
|---|---|
| **Diagrama de dependencias** | Airflow → `dag_01_etl_importaciones` → pestaña **Graph**. Captura de los 3 DAGs |
| **Logs relevantes** | Airflow → tarea `validar_api` del run fallido → **Logs**. Debe verse el `400 Bad Request`, los intentos 1, 2 y 3 y el callback. Otra captura del log de `extraer_a_raw` exitoso (páginas 1/5…5/5) |
| **Consultas SQL de los KPIs** | Captura del SQL de al menos 2 KPIs en DBeaver con su resultado |
| **Prueba de idempotencia** | Correr la consulta de abajo, re-ejecutar `dag_01` y volver a correrla: mismo número |
| **Registros cargados por tabla** | Consulta de abajo: la pauta lo pide **por tabla**, no por DAG |
| **% de registros con errores o nulos** | Desde `etl_audit` + los % estimados de la sección 7c |

**Idempotencia:**

```sql
SELECT tipo_flujo, COUNT(*) AS hechos, COUNT(DISTINCT codigo_operacion) AS operaciones_unicas
FROM dw.fact_comercio_exterior GROUP BY tipo_flujo;
```

**Registros por tabla:**

```sql
SELECT 'raw.importaciones_raw' AS tabla, COUNT(*) FROM raw.importaciones_raw
UNION ALL SELECT 'raw.exportaciones_raw', COUNT(*) FROM raw.exportaciones_raw
UNION ALL SELECT 'staging.stg_operaciones_aduanas', COUNT(*) FROM staging.stg_operaciones_aduanas
UNION ALL SELECT 'dw.dim_tiempo', COUNT(*) FROM dw.dim_tiempo
UNION ALL SELECT 'dw.dim_aduana', COUNT(*) FROM dw.dim_aduana
UNION ALL SELECT 'dw.dim_pais', COUNT(*) FROM dw.dim_pais
UNION ALL SELECT 'dw.dim_via_transporte', COUNT(*) FROM dw.dim_via_transporte
UNION ALL SELECT 'dw.fact_comercio_exterior', COUNT(*) FROM dw.fact_comercio_exterior
UNION ALL SELECT 'mart.kpi_01_balanza_comercial_aduana', COUNT(*) FROM mart.kpi_01_balanza_comercial_aduana
UNION ALL SELECT 'mart.kpi_02_incidencia_costo_logistico', COUNT(*) FROM mart.kpi_02_incidencia_costo_logistico
UNION ALL SELECT 'mart.kpi_03_concentracion_carga_aduanera', COUNT(*) FROM mart.kpi_03_concentracion_carga_aduanera
UNION ALL SELECT 'mart.kpi_04_valor_agregado_tonelada', COUNT(*) FROM mart.kpi_04_valor_agregado_tonelada
UNION ALL SELECT 'mart.kpi_05_participacion_socios_comerciales', COUNT(*) FROM mart.kpi_05_participacion_socios_comerciales
UNION ALL SELECT 'public.etl_audit', COUNT(*) FROM public.etl_audit;
```

**Resumen de auditoría (tiempos, filas y nulos por DAG):**

```sql
SELECT dag_id, estado, COUNT(*) AS ejecuciones,
       ROUND(AVG(duracion_seg), 1) AS duracion_prom_seg,
       ROUND(AVG(filas_leidas)) AS filas_leidas_prom,
       ROUND(AVG(filas_cargadas)) AS filas_cargadas_prom,
       ROUND(AVG(porcentaje_nulos), 2) AS pct_nulos_prom
FROM public.etl_audit
GROUP BY dag_id, estado ORDER BY dag_id, estado;
```

---

## 15. 🟠 Cambios en el informe para que calce con el código

| Sección del informe | Dice | Debe decir |
|---|---|---|
| 4.2 | Connections "de forma cifrada" | Solo es cierto **después** de la corrección 8 |
| 4.2 / título | "Zero-Hardcoding" | Solo es cierto **después** de la corrección 1. Además, cambiar el anglicismo: "Configuración fuera del código" |
| 6.2 punto 2 | "Foreign Keys con políticas ON DELETE RESTRICT" | "Foreign Keys a las 4 dimensiones" (el DDL no tiene `ON DELETE RESTRICT`), o agregar `ON DELETE RESTRICT` al DDL |
| 6.2 punto 3 | "Filtro automático… declaraciones sin fecha válida" | Solo es cierto **después** de la corrección 4 (hoy se asigna enero del año actual) |
| 6.4 | `@monthly` / `schedule=None` | Lo que diga el código final: hoy `@daily`; si aplican la opción A de la sección 12, `"0 3 5 * *"` |
| 6.4 | Backfill con `logical_date` | Según la decisión de la sección 12 |
| 6.5 | "Licencia… Ley 20.285 de Transparencia" | La Ley 20.285 es de acceso a la información pública, **no es una licencia**. Revisar en la ficha del dataset en datos.gob.cl qué licencia tiene (suele ser Creative Commons) y citarla |
| 7, hallazgo 1 | San Antonio 35,8 % | El valor de la tabla recalculada |
| 7, hallazgo 2 | "Antofagasta, Iquique… sobre 250 %" | No aparece en ninguna tabla: eliminarlo o mostrar la tabla que lo respalda |
| 7, hallazgo 3 | Marítimo 6,8 %, aéreo 14,2 % | La tabla dice 9,00 % en todo. Explicar la imputación (sección 7) |
| 3.3 | No menciona estimaciones | Agregar: imputación de flete, seguro y FOB; peso estimado; aduana y vía derivadas; % de cada una |
| 1 | "más del 56 % del PIB" | Citar la fuente (Banco Central o Banco Mundial) o quitar la cifra |
| 2 | Solo los resource_id | Agregar la URL completa de cada API y el link a la documentación de CKAN |
| 6 | No trata la consistencia entre fuentes | Agregar: mismos catálogos de aduana, país y vía para DIN y DUS, código de país estable, verificación de duplicados en dimensiones |
| 7 | Tabla por DAG | Agregar la **tabla por tabla** de la sección 14 |
| 8 | 2 párrafos de conclusión | Agregar resultados concretos de los KPIs, limitaciones (muestra de 2.500, datos estimados) y trabajo futuro |
| Todo | Tono de "consultora", "forense", "nivel empresarial", "riguroso", "exhaustivo" | Primera persona del grupo, contando qué hicieron y qué les costó |

> El grupo tiene 5 integrantes y la pauta indica "no más de 3 personas". Si no lo han conversado con los profesores, conviene hacerlo antes de la presentación.

---

## 16. Recarga completa después de corregir

Las correcciones 2, 3, 4, 6 y 7 cambian cómo se calculan las dimensiones y los hechos. Los datos viejos (con los países duplicados y las aduanas asignadas por turnos) **hay que borrarlos**.

**Opción simple** (borra todo, incluida la auditoría):

```bash
docker compose down -v          # -v borra el volumen postgres_data
docker compose up -d --build    # el DDL se vuelve a ejecutar automáticamente
docker exec -it aduanas-airflow-webserver python /opt/airflow/scripts/init_airflow_resources.py
```

**Opción conservando `etl_audit`:**

```sql
TRUNCATE dw.fact_comercio_exterior, dw.dim_tiempo, dw.dim_aduana, dw.dim_pais, dw.dim_via_transporte,
         staging.stg_operaciones_aduanas, raw.importaciones_raw, raw.exportaciones_raw
         RESTART IDENTITY CASCADE;
-- Si agregaron columnas (sección 7), aplicar los ALTER TABLE o volver a correr el DDL.
```

Luego:

1. Disparar `dag_01` y `dag_02` y esperar a que `dag_03` termine.
2. Correr las consultas de verificación de las secciones 2, 3 y 14.
3. **Volver a sacar todas las capturas de los KPIs.** Las cifras cambiarán (San Antonio, China, Paraguay, Talcahuano).
4. Actualizar el informe y la presentación con las cifras nuevas.

---

## 17. Estructura final recomendada para el `.zip`

```
ETL_AduanasChile_Ayala_Barrios_Mellado_Troncoso_Valenzuela/
├── README.md
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example                 ← POSTGRES_PORT=5433 y AIRFLOW_FERNET_KEY vacía
├── .gitignore
├── dags/
│   ├── dag_01_etl_importaciones.py
│   ├── dag_02_etl_exportaciones.py
│   └── dag_03_indicadores_mart.py
├── src/
│   ├── __init__.py
│   ├── db.py                    ← nuevo: conexión única sin contraseña por defecto
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── audit.py
├── sql/
│   ├── 01_ddl_esquemas.sql
│   └── 02_indicadores_mart.sql
├── scripts/
│   ├── init_airflow_resources.py
│   ├── descubrir_endpoints_aduanas.py
│   └── generar_dashboard_visual.py   (opcional)
└── docs/
    ├── Informe_ETL_Aduanas_Chile.pdf
    └── Presentacion_ETL_Aduanas_Chile.pdf   (o .pptx)
```

> **No incluir** el `.env` real, `logs/`, `__pycache__/` ni la carpeta `.git` en el `.zip`.

---

## 18. Checklist final antes de entregar

**Código y configuración**

- [ ] `grep -rn "aduanas_dw_secure_pass" .` → 0 resultados
- [ ] `grep -rni "antigravity" .` → 0 resultados
- [ ] `grep -n "hash(" src/transform.py` → 0 resultados
- [ ] `grep -n "idx % 2" src/transform.py` → 0 resultados
- [ ] `grep -n '"CHINA")' src/transform.py` → ya no es el valor por defecto
- [ ] `docker-compose.yml` usa `AIRFLOW_FERNET_KEY` desde `.env`
- [ ] `.env.example` con `POSTGRES_PORT=5433` y sin contraseñas reales
- [ ] Un solo script de inicialización y crea las Variables por fuente

**Reproducibilidad (probar en una carpeta nueva, siguiendo SOLO el README)**

- [ ] `cp .env.example .env` → editar contraseña y Fernet → `docker compose up -d --build` funciona
- [ ] `init_airflow_resources.py` crea la Connection y las 8 Variables
- [ ] `dag_01` y `dag_02` terminan en SUCCESS y `dag_03` se dispara solo
- [ ] La demo de falla del README provoca FAILED en `validar_api` y `dag_02` sigue funcionando
- [ ] Re-ejecutar `dag_01` no cambia `COUNT(*)` de `dw.fact_comercio_exterior`

**Datos**

- [ ] `dw.dim_pais` sin nombres duplicados
- [ ] Aduana "SIN INFORMACION" visible con su cantidad real
- [ ] KPI 1 sin coberturas de 100 % con CIF = 0
- [ ] % de montos, peso y aduana estimados calculado y documentado

**Entregables**

- [ ] Informe en **PDF**, con diagrama de dependencias, logs, consultas SQL y registros por tabla
- [ ] Cifras del informe = cifras de las tablas = cifras de la presentación
- [ ] Presentación de 8 a 12 láminas actualizada con las cifras nuevas
- [ ] `.zip` con la estructura de la sección 17

---

## 19. Commits sugeridos (en este orden)

```text
fix(seguridad): quitar contraseña por defecto y centralizar conexión en src/db.py
fix(config): Fernet key desde .env, puerto 5433 en .env.example y un solo script de init
fix(readme): demo de falla con aduanas_api_url_importaciones y pasos reproducibles
fix(transform): código de país estable, sin país ni aduana inventados, rechazar filas sin fecha
feat(calidad): marcar montos, peso y aduana estimados en staging y dw
fix(mart): cobertura NULL sin importaciones y KPIs sin filas estimadas
chore: eliminar generador de entregables, guías internas y pauta; cambiar User-Agent
docs: informe y presentación finales en docs/
```
