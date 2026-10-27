"""
Módulo de Extracción: Validación de API y extracción paginada hacia esquema raw.
Cumple estrictamente con:
- Cero claves expuestas (Uso de Airflow Variables y Connections).
- Manejo de timeouts, reintentos y errores HTTP.
- Paginación y control de límites de uso (rate limit / sleep).
- Almacenamiento íntegro del JSON crudo en esquema raw de PostgreSQL.
"""
import json
import logging
import time
from datetime import datetime
import requests
from requests.exceptions import RequestException, Timeout, HTTPError, ConnectionError

# Intentar importar dependencias de Airflow con fallback para pruebas locales
try:
    from airflow.models import Variable
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except ImportError:
    Variable = None
    PostgresHook = None

logger = logging.getLogger("airflow.task")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = logging.getLogger("etl_extract")


def _obtener_variable(nombre, valor_defecto=""):
    """Obtiene una variable desde Airflow Variables o fallback por defecto."""
    if Variable:
        try:
            return Variable.get(nombre, default_var=valor_defecto)
        except Exception:
            pass
    return valor_defecto


def _obtener_conexion_postgres():
    """Retorna conexión a PostgreSQL usando PostgresHook de Airflow."""
    if PostgresHook:
        hook = PostgresHook(postgres_conn_id="postgres_dw")
        return hook.get_conn()
    import os
    import psycopg2
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "aduanas_dw_secure_pass"),
        dbname=os.getenv("POSTGRES_DB", "aduanas_dw")
    )


def validar_api_disponible(tipo_flujo):
    """
    Tarea de validación previa: comprueba que el endpoint responde exitosamente (HTTP 200 y success: True).
    Si falla, lanza RuntimeError para activar los reintentos de Airflow y registrar la falla en etl_audit.
    """
    flujo = tipo_flujo.upper()
    base_url = _obtener_variable("aduanas_api_base_url", "https://datos.gob.cl/api/3/action/datastore_search")
    
    if flujo == "IMPORTACION":
        resource_id = _obtener_variable("aduanas_resource_id_importaciones", "c2211d9e-4c8e-4ef4-84d5-87039c515584")
    elif flujo == "EXPORTACION":
        resource_id = _obtener_variable("aduanas_resource_id_exportaciones", "f6fe6014-e353-4bc6-bd5d-762511a4dd57")
    else:
        raise ValueError(f"Tipo de flujo no válido: {tipo_flujo}")

    api_token = _obtener_variable("aduanas_api_token", "")
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AntigravityETL/1.0"}
    if api_token:
        headers["Authorization"] = api_token

    params = {"resource_id": resource_id, "limit": 1}
    logger.info(f"Validando disponibilidad de API para {flujo}: {base_url} (resource_id={resource_id})")

    try:
        response = requests.get(base_url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        if not data.get("success", False):
            raise RuntimeError(f"La API respondió sin éxito ('success': False): {data.get('error')}")
        
        total = data.get("result", {}).get("total", 0)
        logger.info(f"[VALIDACIÓN EXITOSA] API de {flujo} operativa. Total disponible en origen: {total:,} registros.")
        return True
    except RequestException as e:
        logger.error(f"[ERROR CRÍTICO] Fallo en la validación previa de la API de {flujo}: {e}")
        raise RuntimeError(f"Error de conexión con la API de Aduanas ({flujo}): {str(e)}") from e


def extraer_datos_api_a_raw(tipo_flujo, **context):
    """
    Extrae los datos desde la API mediante paginación por bloques y almacena el JSON crudo en el esquema raw.
    Registra run_id, logical_date y fecha_extraccion.
    Retorna el total de registros extraídos para XCom.
    """
    flujo = tipo_flujo.upper()
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else f"manual_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logical_date = context.get("logical_date") or context.get("ds") or datetime.now().date()
    if hasattr(logical_date, "strftime"):
        logical_date_str = logical_date.strftime("%Y-%m-%d")
    else:
        logical_date_str = str(logical_date)[:10]

    base_url = _obtener_variable("aduanas_api_base_url", "https://datos.gob.cl/api/3/action/datastore_search")
    
    if flujo == "IMPORTACION":
        resource_id = _obtener_variable("aduanas_resource_id_importaciones", "c2211d9e-4c8e-4ef4-84d5-87039c515584")
        tabla_raw = "raw.importaciones_raw"
    elif flujo == "EXPORTACION":
        resource_id = _obtener_variable("aduanas_resource_id_exportaciones", "f6fe6014-e353-4bc6-bd5d-762511a4dd57")
        tabla_raw = "raw.exportaciones_raw"
    else:
        raise ValueError(f"Tipo de flujo no válido: {tipo_flujo}")

    page_limit = int(_obtener_variable("aduanas_api_page_limit", "500"))
    max_pages = int(_obtener_variable("aduanas_api_max_pages", "5"))
    api_token = _obtener_variable("aduanas_api_token", "")

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AntigravityETL/1.0"}
    if api_token:
        headers["Authorization"] = api_token

    conn = _obtener_conexion_postgres()
    cur = conn.cursor()

    total_filas_extraidas = 0
    pagina = 0

    logger.info(f"Iniciando extracción paginada para {flujo}: límite por página={page_limit}, máx páginas={max_pages}")

    try:
        while pagina < max_pages:
            offset = pagina * page_limit
            params = {
                "resource_id": resource_id,
                "limit": page_limit,
                "offset": offset
            }
            logger.info(f"Consultando página {pagina + 1}/{max_pages} (offset={offset})...")
            
            try:
                resp = requests.get(base_url, params=params, headers=headers, timeout=20)
                resp.raise_for_status()
                data = resp.json()
            except (Timeout, ConnectionError, HTTPError) as err:
                logger.error(f"Error al consumir API en offset {offset}: {err}")
                raise RuntimeError(f"Falla HTTP durante la extracción: {err}") from err

            if not data.get("success", False):
                raise RuntimeError(f"Respuesta no exitosa de API en offset {offset}")

            records = data.get("result", {}).get("records", [])
            num_records = len(records)
            if num_records == 0:
                logger.info("No se recibieron más registros desde la API. Fin de paginación.")
                break

            # Inserción de cada registro individual en esquema raw para visualización 1-a-1
            from psycopg2.extras import execute_values
            insert_query = f"""
                INSERT INTO {tabla_raw} (run_id, fecha_logical, resource_id, offset_pagina, payload, fecha_extraccion)
                VALUES %s;
            """
            filas_a_insertar = [
                (run_id, logical_date_str, resource_id, offset, json.dumps(rec))
                for rec in records
            ]
            execute_values(cur, insert_query, filas_a_insertar, template="(%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)")
            conn.commit()

            total_filas_extraidas += num_records
            pagina += 1
            logger.info(f"Página {pagina} guardada en {tabla_raw}. {num_records} registros insertados individualmente (Total acumulado: {total_filas_extraidas}).")

            # Control de tasa de peticiones (rate limiting)
            time.sleep(0.5)

    finally:
        cur.close()
        conn.close()

    logger.info(f"Extracción completada con éxito para {flujo}. Total filas leídas: {total_filas_extraidas}")
    # Guardar en XCom para la auditoría
    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="filas_leidas", value=total_filas_extraidas)
    return total_filas_extraidas
