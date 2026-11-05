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


from src.db import obtener_conexion

USER_AGENT = "ETL-Aduanas-UCM-INF522/1.0"


def _ejecutar_request_con_backoff(url, params, headers, timeout=20, max_intentos=3):
    """
    Realiza peticiones HTTP gestionando límites de tasa (HTTP 429 Too Many Requests).
    Respeta el encabezado 'Retry-After' y aplica retroceso exponencial ante congestión o errores transitorios.
    """
    intento = 0
    while intento <= max_intentos:
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)

            # Manejo específico del límite de uso (HTTP 429 Too Many Requests)
            if resp.status_code == 429:
                intento += 1
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.strip().isdigit():
                    espera = int(retry_after.strip())
                else:
                    espera = 2 ** intento  # Retroceso exponencial: 2s, 4s, 8s

                logger.warning(
                    f"[RATE LIMIT] HTTP 429 (Too Many Requests). Reintento {intento}/{max_intentos}. "
                    f"Pausa de {espera}s según Retry-After / backoff exponencial..."
                )
                if intento > max_intentos:
                    resp.raise_for_status()
                time.sleep(espera)
                continue

            resp.raise_for_status()
            data = resp.json()
            return data

        except (Timeout, ConnectionError) as err:
            intento += 1
            if intento > max_intentos:
                logger.error(f"[ERROR CONEXIÓN] Intentos agotados ({max_intentos}) ante error de red: {err}")
                raise
            espera = 2 ** intento
            logger.warning(f"Falla temporal de red/timeout ({err}). Reintento {intento}/{max_intentos} en {espera}s...")
            time.sleep(espera)
        except HTTPError as err:
            if resp.status_code == 429 and intento <= max_intentos:
                continue
            raise


def validar_api_disponible(tipo_flujo):
    """
    Tarea de validación previa: comprueba que el endpoint responde exitosamente (HTTP 200 y success: True).
    Si falla, lanza RuntimeError para activar los reintentos de Airflow y registrar la falla en etl_audit.
    """
    flujo = tipo_flujo.upper()
    default_base = _obtener_variable("aduanas_api_base_url", "https://datos.gob.cl/api/3/action/datastore_search")
    
    if flujo == "IMPORTACION":
        base_url = _obtener_variable("aduanas_api_url_importaciones", default_base)
        resource_id = _obtener_variable("aduanas_resource_id_importaciones", "c2211d9e-4c8e-4ef4-84d5-87039c515584")
    elif flujo == "EXPORTACION":
        base_url = _obtener_variable("aduanas_api_url_exportaciones", default_base)
        resource_id = _obtener_variable("aduanas_resource_id_exportaciones", "f6fe6014-e353-4bc6-bd5d-762511a4dd57")
    else:
        raise ValueError(f"Tipo de flujo no válido: {tipo_flujo}")

    api_token = _obtener_variable("aduanas_api_token", "")
    headers = {"User-Agent": USER_AGENT}
    if api_token:
        headers["Authorization"] = api_token

    params = {"resource_id": resource_id, "limit": 1}
    logger.info(f"Validando disponibilidad de API para {flujo}: {base_url} (resource_id={resource_id})")

    try:
        data = _ejecutar_request_con_backoff(base_url, params=params, headers=headers, timeout=10)
        if not data.get("success", False):
            raise RuntimeError(f"La API respondió sin éxito ('success': False): {data.get('error')}")
        
        total = data.get("result", {}).get("total", 0)
        logger.info(f"[VALIDACIÓN EXITOSA] API de {flujo} operativa. Total disponible en origen: {total:,} registros.")
        return True
    except Exception as e:
        logger.error(f"[ERROR CRÍTICO] Fallo en la validación previa de la API de {flujo}: {e}")
        raise RuntimeError(f"Error de conexión con la API de Aduanas ({flujo}): {str(e)}") from e


def extraer_datos_api_a_raw(tipo_flujo, **context):
    """
    Extrae los datos desde la API mediante paginación por bloques y almacena el JSON crudo en el esquema raw.
    Gestiona límites de uso (HTTP 429), paginación completa o controlada, y registra run_id, logical_date y auditoría.
    """
    flujo = tipo_flujo.upper()
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else f"manual_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logical_date = context.get("logical_date") or context.get("ds") or datetime.now().date()
    if hasattr(logical_date, "strftime"):
        logical_date_str = logical_date.strftime("%Y-%m-%d")
    else:
        logical_date_str = str(logical_date)[:10]

    default_base = _obtener_variable("aduanas_api_base_url", "https://datos.gob.cl/api/3/action/datastore_search")
    
    if flujo == "IMPORTACION":
        base_url = _obtener_variable("aduanas_api_url_importaciones", default_base)
        resource_id = _obtener_variable("aduanas_resource_id_importaciones", "c2211d9e-4c8e-4ef4-84d5-87039c515584")
        tabla_raw = "raw.importaciones_raw"
    elif flujo == "EXPORTACION":
        base_url = _obtener_variable("aduanas_api_url_exportaciones", default_base)
        resource_id = _obtener_variable("aduanas_resource_id_exportaciones", "f6fe6014-e353-4bc6-bd5d-762511a4dd57")
        tabla_raw = "raw.exportaciones_raw"
    else:
        raise ValueError(f"Tipo de flujo no válido: {tipo_flujo}")

    page_limit = int(_obtener_variable("aduanas_api_page_limit", "500"))
    max_pages_var = _obtener_variable("aduanas_api_max_pages", "5")
    try:
        max_pages = int(max_pages_var)
    except (ValueError, TypeError):
        max_pages = 5

    api_token = _obtener_variable("aduanas_api_token", "")
    headers = {"User-Agent": USER_AGENT}
    if api_token:
        headers["Authorization"] = api_token

    conn = obtener_conexion()
    cur = conn.cursor()

    # La capa raw es un área transitoria de aterrizaje para el lote actual.
    # Se vacía la tabla correspondiente antes de la extracción para evitar basura histórica intermedia.
    logger.info(f"Limpiando tabla transitoria {tabla_raw} antes de la extracción...")
    cur.execute(f"TRUNCATE TABLE {tabla_raw};")
    conn.commit()

    total_filas_extraidas = 0
    total_en_origen = 0
    pagina = 0

    modo_paginacion = "completa (hasta fin de datos)" if max_pages <= 0 else f"controlada (máx {max_pages} páginas / {max_pages * page_limit} filas)"
    logger.info(f"Iniciando extracción paginada para {flujo}: límite={page_limit}, modo={modo_paginacion}")

    try:
        while True:
            # Control de parada si se configuró un límite de páginas positivo
            if max_pages > 0 and pagina >= max_pages:
                logger.info(f"Se alcanzó el límite configurado de páginas ({max_pages}). Fin de extracción controlada.")
                break

            offset = pagina * page_limit
            params = {
                "resource_id": resource_id,
                "limit": page_limit,
                "offset": offset
            }
            logger.info(f"Consultando página {pagina + 1} (offset={offset})...")
            
            try:
                data = _ejecutar_request_con_backoff(base_url, params=params, headers=headers, timeout=25)
            except Exception as err:
                logger.error(f"Error al consumir API en offset {offset}: {err}")
                raise RuntimeError(f"Falla HTTP/Red durante la extracción: {err}") from err

            if not data.get("success", False):
                raise RuntimeError(f"Respuesta no exitosa de API en offset {offset}: {data.get('error')}")

            result = data.get("result", {})
            total_en_origen = result.get("total", total_en_origen)
            records = result.get("records", [])
            num_records = len(records)
            if num_records == 0:
                logger.info("No se recibieron más registros desde la API. Fin de paginación.")
                break

            # Inserción por lotes en esquema raw
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
            logger.info(
                f"Página {pagina} guardada en {tabla_raw}. {num_records} registros insertados. "
                f"Acumulado: {total_filas_extraidas:,} de {total_en_origen:,} disponibles en origen."
            )

            if total_en_origen > 0 and offset + num_records >= total_en_origen:
                logger.info("Se ha alcanzado la totalidad de registros disponibles en la API.")
                break

            # Control preventivo de tasa de peticiones (rate limiting)
            time.sleep(0.5)

    finally:
        cur.close()
        conn.close()

    es_carga_parcial = bool(total_en_origen > 0 and total_filas_extraidas < total_en_origen)
    resumen_carga = "MUESTRA CONTROLADA" if es_carga_parcial else "CARGA COMPLETA"
    logger.info(
        f"Extracción finalizada para {flujo} ({resumen_carga}): "
        f"{total_filas_extraidas:,} filas extraídas de {total_en_origen:,} disponibles en origen."
    )

    # Guardar métricas en XCom para la auditoría y control de calidad
    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="filas_leidas", value=total_filas_extraidas)
        ti.xcom_push(key="total_disponible_api", value=total_en_origen)
        ti.xcom_push(key="es_carga_parcial", value=es_carga_parcial)

    return total_filas_extraidas
