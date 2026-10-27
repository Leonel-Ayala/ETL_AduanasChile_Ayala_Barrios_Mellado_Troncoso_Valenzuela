"""
Módulo de Auditoría y Tolerancia a Fallas.
Registra el historial de ejecuciones en public.etl_audit tanto para flujos exitosos (SUCCESS)
como para fallas controladas o imprevistas (FAILED) mediante el on_failure_callback.
"""
import logging
from datetime import datetime, timezone

try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except ImportError:
    PostgresHook = None

logger = logging.getLogger("airflow.task")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = logging.getLogger("etl_audit")


def _obtener_conexion_postgres():
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


def registrar_auditoria_exito(fuente, **context):
    """
    Tarea final de auditoría en caso de éxito.
    Recupera métricas desde XCom y persiste el registro en public.etl_audit con estado 'SUCCESS'.
    """
    dag_run = context.get("dag_run")
    ti = context.get("ti")
    dag_id = context.get("dag").dag_id if context.get("dag") else "dag_desconocido"
    run_id = dag_run.run_id if dag_run else f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    logical_date = context.get("logical_date") or context.get("ds") or datetime.now().date()
    fecha_logical_str = logical_date.strftime("%Y-%m-%d") if hasattr(logical_date, "strftime") else str(logical_date)[:10]

    # Fecha de inicio
    fecha_inicio = dag_run.start_date if (dag_run and dag_run.start_date) else datetime.now(timezone.utc)
    if hasattr(fecha_inicio, "replace") and fecha_inicio.tzinfo is not None:
        fecha_inicio = fecha_inicio.replace(tzinfo=None)

    fecha_fin = datetime.now()
    duracion = max(0.5, round((fecha_fin - fecha_inicio).total_seconds(), 2))

    # Obtener métricas desde XCom
    filas_leidas = 0
    filas_cargadas = 0
    pct_nulos = 0.0

    if ti:
        filas_leidas = ti.xcom_pull(key="filas_leidas", default=0) or 0
        filas_cargadas = ti.xcom_pull(key="filas_cargadas", default=0) or 0
        pct_nulos = ti.xcom_pull(key="porcentaje_nulos", default=0.0) or 0.0

    conn = _obtener_conexion_postgres()
    cur = conn.cursor()

    sql = """
        INSERT INTO public.etl_audit (
            dag_id, run_id, fuente, fecha_logical, fecha_inicio, fecha_fin,
            duracion_seg, filas_leidas, filas_cargadas, porcentaje_nulos,
            estado, mensaje_error
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    try:
        cur.execute(sql, (
            dag_id,
            run_id,
            fuente,
            fecha_logical_str,
            fecha_inicio,
            fecha_fin,
            duracion,
            int(filas_leidas),
            int(filas_cargadas),
            float(pct_nulos),
            "SUCCESS",
            "Pipeline ejecutado exitosamente sin inconsistencias."
        ))
        conn.commit()
        logger.info(f"[AUDITORÍA EXITOSA] dag_id={dag_id}, run_id={run_id}, leídas={filas_leidas}, cargadas={filas_cargadas}, nulos={pct_nulos}%, duración={duracion}s")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error al registrar auditoría de éxito: {e}")
    finally:
        cur.close()
        conn.close()


def registrar_falla_callback(context):
    """
    Función de retorno on_failure_callback asignada a los DAGs de Airflow.
    Captura excepciones en cualquier tarea que agote sus reintentos e inserta estado 'FAILED' en public.etl_audit.
    """
    dag_run = context.get("dag_run")
    task_instance = context.get("task_instance")
    exception = context.get("exception")

    dag_id = context.get("dag").dag_id if context.get("dag") else "dag_desconocido"
    task_id = task_instance.task_id if task_instance else "task_desconocida"
    run_id = dag_run.run_id if dag_run else f"manual_fail_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    logical_date = context.get("logical_date") or context.get("ds") or datetime.now().date()
    fecha_logical_str = logical_date.strftime("%Y-%m-%d") if hasattr(logical_date, "strftime") else str(logical_date)[:10]

    fecha_inicio = dag_run.start_date if (dag_run and dag_run.start_date) else datetime.now(timezone.utc)
    if hasattr(fecha_inicio, "replace") and fecha_inicio.tzinfo is not None:
        fecha_inicio = fecha_inicio.replace(tzinfo=None)

    fecha_fin = datetime.now()
    duracion = max(0.5, round((fecha_fin - fecha_inicio).total_seconds(), 2))

    mensaje_error = f"Falla en tarea '{task_id}': {str(exception)}"

    logger.error(f"[ALERTA DE AUDITORÍA: FALLA DETECTADA] {mensaje_error} (dag_id={dag_id}, run_id={run_id})")

    try:
        conn = _obtener_conexion_postgres()
        cur = conn.cursor()

        sql = """
            INSERT INTO public.etl_audit (
                dag_id, run_id, fuente, fecha_logical, fecha_inicio, fecha_fin,
                duracion_seg, filas_leidas, filas_cargadas, porcentaje_nulos,
                estado, mensaje_error
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cur.execute(sql, (
            dag_id,
            run_id,
            f"ERROR_TASK_{task_id}",
            fecha_logical_str,
            fecha_inicio,
            fecha_fin,
            duracion,
            0,
            0,
            0.0,
            "FAILED",
            mensaje_error
        ))
        conn.commit()
        cur.close()
        conn.close()
        logger.info("[AUDITORÍA REGISTRADA] Registro de falla persistido exitosamente en public.etl_audit.")
    except Exception as e:
        logger.error(f"Error al escribir registro de falla en public.etl_audit: {e}")
