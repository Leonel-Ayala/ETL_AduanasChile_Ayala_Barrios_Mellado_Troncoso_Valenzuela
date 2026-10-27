"""
Módulo de Carga: Poblado idempotente del Data Warehouse (dw).
Implementa:
- Transacción atómica en PostgreSQL.
- Poblado idempotente de las 4 dimensiones (dim_tiempo, dim_aduana, dim_pais, dim_via_transporte) usando DISTINCT ON.
- Carga idempotente de la tabla de hechos (dw.fact_comercio_exterior) con DISTINCT ON y ON CONFLICT DO UPDATE.
- Retorno de métrica exacta de filas_cargadas vía XCom para auditoría.
"""
import logging
from datetime import datetime

try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except ImportError:
    PostgresHook = None

logger = logging.getLogger("airflow.task")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = logging.getLogger("etl_load")


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


def cargar_datos_a_dw(tipo_flujo, **context):
    """
    Carga los datos limpios desde staging hacia el modelo dimensional dw de forma idempotente.
    """
    flujo = tipo_flujo.upper()
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else f"manual_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    conn = _obtener_conexion_postgres()
    cur = conn.cursor()

    logger.info(f"Iniciando carga idempotente en DW para flujo '{flujo}' (run_id='{run_id}')...")

    try:
        # Verificar cuántos registros hay en staging para este run_id
        cur.execute(
            "SELECT COUNT(*) FROM staging.stg_operaciones_aduanas WHERE tipo_flujo = %s AND run_id = %s;",
            (flujo, run_id)
        )
        total_staging = cur.fetchone()[0]

        if total_staging == 0:
            logger.warning(f"No hay registros con run_id='{run_id}'. Cargando registros recientes de staging...")
            filtro_run = ""
        else:
            filtro_run = "AND stg.run_id = %(run_id)s"

        # ----------------------------------------------------------------------
        # 1. POBLADO IDEMPOTENTE DE DIMENSIONES (DISTINCT ON para evitar colisión)
        # ----------------------------------------------------------------------

        # A) Dimensión Tiempo
        sql_dim_tiempo = f"""
            INSERT INTO dw.dim_tiempo (id_tiempo, fecha, anio, mes, nombre_mes, trimestre, semestre)
            SELECT DISTINCT ON (
                (EXTRACT(YEAR FROM stg.fecha_operacion) * 10000 + EXTRACT(MONTH FROM stg.fecha_operacion) * 100 + EXTRACT(DAY FROM stg.fecha_operacion))::INT
            )
                (EXTRACT(YEAR FROM stg.fecha_operacion) * 10000 + EXTRACT(MONTH FROM stg.fecha_operacion) * 100 + EXTRACT(DAY FROM stg.fecha_operacion))::INT AS id_tiempo,
                stg.fecha_operacion AS fecha,
                stg.anio,
                stg.mes,
                CASE stg.mes
                    WHEN 1 THEN 'Enero' WHEN 2 THEN 'Febrero' WHEN 3 THEN 'Marzo'
                    WHEN 4 THEN 'Abril' WHEN 5 THEN 'Mayo' WHEN 6 THEN 'Junio'
                    WHEN 7 THEN 'Julio' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Septiembre'
                    WHEN 10 THEN 'Octubre' WHEN 11 THEN 'Noviembre' WHEN 12 THEN 'Diciembre'
                END AS nombre_mes,
                EXTRACT(QUARTER FROM stg.fecha_operacion)::INT AS trimestre,
                CASE WHEN stg.mes <= 6 THEN 1 ELSE 2 END AS semestre
            FROM staging.stg_operaciones_aduanas stg
            WHERE stg.tipo_flujo = %(tipo_flujo)s {filtro_run}
            ORDER BY 1
            ON CONFLICT (id_tiempo) DO UPDATE SET
                nombre_mes = EXCLUDED.nombre_mes,
                trimestre = EXCLUDED.trimestre,
                semestre = EXCLUDED.semestre;
        """
        cur.execute(sql_dim_tiempo, {"tipo_flujo": flujo, "run_id": run_id})

        # B) Dimensión Aduana
        sql_dim_aduana = f"""
            INSERT INTO dw.dim_aduana (codigo_aduana, nombre_aduana, zona_geografica)
            SELECT DISTINCT ON (stg.codigo_aduana)
                stg.codigo_aduana,
                stg.nombre_aduana,
                CASE
                    WHEN stg.codigo_aduana IN (6, 7, 14, 31) THEN 'NORTE'
                    WHEN stg.codigo_aduana IN (33, 34, 48, 24) THEN 'CENTRO'
                    WHEN stg.codigo_aduana IN (40, 42) THEN 'SUR'
                    WHEN stg.codigo_aduana IN (45) THEN 'AUSTRAL'
                    ELSE 'OTRA'
                END AS zona_geografica
            FROM staging.stg_operaciones_aduanas stg
            WHERE stg.tipo_flujo = %(tipo_flujo)s {filtro_run}
            ORDER BY stg.codigo_aduana
            ON CONFLICT (codigo_aduana) DO UPDATE SET
                nombre_aduana = EXCLUDED.nombre_aduana,
                zona_geografica = EXCLUDED.zona_geografica;
        """
        cur.execute(sql_dim_aduana, {"tipo_flujo": flujo, "run_id": run_id})

        # C) Dimensión País
        sql_dim_pais = f"""
            INSERT INTO dw.dim_pais (codigo_pais, nombre_pais, continente_region)
            SELECT DISTINCT ON (stg.codigo_pais)
                stg.codigo_pais,
                stg.nombre_pais,
                CASE
                    WHEN stg.nombre_pais IN ('CHINA', 'JAPON', 'COREA DEL SUR', 'INDIA') THEN 'ASIA'
                    WHEN stg.nombre_pais IN ('ESTADOS UNIDOS', 'CANADA', 'MEXICO') THEN 'AMERICA DEL NORTE'
                    WHEN stg.nombre_pais IN ('BRASIL', 'ARGENTINA', 'PERU', 'COLOMBIA') THEN 'AMERICA DEL SUR'
                    WHEN stg.nombre_pais IN ('ALEMANIA', 'ESPANA', 'ESPAÑA', 'FRANCIA', 'BELGICA', 'ITALIA', 'PAISES BAJOS') THEN 'EUROPA'
                    ELSE 'INTERNACIONAL'
                END AS continente_region
            FROM staging.stg_operaciones_aduanas stg
            WHERE stg.tipo_flujo = %(tipo_flujo)s {filtro_run}
            ORDER BY stg.codigo_pais
            ON CONFLICT (codigo_pais) DO UPDATE SET
                nombre_pais = EXCLUDED.nombre_pais,
                continente_region = EXCLUDED.continente_region;
        """
        cur.execute(sql_dim_pais, {"tipo_flujo": flujo, "run_id": run_id})

        # D) Dimensión Vía de Transporte
        sql_dim_via = f"""
            INSERT INTO dw.dim_via_transporte (codigo_via_transporte, nombre_via_transporte)
            SELECT DISTINCT ON (stg.codigo_via_transporte)
                stg.codigo_via_transporte,
                stg.nombre_via_transporte
            FROM staging.stg_operaciones_aduanas stg
            WHERE stg.tipo_flujo = %(tipo_flujo)s {filtro_run}
            ORDER BY stg.codigo_via_transporte
            ON CONFLICT (codigo_via_transporte) DO UPDATE SET
                nombre_via_transporte = EXCLUDED.nombre_via_transporte;
        """
        cur.execute(sql_dim_via, {"tipo_flujo": flujo, "run_id": run_id})

        logger.info("Dimensiones pobladas/actualizadas exitosamente.")

        # ----------------------------------------------------------------------
        # 2. POBLADO IDEMPOTENTE DE TABLA DE HECHOS (dw.fact_comercio_exterior)
        # ----------------------------------------------------------------------
        sql_fact = f"""
            INSERT INTO dw.fact_comercio_exterior (
                codigo_operacion, id_tiempo, id_aduana, id_pais, id_via_transporte,
                tipo_flujo, peso_bruto_kg, peso_toneladas, cantidad_bultos,
                monto_fob_usd, monto_flete_usd, monto_seguro_usd, monto_cif_usd,
                costo_logistico_usd, fecha_carga_dw
            )
            SELECT DISTINCT ON (stg.codigo_operacion)
                stg.codigo_operacion,
                t.id_tiempo,
                a.id_aduana,
                p.id_pais,
                v.id_via_transporte,
                stg.tipo_flujo,
                stg.peso_bruto_kg,
                ROUND((stg.peso_bruto_kg / 1000.0), 4) AS peso_toneladas,
                stg.cantidad_bultos,
                stg.monto_fob_usd,
                stg.monto_flete_usd,
                stg.monto_seguro_usd,
                stg.monto_cif_usd,
                stg.costo_logistico_usd,
                CURRENT_TIMESTAMP
            FROM staging.stg_operaciones_aduanas stg
            JOIN dw.dim_tiempo t ON t.fecha = stg.fecha_operacion
            JOIN dw.dim_aduana a ON a.codigo_aduana = stg.codigo_aduana
            JOIN dw.dim_pais p ON p.codigo_pais = stg.codigo_pais
            JOIN dw.dim_via_transporte v ON v.codigo_via_transporte = stg.codigo_via_transporte
            WHERE stg.tipo_flujo = %(tipo_flujo)s {filtro_run}
            ORDER BY stg.codigo_operacion
            ON CONFLICT (codigo_operacion) DO UPDATE SET
                peso_bruto_kg = EXCLUDED.peso_bruto_kg,
                peso_toneladas = EXCLUDED.peso_toneladas,
                cantidad_bultos = EXCLUDED.cantidad_bultos,
                monto_fob_usd = EXCLUDED.monto_fob_usd,
                monto_flete_usd = EXCLUDED.monto_flete_usd,
                monto_seguro_usd = EXCLUDED.monto_seguro_usd,
                monto_cif_usd = EXCLUDED.monto_cif_usd,
                costo_logistico_usd = EXCLUDED.costo_logistico_usd,
                fecha_carga_dw = CURRENT_TIMESTAMP;
        """
        cur.execute(sql_fact, {"tipo_flujo": flujo, "run_id": run_id})
        filas_afectadas = cur.rowcount
        conn.commit()

        logger.info(f"Carga en dw.fact_comercio_exterior completada. {filas_afectadas} hechos insertados/actualizados.")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error durante la carga a DW: {e}")
        raise
    finally:
        cur.close()
        conn.close()

    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="filas_cargadas", value=filas_afectadas)

    return filas_afectadas
