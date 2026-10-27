"""
DAG 03: Cálculo Automático de Indicadores de Gestión en Data Mart (SQL).
Disparado automáticamente al finalizar dag_01 o dag_02.
Recalcula de forma idempotente las 5 tablas de KPIs en el esquema mart y registra auditoría.
"""
import os
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator

try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except ImportError:
    PostgresHook = None

from src.audit import registrar_auditoria_exito, registrar_falla_callback

default_args = {
    "owner": "analitica_negocio",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
    "execution_timeout": timedelta(minutes=5),
    "on_failure_callback": registrar_falla_callback,
}


def ejecutar_indicadores_sql(**context):
    """Lee y ejecuta el script sql/02_indicadores_mart.sql en la base de datos."""
    rutas_candidatas = [
        Path("/opt/airflow/sql/02_indicadores_mart.sql"),
        Path(__file__).resolve().parent.parent / "sql" / "02_indicadores_mart.sql"
    ]
    sql_path = None
    for r in rutas_candidatas:
        if r.exists():
            sql_path = r
            break

    if not sql_path:
        raise FileNotFoundError("No se encontró el archivo '02_indicadores_mart.sql'.")

    with open(sql_path, "r", encoding="utf-8") as f:
        sql_content = f.read()

    if PostgresHook:
        hook = PostgresHook(postgres_conn_id="postgres_dw")
        conn = hook.get_conn()
    else:
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "aduanas_dw_secure_pass"),
            dbname=os.getenv("POSTGRES_DB", "aduanas_dw")
        )

    cur = conn.cursor()
    try:
        cur.execute(sql_content)
        conn.commit()
    finally:
        cur.close()
        conn.close()

    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="filas_leidas", value=5)
        ti.xcom_push(key="filas_cargadas", value=5)
        ti.xcom_push(key="porcentaje_nulos", value=0.0)


with DAG(
    dag_id="dag_03_indicadores_mart",
    default_args=default_args,
    description="Cálculo automático de los 5 Indicadores de Gestión en esquema mart",
    schedule_interval=None,  # Disparado automáticamente por TriggerDagRunOperator
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aduanas", "indicadores", "mart", "kpi", "inf-522"],
) as dag:

    tarea_recalcular_kpis = PythonOperator(
        task_id="recalcular_kpis_mart",
        python_callable=ejecutar_indicadores_sql,
    )

    tarea_auditar_kpis = PythonOperator(
        task_id="registrar_auditoria_kpis",
        python_callable=registrar_auditoria_exito,
        op_kwargs={"fuente": "MART_INDICADORES_SQL"},
    )

    tarea_recalcular_kpis >> tarea_auditar_kpis
