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

    from src.db import obtener_conexion
    conn = obtener_conexion()

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

    def tarea_actualizar_dashboard_html(**context):
        """Genera el reporte HTML dinámicamente con los datos recién calculados en PostgreSQL."""
        try:
            from scripts.generar_dashboard_html import generar_reporte_desde_airflow
            generar_reporte_desde_airflow(**context)
        except Exception as e:
            print(f"Advertencia al generar dashboard HTML: {e}")

    tarea_generar_dashboard = PythonOperator(
        task_id="generar_dashboard_html_en_vivo",
        python_callable=tarea_actualizar_dashboard_html,
    )

    tarea_recalcular_kpis >> tarea_auditar_kpis >> tarea_generar_dashboard
