"""
DAG 01: ETL de Operaciones de Importaciones de Aduanas de Chile.
Orquestación desacoplada, con validación previa de API, reintentos breves para demo,
persistencia cruda, normalización a staging, carga idempotente en DW, auditoría y disparo de KPIs.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from src.extract import validar_api_disponible, extraer_datos_api_a_raw
from src.transform import transformar_datos_a_staging
from src.load import cargar_datos_a_dw
from src.audit import registrar_auditoria_exito, registrar_falla_callback

default_args = {
    "owner": "ingenieria_datos",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
    "execution_timeout": timedelta(minutes=5),
    "on_failure_callback": registrar_falla_callback,
}

with DAG(
    dag_id="dag_01_etl_importaciones",
    default_args=default_args,
    description="ETL independiente de Importaciones de Aduanas de Chile desde datos.gob.cl",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aduanas", "importaciones", "etl", "inf-522"],
) as dag:

    # 1. Validación previa de la API
    tarea_validar_api = PythonOperator(
        task_id="validar_api",
        python_callable=validar_api_disponible,
        op_kwargs={"tipo_flujo": "IMPORTACION"},
    )

    # 2. Extracción paginada hacia esquema raw
    tarea_extraer_a_raw = PythonOperator(
        task_id="extraer_a_raw",
        python_callable=extraer_datos_api_a_raw,
        op_kwargs={"tipo_flujo": "IMPORTACION"},
    )

    # 3. Limpieza, validación de calidad y paso a staging
    tarea_transformar_a_staging = PythonOperator(
        task_id="transformar_a_staging",
        python_callable=transformar_datos_a_staging,
        op_kwargs={"tipo_flujo": "IMPORTACION"},
    )

    # 4. Carga idempotente en modelo dimensional DW (ON CONFLICT)
    tarea_cargar_a_dw = PythonOperator(
        task_id="cargar_a_dw",
        python_callable=cargar_datos_a_dw,
        op_kwargs={"tipo_flujo": "IMPORTACION"},
    )

    # 5. Registro de auditoría exitosa en public.etl_audit
    tarea_registrar_auditoria = PythonOperator(
        task_id="registrar_auditoria",
        python_callable=registrar_auditoria_exito,
        op_kwargs={"fuente": "API_IMPORTACIONES_ADUANAS"},
    )

    # 6. Disparo automático desacoplado del DAG de KPIs
    tarea_disparar_indicadores = TriggerDagRunOperator(
        task_id="disparar_dag_indicadores",
        trigger_dag_id="dag_03_indicadores_mart",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # Flujo de dependencias
    (
        tarea_validar_api
        >> tarea_extraer_a_raw
        >> tarea_transformar_a_staging
        >> tarea_cargar_a_dw
        >> tarea_registrar_auditoria
        >> tarea_disparar_indicadores
    )
