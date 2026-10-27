"""
Configura Conexiones y Variables en Airflow utilizando la API interna de Airflow.
Puede ejecutarse directamente dentro del contenedor de Airflow con:
python /opt/airflow/scripts/init_airflow_resources.py
"""
import os
import sys

def init_resources():
    try:
        from airflow import settings
        from airflow.models import Connection, Variable
    except ImportError:
        print("Airflow no está instalado en este entorno Python. Debe ejecutarse dentro del contenedor Airflow.")
        sys.exit(1)

    session = settings.Session()

    # 1. Configurar Conexión postgres_dw
    conn_id = "postgres_dw"
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if existing_conn:
        session.delete(existing_conn)
        session.commit()

    conn = Connection(
        conn_id=conn_id,
        conn_type="postgres",
        host=os.getenv("POSTGRES_HOST", "postgres"),
        login=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "aduanas_dw_secure_pass"),
        schema=os.getenv("POSTGRES_DB", "aduanas_dw"),
        port=5432,
    )
    session.add(conn)
    session.commit()
    print(f"[OK] Connection '{conn_id}' configurada exitosamente.")

    # 2. Configurar Variables de Airflow
    variables = {
        "aduanas_api_base_url": "https://datos.gob.cl/api/3/action/datastore_search",
        "aduanas_resource_id_importaciones": "c2211d9e-4c8e-4ef4-84d5-87039c515584",
        "aduanas_resource_id_exportaciones": "f6fe6014-e353-4bc6-bd5d-762511a4dd57",
        "aduanas_api_page_limit": "500",
        "aduanas_api_max_pages": "5",
        "aduanas_api_token": ""
    }

    for key, value in variables.items():
        Variable.set(key, value)
        print(f"[OK] Variable '{key}' establecida.")

    session.close()
    print("=== Inicialización de recursos de Airflow completada con éxito ===")

if __name__ == "__main__":
    init_resources()
