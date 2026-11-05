"""
Conexión centralizada a PostgreSQL:
- Dentro de Airflow: utiliza PostgresHook('postgres_dw').
- Fuera de Airflow: utiliza variables de entorno obligatorias (sin contraseñas escritas en el código).
"""
import os

try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except ImportError:
    PostgresHook = None


def obtener_conexion():
    """
    Retorna una conexión activa a la base de datos PostgreSQL.
    Lanza RuntimeError si faltan variables de entorno requeridas al ejecutarse fuera de Airflow.
    """
    if PostgresHook:
        try:
            return PostgresHook(postgres_conn_id="postgres_dw").get_conn()
        except Exception:
            pass

    import psycopg2
    faltantes = [v for v in ("POSTGRES_HOST", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB")
                 if not os.getenv(v)]
    if faltantes:
        raise RuntimeError(f"Faltan variables de entorno requeridas: {', '.join(faltantes)}. Revise su archivo .env")

    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )

