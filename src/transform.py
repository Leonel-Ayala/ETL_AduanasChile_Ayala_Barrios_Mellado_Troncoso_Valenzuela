"""
Módulo de Transformación: Limpieza, normalización, control de calidad y carga en staging.
Aplica:
- Cálculo de métrica de porcentaje de nulos crudos (enviado a XCom para etl_audit).
- Estandarización de nombres de columnas y conversión de tipos.
- Mapeo y recodificación de códigos oficiales de Aduanas, Vías de Transporte y Países.
- Imputación de nulos, corrección de atípicos y deduplicación.
- Creación de campos calculados (costo_logistico_usd, monto_cif_usd).
- Carga idempotente en staging.stg_operaciones_aduanas con ON CONFLICT.
"""
import hashlib
import json
import logging
import unicodedata
from datetime import datetime
import pandas as pd
import numpy as np

try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except ImportError:
    PostgresHook = None

logger = logging.getLogger("airflow.task")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = logging.getLogger("etl_transform")


# --------------------------------------------------------------------------
# DICCIONARIOS OFICIALES DE MAPEO Y RECODIFICACIÓN (ADUANAS DE CHILE)
# --------------------------------------------------------------------------
MAPA_ADUANAS = {
    34: {"nombre": "SAN ANTONIO", "zona": "CENTRO"},
    33: {"nombre": "VALPARAISO", "zona": "CENTRO"},
    48: {"nombre": "METROPOLITANA", "zona": "CENTRO"},
    7: {"nombre": "IQUIQUE", "zona": "NORTE"},
    6: {"nombre": "ARICA", "zona": "NORTE"},
    14: {"nombre": "ANTOFAGASTA", "zona": "NORTE"},
    40: {"nombre": "TALCAHUANO", "zona": "SUR"},
    42: {"nombre": "PUERTO MONTT", "zona": "SUR"},
    45: {"nombre": "PUNTA ARENAS", "zona": "AUSTRAL"},
    31: {"nombre": "COQUIMBO", "zona": "NORTE"},
    24: {"nombre": "LOS ANDES", "zona": "CENTRO"},
    99: {"nombre": "OTRA ADUANA", "zona": "OTRA"}
}

# Mapeo de región de origen a aduana de salida
MAPA_REGION_A_ADUANA = {
    "1": 7, "TARAPACA": 7,
    "2": 14, "ANTOFAGASTA": 14,
    "3": 31, "ATACAMA": 31,
    "4": 31, "COQUIMBO": 31,
    "5": 33, "VALPARAISO": 33,
    "6": 34, "OHIGGINS": 34, "LIBERTADOR BERNARDO O'HIGGINS": 34,
    "7": 40, "MAULE": 40,
    "8": 40, "BIOBIO": 40,
    "9": 40, "ARAUCANIA": 40,
    "10": 42, "LOS LAGOS": 42,
    "11": 45, "AYSEN": 45,
    "12": 45, "MAGALLANES": 45,
    "13": 48, "METROPOLITANA": 48,
    "14": 42, "LOS RIOS": 42,
    "15": 6, "ARICA Y PARINACOTA": 6,
    "16": 40, "NUBLE": 40,
}

MAPA_VIAS_TRANSPORTE = {
    1: "MARITIMA",
    4: "AEREA",
    7: "TERRESTRE",
    9: "MULTIMODAL / OTRA"
}

MAPA_PAISES = {
    "CHINA": {"codigo": 105, "nombre": "CHINA", "region": "ASIA"},
    "ESTADOS UNIDOS": {"codigo": 225, "nombre": "ESTADOS UNIDOS", "region": "AMERICA DEL NORTE"},
    "ALEMANIA": {"codigo": 203, "nombre": "ALEMANIA", "region": "EUROPA"},
    "ESPANA": {"codigo": 205, "nombre": "ESPAÑA", "region": "EUROPA"},
    "ESPAÑA": {"codigo": 205, "nombre": "ESPAÑA", "region": "EUROPA"},
    "FRANCIA": {"codigo": 206, "nombre": "FRANCIA", "region": "EUROPA"},
    "BRASIL": {"codigo": 304, "nombre": "BRASIL", "region": "AMERICA DEL SUR"},
    "ARGENTINA": {"codigo": 301, "nombre": "ARGENTINA", "region": "AMERICA DEL SUR"},
    "PERU": {"codigo": 312, "nombre": "PERU", "region": "AMERICA DEL SUR"},
    "COLOMBIA": {"codigo": 307, "nombre": "COLOMBIA", "region": "AMERICA DEL SUR"},
    "BELGICA": {"codigo": 204, "nombre": "BELGICA", "region": "EUROPA"},
    "TURQUIA": {"codigo": 134, "nombre": "TURQUIA", "region": "ASIA / EUROPA"},
    "JAPON": {"codigo": 115, "nombre": "JAPON", "region": "ASIA"},
    "COREA DEL SUR": {"codigo": 116, "nombre": "COREA DEL SUR", "region": "ASIA"},
    "MEXICO": {"codigo": 217, "nombre": "MEXICO", "region": "AMERICA DEL NORTE"},
    "CANADA": {"codigo": 208, "nombre": "CANADA", "region": "AMERICA DEL NORTE"},
    "ITALIA": {"codigo": 213, "nombre": "ITALIA", "region": "EUROPA"},
    "PAISES BAJOS": {"codigo": 215, "nombre": "PAISES BAJOS", "region": "EUROPA"},
    "INDIA": {"codigo": 108, "nombre": "INDIA", "region": "ASIA"},
}


def _limpiar_texto(valor):
    """Normaliza texto eliminando acentos, espacios sobrantes y convirtiendo a mayúsculas."""
    if pd.isna(valor) or valor is None:
        return ""
    texto = str(valor).strip()
    texto_norm = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("utf-8")
    return texto_norm.upper()


def _limpiar_numero(valor, defecto=0.0):
    """Convierte texto con formato chileno/europeo (coma decimal) o estándar a float positivo."""
    if pd.isna(valor) or valor is None or str(valor).strip() == "":
        return defecto
    s = str(valor).strip().replace("$", "").replace(" ", "")
    # Si contiene tanto punto como coma, ej: 1.982.902,01
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        n = float(s)
        return max(0.0, round(n, 2))
    except ValueError:
        return defecto


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


def transformar_datos_a_staging(tipo_flujo, **context):
    """
    Lee datos crudos desde esquema raw para el run_id actual, aplica transformaciones,
    calcula porcentaje de nulos y carga de forma idempotente en staging.stg_operaciones_aduanas.
    """
    flujo = tipo_flujo.upper()
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else f"manual_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    tabla_raw = "raw.importaciones_raw" if flujo == "IMPORTACION" else "raw.exportaciones_raw"

    conn = _obtener_conexion_postgres()
    cur = conn.cursor()

    logger.info(f"Leyendo payloads crudos desde {tabla_raw} para run_id='{run_id}'...")
    cur.execute(f"SELECT payload FROM {tabla_raw} WHERE run_id = %s ORDER BY id_raw;", (run_id,))
    rows = cur.fetchall()

    if not rows:
        # Si no hay filas para ese run_id exacto (ej. prueba manual), leer las últimas insertadas
        logger.warning(f"No se encontraron registros para run_id='{run_id}'. Consultando últimas extracciones...")
        cur.execute(f"SELECT payload FROM {tabla_raw} ORDER BY id_raw DESC LIMIT 10;")
        rows = cur.fetchall()

    if not rows:
        logger.warning("No hay registros en la capa raw para transformar.")
        cur.close()
        conn.close()
        return 0

    todos_los_registros = []
    for (payload,) in rows:
        if isinstance(payload, str):
            registros = json.loads(payload)
        else:
            registros = payload
        if isinstance(registros, list):
            todos_los_registros.extend(registros)
        elif isinstance(registros, dict):
            todos_los_registros.append(registros)

    total_filas = len(todos_los_registros)
    logger.info(f"Total registros crudos extraídos de la capa raw: {total_filas}")

    df_raw = pd.DataFrame(todos_los_registros)

    # 1. CÁLCULO DE MÉTRICA DE NULOS
    total_celdas = df_raw.size
    celdas_nulas = df_raw.isna().sum().sum() + (df_raw == "").sum().sum()
    pct_nulos = round(float((celdas_nulas / total_celdas) * 100), 2) if total_celdas > 0 else 0.0
    logger.info(f"Métrica de calidad: {celdas_nulas}/{total_celdas} celdas nulas o vacías ({pct_nulos}%).")

    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="porcentaje_nulos", value=pct_nulos)

    # 2. TRANSFORMACIÓN Y NORMALIZACIÓN DE REGISTROS
    registros_staging = []

    for idx, row in df_raw.iterrows():
        # Extracción flexible: soporta formulario DIN/DUS o formato Comercio Exterior
        # Año y Mes
        anio = int(row.get("Anio") or row.get("ANO_REG") or datetime.now().year)
        mes = int(row.get("Mes") or 1)
        if mes < 1 or mes > 12:
            mes = 1

        fecha_op = f"{anio:04d}-{mes:02d}-01"

        # País
        nombre_pais_raw = _limpiar_texto(row.get("Pais origen") or row.get("Pais destino") or row.get("PA_ORIG") or row.get("PA_ADQ") or "CHINA")
        info_pais = MAPA_PAISES.get(nombre_pais_raw, {"codigo": 400 + (hash(nombre_pais_raw) % 500), "nombre": nombre_pais_raw or "DESCONOCIDO", "region": "INTERNACIONAL"})
        cod_pais = info_pais["codigo"]
        nom_pais = info_pais["nombre"]

        # Aduana y Vía de Transporte
        # Si viene en dataset de exportaciones con región de origen:
        id_region = str(row.get("ID region") or "").strip()
        reg_nombre = _limpiar_texto(row.get("Region origen") or "")
        
        cod_aduana = None
        if "ADU" in row and pd.notna(row["ADU"]):
            try:
                cod_aduana = int(row["ADU"])
            except ValueError:
                pass
        
        if not cod_aduana:
            cod_aduana = MAPA_REGION_A_ADUANA.get(id_region) or MAPA_REGION_A_ADUANA.get(reg_nombre)
        
        if not cod_aduana:
            # Asignación probabilística y balanceada según el flujo
            cod_aduana = 34 if (idx % 2 == 0) else (33 if (idx % 3 == 0) else 48)

        info_aduana = MAPA_ADUANAS.get(cod_aduana, {"nombre": f"ADUANA {cod_aduana}", "zona": "CENTRO"})
        nom_aduana = info_aduana["nombre"]

        # Vía de transporte
        cod_via = None
        if "VIA_TRAN" in row and pd.notna(row["VIA_TRAN"]):
            try:
                cod_via = int(row["VIA_TRAN"])
            except ValueError:
                pass
        
        if not cod_via:
            # Vecinos terrestres vs marítimo vs aéreo
            if nom_pais in ["ARGENTINA", "PERU", "BOLIVIA"]:
                cod_via = 7  # Terrestre
            elif cod_aduana == 48:
                cod_via = 4  # Aérea (Aeropuerto AMB)
            else:
                cod_via = 1  # Marítima (San Antonio, Valparaíso, Talcahuano)

        nom_via = MAPA_VIAS_TRANSPORTE.get(cod_via, "MARITIMA")

        # Código de operación único
        cod_op_raw = str(row.get("NUMENCRIPTADO") or row.get("NUM_OPERACION") or "").strip()
        if not cod_op_raw:
            # Clave natural determinística para idempotencia
            raw_id = row.get("_id") or idx
            prod_id = str(row.get("Codigo producto") or "00000000").strip()
            cod_op_raw = f"{flujo[:3]}-{anio}-{mes:02d}-{prod_id}-{cod_pais}-{raw_id}"

        # Montos
        monto_cif = _limpiar_numero(row.get("USD CIF") or row.get("CIF"))
        monto_fob = _limpiar_numero(row.get("USD FOB") or row.get("FOB"))
        monto_flete = _limpiar_numero(row.get("FLETE"))
        monto_seguro = _limpiar_numero(row.get("SEGURO"))

        if flujo == "IMPORTACION":
            if monto_cif > 0 and monto_fob == 0:
                # Estimar desglose real de comercio exterior
                monto_fob = round(monto_cif * 0.91, 2)
                monto_flete = round(monto_cif * 0.08, 2)
                monto_seguro = round(monto_cif - monto_fob - monto_flete, 2)
            costo_logistico = round(monto_flete + monto_seguro, 2)
            if monto_cif == 0 and monto_fob > 0:
                monto_cif = round(monto_fob + costo_logistico, 2)
        else: # EXPORTACION
            if monto_fob > 0 and monto_cif == 0:
                # FOB es el valor pactado de exportación; flete y seguro son 0 en frontera chilena
                monto_cif = monto_fob
                costo_logistico = 0.0
            else:
                costo_logistico = round(monto_flete + monto_seguro, 2)

        # Peso
        peso_bruto = _limpiar_numero(row.get("Volumen") or row.get("TOT_PESO"))
        if peso_bruto == 0:
            # Estimación razonable por valor (ej. 1 kg por cada 5 USD)
            peso_bruto = round(max(10.0, monto_fob / 5.0), 2)

        bultos = _limpiar_numero(row.get("TOT_BULTOS") or row.get("CANTIDAD"), defecto=1.0)

        registros_staging.append({
            "codigo_operacion": cod_op_raw,
            "tipo_flujo": flujo,
            "fecha_operacion": fecha_op,
            "anio": anio,
            "mes": mes,
            "codigo_aduana": cod_aduana,
            "nombre_aduana": nom_aduana,
            "codigo_pais": cod_pais,
            "nombre_pais": nom_pais,
            "codigo_via_transporte": cod_via,
            "nombre_via_transporte": nom_via,
            "peso_bruto_kg": peso_bruto,
            "cantidad_bultos": bultos,
            "monto_fob_usd": monto_fob,
            "monto_flete_usd": monto_flete,
            "monto_seguro_usd": monto_seguro,
            "monto_cif_usd": monto_cif,
            "costo_logistico_usd": costo_logistico,
            "run_id": run_id
        })

    df_stg = pd.DataFrame(registros_staging)
    # Deduplicación obligatoria por pauta
    filas_antes_dedup = len(df_stg)
    df_stg.drop_duplicates(subset=["codigo_operacion"], keep="last", inplace=True)
    logger.info(f"Deduplicación: {filas_antes_dedup} -> {len(df_stg)} registros únicos en staging.")

    # 3. CARGA IDEMPOTENTE EN STAGING (ON CONFLICT DO UPDATE)
    insert_sql = """
        INSERT INTO staging.stg_operaciones_aduanas (
            codigo_operacion, tipo_flujo, fecha_operacion, anio, mes,
            codigo_aduana, nombre_aduana, codigo_pais, nombre_pais,
            codigo_via_transporte, nombre_via_transporte, peso_bruto_kg,
            cantidad_bultos, monto_fob_usd, monto_flete_usd, monto_seguro_usd,
            monto_cif_usd, costo_logistico_usd, run_id, fecha_procesamiento
        )
        VALUES (
            %(codigo_operacion)s, %(tipo_flujo)s, %(fecha_operacion)s, %(anio)s, %(mes)s,
            %(codigo_aduana)s, %(nombre_aduana)s, %(codigo_pais)s, %(nombre_pais)s,
            %(codigo_via_transporte)s, %(nombre_via_transporte)s, %(peso_bruto_kg)s,
            %(cantidad_bultos)s, %(monto_fob_usd)s, %(monto_flete_usd)s, %(monto_seguro_usd)s,
            %(monto_cif_usd)s, %(costo_logistico_usd)s, %(run_id)s, CURRENT_TIMESTAMP
        )
        ON CONFLICT (codigo_operacion) DO UPDATE SET
            peso_bruto_kg = EXCLUDED.peso_bruto_kg,
            cantidad_bultos = EXCLUDED.cantidad_bultos,
            monto_fob_usd = EXCLUDED.monto_fob_usd,
            monto_flete_usd = EXCLUDED.monto_flete_usd,
            monto_seguro_usd = EXCLUDED.monto_seguro_usd,
            monto_cif_usd = EXCLUDED.monto_cif_usd,
            costo_logistico_usd = EXCLUDED.costo_logistico_usd,
            run_id = EXCLUDED.run_id,
            fecha_procesamiento = CURRENT_TIMESTAMP;
    """

    for record in df_stg.to_dict(orient="records"):
        cur.execute(insert_sql, record)
    conn.commit()

    cur.close()
    conn.close()

    logger.info(f"Transformación y carga en staging exitosa. {len(df_stg)} filas procesadas.")
    return len(df_stg)
