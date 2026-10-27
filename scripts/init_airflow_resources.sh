#!/usr/bin/env bash
# ==============================================================================
# INICIALIZADOR DE CONEXIONES Y VARIABLES EN APACHE AIRFLOW
# Proyecto: ETL Aduanas de Chile (Importaciones y Exportaciones)
# ==============================================================================
set -e

echo "=== Configurando Conexión a PostgreSQL (postgres_dw) en Airflow ==="
# Eliminar conexión previa si existe para garantizar idempotencia
airflow connections delete 'postgres_dw' 2>/dev/null || true

airflow connections add 'postgres_dw' \
    --conn-type 'postgres' \
    --conn-host "${POSTGRES_HOST:-postgres}" \
    --conn-login "${POSTGRES_USER:-postgres}" \
    --conn-password "${POSTGRES_PASSWORD:-aduanas_dw_secure_pass}" \
    --conn-schema "${POSTGRES_DB:-aduanas_dw}" \
    --conn-port 5432

echo "=== Configurando Variables de Airflow para API de Aduanas ==="
airflow variables set 'aduanas_api_base_url' 'https://datos.gob.cl/api/3/action/datastore_search'
airflow variables set 'aduanas_resource_id_importaciones' 'c2211d9e-4c8e-4ef4-84d5-87039c515584'
airflow variables set 'aduanas_resource_id_exportaciones' 'f6fe6014-e353-4bc6-bd5d-762511a4dd57'
airflow variables set 'aduanas_api_page_limit' '500'
airflow variables set 'aduanas_api_max_pages' '5'
airflow variables set 'aduanas_api_token' ''

echo "=== Conexiones y Variables configuradas exitosamente ==="
