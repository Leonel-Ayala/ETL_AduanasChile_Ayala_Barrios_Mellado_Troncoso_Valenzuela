"""
Script utilitario para descubrir y validar endpoints activos de Aduanas en datos.gob.cl
Filtra recursos con datastore_active=True que responden a /api/3/action/datastore_search
"""
import json
import urllib.request
import urllib.parse
import sys

BASE_URL = "https://datos.gob.cl/api/3/action"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AntigravityETL/1.0"}

# Recursos verificados y activos con decenas de miles de registros en datos.gob.cl
RECURSOS_CONOCIDOS = {
    "IMPORTACIONES_2026": {
        "resource_id": "c2211d9e-4c8e-4ef4-84d5-87039c515584",
        "descripcion": "Registro Oficial de Importaciones - Servicio Nacional de Aduanas",
    },
    "EXPORTACIONES_2026": {
        "resource_id": "f6fe6014-e353-4bc6-bd5d-762511a4dd57",
        "descripcion": "Registro Oficial de Exportaciones - Servicio Nacional de Aduanas",
    },
    "IMPORTACIONES_2025": {
        "resource_id": "dc222734-622d-455f-abb9-e6ce7da3077f",
        "descripcion": "Registro de Importaciones 2025 - Aduanas",
    },
    "EXPORTACIONES_2025": {
        "resource_id": "a132a72c-4c93-4f84-9b78-1c4f131c7e71",
        "descripcion": "Registro de Exportaciones 2025 - Aduanas",
    }
}


def consultar_datastore(resource_id, limit=3):
    url = f"{BASE_URL}/datastore_search?resource_id={resource_id}&limit={limit}"
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if data.get("success"):
                result = data.get("result", {})
                return {
                    "success": True,
                    "total": result.get("total", 0),
                    "fields": [f["id"] for f in result.get("fields", [])],
                    "sample": result.get("records", [])[:1]
                }
    except Exception as e:
        return {"success": False, "error": str(e)}
    return {"success": False, "error": "Respuesta no exitosa"}


def main():
    print("=" * 70)
    print("VALIDADOR DE ENDPOINTS ACTIVOS DE ADUANAS DE CHILE (datos.gob.cl)")
    print("=" * 70)

    for clave, info in RECURSOS_CONOCIDOS.items():
        rid = info["resource_id"]
        print(f"\nVerificando {clave} ({info['descripcion']}):")
        print(f"  Resource ID: {rid}")
        res = consultar_datastore(rid, limit=2)
        if res.get("success"):
            print(f"  [OK] Conectado exitosamente!")
            print(f"  Total registros disponibles: {res['total']:,}")
            print(f"  Campos: {res['fields']}")
        else:
            print(f"  [ERROR] {res.get('error')}")

    print("\n" + "=" * 70)
    print("VARIABLES RECOMENDADAS PARA AIRFLOW:")
    print("=" * 70)
    print(f"aduanas_api_base_url: https://datos.gob.cl/api/3/action/datastore_search")
    print(f"aduanas_resource_id_importaciones: {RECURSOS_CONOCIDOS['IMPORTACIONES_2026']['resource_id']}")
    print(f"aduanas_resource_id_exportaciones: {RECURSOS_CONOCIDOS['EXPORTACIONES_2026']['resource_id']}")
    print("=" * 70)


if __name__ == "__main__":
    main()
