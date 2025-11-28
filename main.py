# main.py

import sys
import mongo as mng
import lakehouseConfig as lakehouseConfig
import bronze_layer as bl
import silver_layer as sl
import gold_layer as gl


def main():
    print("=" * 60)
    print("🚀 INICIANDO ORQUESTADOR DEL LAKEHOUSE")
    print("=" * 60)

    # ------------------------------------------------------
    # PASO 1: CARGA DE DATOS EN MONGODB
    # ------------------------------------------------------
    print("\n📦 [PASO 1/5] Cargando datos en MongoDB...")
    try:
        mng.load_data_to_mongo()
    except Exception as e:
        print(f"❌ Falló el Paso 1 (MongoDB): {e}")
        sys.exit(1)

    # ------------------------------------------------------
    # PASO 2: CREACIÓN DE ESTRUCTURA DEL LAKEHOUSE (ClickHouse)
    # ------------------------------------------------------
    print("\n🏗️  [PASO 2/5] Inicializando estructura en ClickHouse...")
    try:
        lakehouseConfig.setup_lakehouse()
    except Exception as e:
        print(f"❌ Falló el Paso 2 (Estructura CH): {e}")
        sys.exit(1)

    # ------------------------------------------------------
    # PASO 3: INGESTA BRONZE
    # ------------------------------------------------------
    print("\n🥉 [PASO 3/5] Ingestando datos en Capa BRONZE...")
    try:
        bl.ingest_bronze()
    except Exception as e:
        print(f"❌ Falló el Paso 3 (Bronze): {e}")
        sys.exit(1)

    # ------------------------------------------------------
    # PASO 4: PROCESAMIENTO SILVER (si existe)
    # ------------------------------------------------------
    print("\n🥈 [PASO 4/5] Procesando Capa SILVER...")
    try:
        sl.process_silver()
    except Exception as e:
        print(f"❌ Falló el Paso 4 (Silver): {e}")
        sys.exit(1)

    # ------------------------------------------------------
    # PASO 5: KPIs GOLD (si existe)
    # ------------------------------------------------------
    print("\n🥇 [PASO 5/5] Calculando métricas GOLD...")
    try:
        gl.calculate_gold()
    except Exception as e:
        print(f"❌ Falló el Paso 5 (Gold): {e}")
        sys.exit(1)

    # ------------------------------------------------------
    # FIN
    # ------------------------------------------------------
    print("\n" + "=" * 60)
    print("🏁 EJECUCIÓN COMPLETADA CON ÉXITO")
    print("=" * 60)


if __name__ == "__main__":
    main()