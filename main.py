# Imports
from datetime import date
import random
from random import randint, choice
import time
import faker
from datetime import datetime
import os
import clickhouse_connect
import json
import pandas as pd
import sys

import mongo as mng
import lakehouseConfig as lakehouseConfig
import bronze_layer as bl
import silver_layer as sl
import gold_layer as gl

def main():
    print("="*50)
    print("🚀 INICIANDO ORQUESTADOR DEL LAKEHOUSE")
    print("="*50)

    # ------------------------------------------------------
    # PASO 1: Carga de Datos Operacionales (MongoDB)
    # ------------------------------------------------------
    print("\n[PASO 1/5] Cargando datos en MongoDB...")
    try:
        mng.load_data_to_mongo() # Llamamos a la función principal del script 1
    except Exception as e:
        print(f"Falló el Paso 1: {e}")
        sys.exit(1) # Detenemos todo si falla la fuente
     #------------------------------------------------------
     #PASO 2: Inicialización de Estructura (ClickHouse DDL)
     #------------------------------------------------------
    print("\n [PASO 2/5] Creando estructura del Lakehouse...")
    try:
        lakehouseConfig.setup_lakehouse() # Llamamos a la función del script 2
    except Exception as e:
        print(f"Falló el Paso 2: {e}")
        sys.exit(1)

    # ------------------------------------------------------
    # PASO 3: Ingesta Capa Bronze (Raw Data)
    # ------------------------------------------------------
    print("\n [PASO 3/5] Ingestando Capa BRONZE...")
    try:
        bl.ingest_bronze() # Llamamos a la función del script 3
    except Exception as e:
        print(f" Falló el Paso 3: {e}")
        sys.exit(1)

    # ------------------------------------------------------
    # PASO 4: Procesamiento Capa Silver (Clean & Join)
    # ------------------------------------------------------
    print("\n [PASO 4/5] Procesando Capa SILVER...")
    try:
         sl.process_silver()
    except Exception as e:
         print(f"Falló el Paso 4: {e}")
         sys.exit(1)

    # ------------------------------------------------------
    # PASO 5: Agregación Capa Gold 
    # ------------------------------------------------------
    print("\n [PASO 5/5] Ejecutando Capa GOLD...")
    try:
         gl.run_gold_layer()
    except Exception as e:
        print(f" Falló el Paso 5: {e}")
        sys.exit(1)
        
    print("\n" + "="*50)
    print(f" EJECUCIÓN COMPLETADA CON ÉXITO")
    print("="*50)

if __name__ == "__main__":
    main()