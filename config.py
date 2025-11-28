# config.py
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ruta_data = os.path.join(BASE_DIR, "data")
config_file = os.path.join(BASE_DIR, "config.json")

db_uri = "mongodb://localhost:27017/"
db_name = "lakehouse"