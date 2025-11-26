#MONGODB CONFIG
db_name = "practica_final_mongodb"
db_uri = 'mongodb://localhost:27017/'


#ruta donde se encuentran los arvhivos con los datos en tu sistema
ruta_data = r'C:\Users\pablo\Desktop\Master\GestionAlmacenamientoBigData\PracticaFinal\data'

#CLICKHOUSE CONFIG
#ruta al fichero de configuracion de clickhouse
config_file = r'C:\Users\pablo\Desktop\Master\GestionAlmacenamientoBigData\PracticaFinal\config.json'

#Para la configuración de la conexión a ClickHouse, crea un fichero config.json con el siguiente formato:
#y añade tus credenciales de conexión.
'''
{
    "host": "your_clickhouse_host",
    "port": your_clickhouse_port,
    "username": "your_username",
    "password": "your_password",
    "secure": true_or_false
}
'''
