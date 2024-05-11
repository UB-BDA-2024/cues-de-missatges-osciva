from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime

from shared.mongodb_client import MongoDBClient
from shared.redis_client import RedisClient
from shared.sensors import models, schemas
from shared.timescale import Timescale
from shared.elasticsearch_client import ElasticsearchClient
from shared.cassandra_client import CassandraClient
from . import models, schemas


from datetime import datetime, timedelta
import json




class DataCommand():
    def __init__(self, from_time, to_time, bucket):
        if not from_time or not to_time:
            raise ValueError("from_time and to_time must be provided")
        if not bucket:
            bucket = 'day'
        self.from_time = from_time
        self.to_time = to_time
        self.bucket = bucket


def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()


def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()


# def create_sensor(db: Session, mongodb_clinet: MongoDBClient, sensor: schemas.SensorCreate) -> models.Sensor:
#     # Add sensor to postgres database
#     db_sensor = add_sensor_to_postgres(db, sensor)

#     # Add sensor to mongodb database
#     mongo_sensor = add_sensor_to_mongodb(mongodb_clinet, sensor, db_sensor.id)

#     del mongo_sensor['location']
#     mongo_sensor['latitude'] = sensor.latitude
#     mongo_sensor['longitude'] = sensor.longitude

#     return mongo_sensor


# def add_sensor_to_postgres(db: Session, sensor: schemas.SensorCreate) -> models.Sensor:
#     date = datetime.now()

#     db_sensor = models.Sensor(name=sensor.name, joined_at=date)
#     db.add(db_sensor)
#     db.commit()
#     db.refresh(db_sensor)

#     return db_sensor


# def add_sensor_to_mongodb(mongodb_client: MongoDBClient, db_sensor: schemas.SensorCreate, id):
#     mongo_projection = schemas.SensorMongoProjection(id=id, name=db_sensor.name, location={'type': 'Point',
#                                                                                            'coordinates': [
#                                                                                                db_sensor.longitude,
#                                                                                                db_sensor.latitude]},
#                                                      type=db_sensor.type, mac_address=db_sensor.mac_address,
#                                                      description=db_sensor.description,
#                                                      serie_number=db_sensor.serie_number,
#                                                      firmware_version=db_sensor.firmware_version, model=db_sensor.model,
#                                                      manufacturer=db_sensor.manufacturer)
#     mongodb_client.getDatabase()
#     mongoInsert = mongo_projection.dict()
#     mongodb_client.getCollection().insert_one(mongoInsert)
#     return mongo_projection.dict()


# def record_data(redis: RedisClient, timescale: Timescale, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
#     # We store the recieved data as a JSON string in Redis
#     redis.set(sensor_id, data.json())

#     # We set the data to the timescale database
#     data_to_insert = data.dict()
#     data_to_insert['sensor_id'] = sensor_id
#     data_to_insert['time'] = data_to_insert['last_seen']
#     del data_to_insert['last_seen']

#     query = timescale.generate_insert_query('sensor_data', data_to_insert)
#     print(query)
#     var = timescale.execute(query)
#     print(var)

#     return data


# def getView(bucket: str) -> str:
#     if bucket == 'year':
#         return 'sensor_data_yearly'
#     if bucket == 'month':
#         return 'sensor_data_monthly'
#     if bucket == 'week':
#         return 'sensor_data_weekly'
#     if bucket == 'day':
#         return 'sensor_data_daily'
#     elif bucket == 'hour':
#         return 'sensor_data_hourly'
#     else:
#         raise ValueError("Invalid bucket size")


# def get_data(timescale: Timescale, sensor_id: int, dataCommand: DataCommand) -> schemas.Sensor:
#     # We need to get the bucket to know wich view query on timescale
#     view = getView(dataCommand.bucket)
#     query = f"SELECT * FROM {view} WHERE sensor_id = {sensor_id} AND bucket >= '{dataCommand.from_time}' AND bucket <= '{dataCommand.to_time}'"
#     data = timescale.execute(query, True)
#     return data


# def delete_sensor(db: Session, sensor_id: int):
#     db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
#     if db_sensor is None:
#         raise HTTPException(status_code=404, detail="Sensor not found")
#     db.delete(db_sensor)
#     db.commit()
#     return db_sensor


def create_sensor(db: Session, sensor: schemas.SensorCreate, mongo: MongoDBClient, es: ElasticsearchClient) -> schemas.Sensor:
    db_sensor = models.Sensor(name=sensor.name)   
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    # Obtener la base de datos y la colección de MongoDB
    database = mongo.getDatabase("sensors")
    collection = mongo.getCollection("sensorsdata")

    es = ElasticsearchClient(host="elasticsearch")
    index_name = 'sensors'

    # Verificar si el índice existe en Elasticsearch
    if not es.client.indices.exists(index=index_name):
        # Si el índice no existe, crearlo
        es.create_index(index_name)
        # Definir el mapeo para el índice
        mapping = {
            'properties': {
                'name': {'type': 'keyword'},
                'type': {'type': 'text'},
                'description': {'type': 'text'}
            }
        }
        es.create_mapping('sensors',mapping)
    
    #----Documental Sensor----
    # Preparar la ubicación en formato GeoJSON
    location = {
        "type": "Point",
        "coordinates": [sensor.longitude, sensor.latitude]
    }

    # Crear el documento del sensor
    doc = {
        "id": db_sensor.id, 
        "name": sensor.name,
        "location": location,
        # new: no existien last_seen ni joined_at aquí... al ser nova dada temporal ja no es pot emmagatzemar a redis així que s'emmagatzema a mongo
        # "joined_at": sensor.joined_at,
        # "last_seen": sensor.last_seen,
        "type": sensor.type,
        "mac_address": sensor.mac_address,
        "manufacturer": sensor.manufacturer,
        "model": sensor.model,
        "serie_number": sensor.serie_number,
        "firmware_version": sensor.firmware_version,
        "description": sensor.description
    }

    # Insertar el documento en la colección de MongoDB
    collection.insert_one(doc)
    # Obtener el documento del sensor de MongoDB
    documental_sensor = collection.find_one({"name": db_sensor.name})

    # Preparar el documento para Elasticsearch
    es_doc = {
        'name': documental_sensor["name"],
        'type': documental_sensor["type"],
        'description': documental_sensor["description"]
            
    }
    # Indexar el documento en Elasticsearch
    es.index_document('sensors', es_doc)

    sensor = schemas.Sensor(
        id= db_sensor.id,
        name= db_sensor.name,
        longitude= documental_sensor["location"]["coordinates"][0],
        latitude= documental_sensor["location"]["coordinates"][1],       
        # new: no existien last_seen ni joined_at aquí... al ser nova dada temporal ja no es pot emmagatzemar a redis així que s'emmagatzema a mongo
        # joined_at=documental_sensor["joined_at"],
        # last_seen=documental_sensor["last_seen"],
        type= documental_sensor["type"],
        mac_address= documental_sensor["mac_address"],
        manufacturer= documental_sensor["manufacturer"],
        model= documental_sensor["model"],
        serie_number= documental_sensor["serie_number"],
        firmware_version= documental_sensor["firmware_version"],
        description= documental_sensor["description"],
        # battery_level= None,
        # temperature=None,
        # humidity=None,
        # velocity=None
    )
    return sensor.dict(exclude_none=True)

def record_data(db: Session, redis: RedisClient, mongo: MongoDBClient, sensor_id: int, data: schemas.SensorData, timescale: Timescale,  cassandra: CassandraClient) -> schemas.Sensor:
    
    # Record sensor data in Redis
    temperature_key = f"sensor-{sensor_id}:temperature"
    if data.temperature is not None:
        redis.set(temperature_key, data.temperature)
    
    humidity_key = f"sensor-{sensor_id}:humidity"
    if data.humidity is not None:
        redis.set(humidity_key, data.humidity)

    battery_level_key = f"sensor-{sensor_id}:battery_level"
    redis.set(battery_level_key, data.battery_level)

    # new: 
    last_seen_key = f"sensor-{sensor_id}:last_seen"
    redis.set(last_seen_key, data.last_seen)

    velocity_key = f"sensor-{sensor_id}:velocity"
    if data.velocity is not None:
        redis.set(velocity_key, data.velocity)

    mongo_database = mongo.getDatabase("sensors")
    collection = mongo.getCollection("sensorsdata")

    cassandra.execute("USE sensor")

    # Update sensor data in the database
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")

    documental_sensor = collection.find_one({"name": db_sensor.name})


    # last_seen = redis.get(last_seen_key)
    # battery_level = redis.get(battery_level_key)

    # Obtener la fecha y hora actual
    fecha_datetime = datetime.strptime(data.last_seen, "%Y-%m-%dT%H:%M:%S.%fZ")
    fecha_formateada = fecha_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")

    
    if data.temperature == None:
    
        query = f"""INSERT INTO sensor_data (sensor_id, velocity, battery_level, last_seen)
                VALUES ('{sensor_id}', {data.velocity}, {data.battery_level}, '{data.last_seen}')"""
        timescale.execute(query)

    if data.velocity == None:
    
        query = f"""INSERT INTO sensor_data (sensor_id, temperature, humidity, battery_level, last_seen)
                VALUES ('{sensor_id}', {data.temperature}, {data.humidity},{data.battery_level}, '{data.last_seen}')"""
        timescale.execute(query)
    
    timescale.execute("commit")



    if data.temperature is not None:
        cassandra.get_session().execute("""INSERT INTO temperature (sensor_id, temperature) VALUES (%s, %s)""", (sensor_id, data.temperature))
    
    cassandra.get_session().execute("""INSERT INTO types (type, sensor_id) VALUES (%s, %s)""", (documental_sensor["type"], sensor_id))
    
    cassandra.get_session().execute("""INSERT INTO battery (sensor_id, battery_level) VALUES (%s, %s)""", (sensor_id, data.battery_level))
    
    last_seen = redis.get(last_seen_key)
    battery_level = redis.get(battery_level_key)    

    return schemas.Sensor(
        id=db_sensor.id,
        name=db_sensor.name,
        latitude = documental_sensor["location"]["coordinates"][1],
        longitude = documental_sensor["location"]["coordinates"][0],

        # new: canvi de comments, no es passaven ni last_seen ni joined_at per mongodb
        joined_at=str(db_sensor.joined_at),
        # joined_at=documental_sensor["joined_at"],
        last_seen=last_seen,
        # last_seen=documental_sensor["last_seen"],
        type=documental_sensor["type"],
        mac_address=documental_sensor["mac_address"],
        battery_level=battery_level,
        temperature=data.temperature,
        humidity=data.humidity,
        velocity=data.velocity,
        manufacturer=documental_sensor["manufacturer"],
        model=documental_sensor["model"],
        serie_number=documental_sensor["serie_number"],
        firmware_version=documental_sensor["firmware_version"],
        description=documental_sensor["description"]
    )



def get_data(sensor_id: int, from_date: str, to: str, bucket: str, db: Session, timescale: Timescale):

    # Obtiene los datos del sensor de la base de datos
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    # Obtiene los datos del sensor de Redis
    # temperature_key = f"sensor-{db_sensor.sensor_id}:temperature"
    # humidity_key = f"sensor-{db_sensor.sensor_id}:humidity"
    # battery_level_key = f"sensor-{db_sensor.sensor_id}:battery_level"
    # # new: comentat
    # # last_seen_key = f"sensor-{sensor_id}:last_seen"
    # velocity_key = f"sensor-{db_sensor.sensor_id}:velocity"

    # temperature = redis.get(temperature_key)
    # humidity = redis.get(humidity_key)
    # velocity = redis.get(velocity_key)
    # # new: comentat
    # # last_seen = redis.get(last_seen_key)
    # battery_level = redis.get(battery_level_key)

    # mongo_database = mongo.getDatabase("sensors")
    # collection = mongo.getCollection("sensorsdata")
    # documental_sensor = collection.find_one({"name": db_sensor.name})

    timescale.execute("commit")
    # faig les views aquí i no al migrations_ts.sql perquè no se'm crea la taula sensors_data sinó
    query_create = f"""CREATE MATERIALIZED VIEW IF NOT EXISTS conditions_{bucket}
        WITH (timescaledb.continuous) AS
        SELECT sensor_id,
        time_bucket(INTERVAL '1 {bucket}', last_seen) AS bucket,
        AVG(temperature) AS temp_avg
        FROM sensor_data
        GROUP BY sensor_id, bucket;
        """
    timescale.execute(query_create)
    timescale.execute("commit")
    
    if bucket == "week":
        format = datetime.fromisoformat(from_date[:-1])
        from_date = format - timedelta(days=format.weekday())

    query = f"""
        SELECT *
        FROM conditions_{bucket}
        WHERE sensor_id = {sensor_id}
        AND bucket >= '{from_date}'
        AND bucket <= '{to}';
    """
    timescale.execute(query)
    cursor = timescale.getCursor()
    results = cursor.fetchall()
    return results


    # sensor = schemas.Sensor(
    #     id=db_sensor.sensor_id,
    #     name=documental_sensor["name"],
    #     latitude=documental_sensor["location"]["coordinates"][1],
    #     longitude=documental_sensor["location"]["coordinates"][0],
    #     # joined_at=documental_sensor["joined_at"],
    #     # new: comentat
    #     last_seen=last_seen,
    #     # last_seen=documental_sensor["last_seen"],
    #     type=documental_sensor["type"],
    #     mac_address=documental_sensor["mac_address"],
    #     battery_level=battery_level,
    #     temperature=temperature,
    #     humidity=humidity,
    #     velocity=velocity,
    #     manufacturer=documental_sensor["manufacturer"],
    #     model=documental_sensor["model"],
    #     serie_number=documental_sensor["serie_number"],
    #     firmware_version=documental_sensor["firmware_version"],
    #     description=documental_sensor["description"]
    # )
        

    # return sensor
    

        


def get_new_sensor(sensor_id: int, db: Session, mongo: MongoDBClient) -> schemas.Sensor:
    # Obtiene los datos del sensor de la base de datos
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")

    
    mongo_database = mongo.getDatabase("sensors")
    collection = mongo.getCollection("sensorsdata")
    documental_sensor = collection.find_one({"name": db_sensor.name})


    
    sensor = schemas.Sensor(
        id=db_sensor.id,
        name=db_sensor.name,
        latitude = documental_sensor["location"]["coordinates"][1],
        longitude = documental_sensor["location"]["coordinates"][0],
        #new: no estaven last_seen ni joined_at
        # joined_at= documental_sensor["joined_at"],
        # last_seen= documental_sensor["last_seen"],
        type=documental_sensor["type"],
        mac_address=documental_sensor["mac_address"],
        manufacturer=documental_sensor["manufacturer"],
        model=documental_sensor["model"],
        serie_number=documental_sensor["serie_number"],
        firmware_version=documental_sensor["firmware_version"],
        description=documental_sensor["description"]

    )
    return sensor.dict(exclude_none=True)


def delete_sensor(db: Session, redis: RedisClient, mongo: MongoDBClient, sensor_id: int):
    # Obtiene el sensor de la base de datos
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")

    # Elimina el sensor de la base de datos
    db.delete(db_sensor)
    db.commit()

    # Elimina los registros asociados en Redis
    temperature_key = f"sensor-{sensor_id}:temperature"
    humidity_key = f"sensor-{sensor_id}:humidity"
    velocity_key = f"sensor-{sensor_id}:velocity"
    battery_level_key = f"sensor-{sensor_id}:battery_level"
    #new: no estava comentat
    last_seen_key = f"sensor-{sensor_id}:last_seen"

    redis.delete(temperature_key)
    redis.delete(humidity_key)
    redis.delete(velocity_key)
    redis.delete(battery_level_key)
    #new: no estava comentat
    redis.delete(last_seen_key)

    # database = mongo.getDatabase("sensors")
    database = mongo.getDatabase("data")

    # Elimina los registros asociados en MongoDB
    collection = mongo.getCollection("sensors")
    collection.delete_one({"name": db_sensor.name})

def get_sensors_near(db: Session, redis: RedisClient, mongo: MongoDBClient, latitude: float, longitude: float, radius: float) -> List[schemas.Sensor]:
    
    database = mongo.getDatabase("data")
    # Obtén la colección de MongoDB
    collection = mongo.getCollection("sensors")

    collection.create_index([("location", "2dsphere")])


    # Consulta los sensores cercanos utilizando la función $near
    query = {
        "location": {
            "$near": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude]  
                },
                "$maxDistance": radius
            }
        }
    }

    # Ejecuta la consulta en MongoDB
    nearby_sensors = list(collection.find(query))

    # Lista para almacenar los datos de los sensores cercanos
    nearby_sensor_data = []

    # Itera sobre los sensores cercanos
    for sensor_data in nearby_sensors:
        nom_sensor = sensor_data["name"] # necessari ja que no puc pillar la id
        sensor = get_sensor_by_name(db, nom_sensor) # necessari ja que no puc pillar la id
        sensor_datas = get_data(db=db, redis=redis, mongo=mongo, sensor_id=sensor.id)  
        nearby_sensor_data.append(sensor_datas)

    return nearby_sensor_data


def search_sensors(query: str, db: Session, mongo: MongoDBClient, size: int = 10, search_type: str = "match"):

    # Inicializar cliente Elasticsearch
    es = ElasticsearchClient(host="elasticsearch")
    es_index_name = "sensors"

    # Obtener la base de datos y la colección de MongoDB
    mongo_db = mongo.getDatabase("sensors")
    col = mongo.getCollection("sensorsdata")

    # Convertir la cadena de consulta JSON en un diccionario Python
    query_dict = json.loads(query)
    # Obtener la clave y el valor del primer elemento del diccionario
    clau, valor = next(iter(query_dict.items()))
    
    # Definir la consulta de búsqueda para Elasticsearch
    querySearch = {
        'size' : size,
        'query' : {
            search_type : query_dict
        }

    }


    # Definir la consulta de búsqueda similar para Elasticsearch
    querySearchSimilar = {
        'size' : size,
        'query' : {
            'fuzzy': {
                clau: {
                    'value' : valor
                }
             
            }
        }
    }
    
    

    # Ejecutar la búsqueda en Elasticsearch según el tipo de búsqueda especificado
    if search_type == "similar":
        buscar = es.search(es_index_name, querySearchSimilar)
    else:
        buscar = es.search(es_index_name, querySearch)
    
    # lista donde añadiremos los sensores encontrados
    found_sensors = []

    # Iterar sobre los resultados de la búsqueda
    for hit in buscar['hits']['hits']:
        # Verificar si el campo '_source' está presente en el resultado de la búsqueda y si el campo 'name' está presente dentro del diccionario '_source'
        if '_source' in hit and 'name' in hit['_source']:
            print("hola")
            # Obtener el nombre del sensor de los resultados de la búsqueda en Elasticsearch
            name = hit['_source']['name']
            print(name)
            
            # Obtener el sensor y sus datos
            db_sensor = get_sensor_by_name(db, name)
            print(db_sensor)
            sensor_id = db_sensor.id
            sensor_name = db_sensor.name

            # Obtener el documento del sensor desde la base de datos MongoDB
            documental_sensor = col.find_one({"name":sensor_name})

            
            # location = {
            #     "type": "Point",
            #     "coordinates": [sensor.longitude, sensor.latitude]
            # }

            sensor = schemas.Sensor(
                id=sensor_id,
                name=documental_sensor["name"],
                latitude=documental_sensor["location"]["coordinates"][1],
                longitude=documental_sensor["location"]["coordinates"][0],
                # location=location,
                #new: no estaven last_seen ni joined_at
                # joined_at=documental_sensor["joined_at"],
                # last_seen=documental_sensor["last_seen"],
                type=documental_sensor["type"],
                mac_address=documental_sensor["mac_address"],
                manufacturer=documental_sensor["manufacturer"],
                model=documental_sensor["model"],
                serie_number=documental_sensor["serie_number"],
                firmware_version=documental_sensor["firmware_version"],
                description=documental_sensor["description"]
            )

            # Convertir el sensor a diccionario y añadirlo a la lista de sensores encontrados
            sensor_dict = sensor.dict(exclude_none=True)
            found_sensors.append(sensor_dict)

    es.close()
    return found_sensors

def get_temperature_sensors(db: Session, redis_client: RedisClient, mongo_db: MongoDBClient, cassandra: CassandraClient):
    
    cassandra.execute("USE sensor;")
    data = cassandra.execute("""SELECT sensor_id, MAX(temperature) AS max_temperature, MIN(temperature) AS min_temperature, AVG(temperature) AS average_temperature FROM temperature GROUP BY sensor_id;""")
            
    list = []

    for sensor in data:
        sensor_dict = get_new_sensor(sensor.sensor_id, db, mongo_db)
        sensor_dict["values"] = [{"max_temperature" : sensor.max_temperature, "min_temperature": sensor.min_temperature, "average_temperature" : sensor.average_temperature}]
        list.append(sensor_dict)

    return {"sensors": list}
        
def get_quantity_by_type(cassandra: CassandraClient):

    cassandra.execute("USE sensor;")
    data = cassandra.execute("""SELECT type, COUNT(sensor_id) AS quantity FROM types GROUP BY type;""")
    list = []
    for sensor in data:
        type_dict = {"type" : sensor.type, "quantity": sensor.quantity}
        list.append(type_dict)

    return {"sensors" : list}

def get_low_battery(db: Session, redis_client: RedisClient, mongo_db: MongoDBClient, cassandra: CassandraClient):

    cassandra.execute("USE sensor;")
    data = cassandra.execute("""SELECT sensor_id, battery_level FROM battery WHERE battery_level < 0.2 ALLOW FILTERING;""")
            
    list = []

    for sensor in data:
        sensor_dict = get_new_sensor(sensor.sensor_id, db,  mongo_db)
        sensor_dict["battery_level"] = sensor.battery_level
        list.append(sensor_dict)    

    return {"sensors" : list}