import fastapi
import yoyo
from yoyo import read_migrations
from yoyo import get_backend
from .sensors.controller import router as sensorsRouter
from shared.cassandra_client import CassandraClient

app = fastapi.FastAPI(title="Senser", version="0.1.0-alpha.1")

#TODO: Apply new TS migrations using Yoyo
#Read docs: https://ollycope.com/software/yoyo/latest/


# Ruta al directorio de migraciones
migration_dir = "/migrations_ts"
# Configuración de la base de datos para Yoyo
# database_url = "postgresql://postgres:postgres@postgreSQL/postgres"
database_url = "postgresql://timescale:timescale@timescale:5433/timescale"


# Crear una instancia de backend de Yoyo
backend = get_backend(database_url)
# Leer las migraciones desde el directorio
migrations = read_migrations(migration_dir)

# Aplicar las migraciones
with backend.lock():
    # Apply any outstanding migrations
    backend.apply_migrations(backend.to_apply(migrations))
    # Rollback all migrations
    # backend.rollback_migrations(backend.to_rollback(migrations))


app.include_router(sensorsRouter)
# cassandra_client = CassandraClient(["cassandra"])
# # Crear un keyspace en Cassandra
# cassandra_client.session.execute("""CREATE KEYSPACE IF NOT EXISTS sensor WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};""")
# cassandra_client.session.execute("USE sensor")

# # Crear las tablas en Cassandra
# cassandra_client.session.get_session().execute("""CREATE TABLE IF NOT EXISTS temperature (sensor_id int, temperature DOUBLE, PRIMARY KEY (sensor_id, timestamp)) WITH CLUSTERING ORDER BY (timestamp DESC);""")

# cassandra_client.session.execute("""CREATE TABLE IF NOT EXISTS type (type TEXT, sensor_id int, PRIMARY KEY (type, sensor_id));""")

# cassandra_client.session.execute("""CREATE TABLE IF NOT EXISTS battery (sensor_id int, battery_level DOUBLE, PRIMARY KEY (sensor_id, battery_level)) WITH CLUSTERING ORDER BY (battery_level DESC);""")

@app.get("/")
def index():
    #Return the api name and version
    return {"name": app.title, "version": app.version}
