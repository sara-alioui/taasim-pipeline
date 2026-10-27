from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, date_format

# 1. Initialisation de Spark
# IMPORTANT: On précise l'hôte Cassandra au niveau de la session
spark = SparkSession.builder \
    .appName("TaaSim-Cassandra-Backfill") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

# 2. Lecture des données transformées depuis MinIO
print("Lecture des données depuis MinIO...")
df_trips = spark.read.parquet("/home/jovyan/work/porto-trips-mapped")

# On garde un échantillon de 1000 trajets pour le test
df_trips = df_trips.limit(1000)

# 3. Préparation des données pour la table 'trips'
df_trips_cassandra = df_trips.withColumn("city", lit("Casablanca")) \
    .withColumn("date_bucket", date_format(current_timestamp(), "yyyy-MM-dd")) \
    .withColumn("created_at", current_timestamp()) \
    .withColumn("trip_id", col("TRIP_ID").cast("string")) \
    .withColumn("taxi_id", col("TAXI_ID").cast("string")) \
    .withColumn("rider_id", lit("user_backfill")) \
    .withColumn("origin_zone", lit(1)) \
    .withColumn("dest_zone", lit(5)) \
    .withColumn("status", lit("completed")) \
    .withColumn("fare", lit(15.50))

# Sélectionner uniquement les colonnes qui correspondent au schéma Cassandra
columns_trips = ["city", "date_bucket", "created_at", "trip_id", "rider_id", "taxi_id", "origin_zone", "dest_zone", "status", "fare"]
df_trips_cassandra = df_trips_cassandra.select(columns_trips)

print("Données prêtes pour Cassandra.")
print("Écriture dans la table 'trips'...")

# 4. Écriture dans Cassandra
df_trips_cassandra.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="trips", keyspace="taasim") \
    .save()

print("SUCCÈS ! 1000 trajets insérés dans Cassandra.")
spark.stop()