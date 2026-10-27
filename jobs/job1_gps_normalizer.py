"""
TaaSim - Semaine 3
Flink Job 1 : GPS Normalizer

Pipeline :
  Kafka (raw.gps)
    -> Validation coordonnees + vitesse
    -> Drop late events (watermark-based)
    -> Zone mapping (16 arrondissements Casablanca)
    -> Anonymisation (lat/lon -> centre de zone)
    -> Sink Cassandra (vehicle_positions)
    -> Sink Kafka (processed.gps) pour Job 2

Exigences Section 6 :
  - Freshness GPS -> Cassandra < 15 secondes
  - Checkpointing toutes les 60 secondes vers MinIO
  - Coordonnees brutes jamais persistees (anonymisation)
"""

import json
import logging
from datetime import datetime

from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
)
from pyflink.datastream.functions import MapFunction, FilterFunction, ProcessFunction

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GpsNormalizerJob")

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
KAFKA_BROKER     = "kafka:9092"
TOPIC_INPUT      = "raw.gps"
TOPIC_OUTPUT     = "processed.gps"
CASSANDRA_HOST   = "cassandra"
CASSANDRA_PORT   = 9042
KEYSPACE         = "taasim"

# Bounding box Casablanca
CASA_LAT_MIN, CASA_LAT_MAX = 33.40, 33.70
CASA_LON_MIN, CASA_LON_MAX = -7.80, -7.45
MAX_SPEED_KMH = 150.0

# 16 arrondissements de Casablanca
# Format : (zone_id, lat_min, lat_max, lon_min, lon_max, centroid_lat, centroid_lon, nom)
ZONES = [
    (1,  33.57, 33.65, -7.65, -7.55,  33.610, -7.600, "Anfa"),
    (2,  33.54, 33.60, -7.58, -7.48,  33.570, -7.530, "Ain Chock"),
    (3,  33.60, 33.67, -7.55, -7.45,  33.635, -7.500, "Ain Sebaa"),
    (4,  33.53, 33.58, -7.58, -7.52,  33.555, -7.550, "Ben M'Sik"),
    (5,  33.60, 33.66, -7.62, -7.55,  33.630, -7.575, "Bernoussi"),
    (6,  33.54, 33.59, -7.68, -7.60,  33.565, -7.640, "Hay Hassani"),
    (7,  33.56, 33.62, -7.65, -7.58,  33.590, -7.615, "Maarif"),
    (8,  33.58, 33.63, -7.62, -7.55,  33.605, -7.585, "Sidi Belyout"),
    (9,  33.60, 33.65, -7.70, -7.62,  33.625, -7.660, "Hay Mohammadi"),
    (10, 33.55, 33.60, -7.72, -7.65,  33.575, -7.685, "Sidi Moumen"),
    (11, 33.50, 33.55, -7.65, -7.55,  33.525, -7.600, "Ain Diab"),
    (12, 33.62, 33.68, -7.68, -7.60,  33.650, -7.640, "Bouskoura"),
    (13, 33.48, 33.54, -7.58, -7.48,  33.510, -7.530, "Oulfa"),
    (14, 33.56, 33.61, -7.72, -7.65,  33.585, -7.685, "Sbata"),
    (15, 33.63, 33.70, -7.58, -7.48,  33.665, -7.530, "Sidi Othmane"),
    (16, 33.40, 33.50, -7.80, -7.68,  33.450, -7.740, "Dar Bouazza"),
]

# ─────────────────────────────────────────────
# FONCTIONS UTILITAIRES
# ─────────────────────────────────────────────

def find_zone(lat, lon):
    """
    Retourne (zone_id, centroid_lat, centroid_lon) pour les coordonnees donnees.
    Si aucune zone exacte n'est trouvee, retourne la zone la plus proche par distance.
    Retourne None si les coordonnees sont hors bounding box Casablanca.
    """
    if not (CASA_LAT_MIN <= lat <= CASA_LAT_MAX and CASA_LON_MIN <= lon <= CASA_LON_MAX):
        return None

    for zone_id, lat_min, lat_max, lon_min, lon_max, c_lat, c_lon, name in ZONES:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return zone_id, c_lat, c_lon

    min_dist = float('inf')
    best_zone = ZONES[0]
    for zone in ZONES:
        zone_id, _, _, _, _, c_lat, c_lon, _ = zone
        dist = (lat - c_lat) ** 2 + (lon - c_lon) ** 2
        if dist < min_dist:
            min_dist = dist
            best_zone = zone
    return best_zone[0], best_zone[4], best_zone[5]


# ─────────────────────────────────────────────
# FLINK FUNCTIONS
# ─────────────────────────────────────────────

class GpsEventTimestampAssigner(TimestampAssigner):
    """
    Extrait l'event-time depuis le champ 'timestamp' du message GPS.
    Le timestamp est en millisecondes (tel que produit par vehicle_gps_producer.py).
    """
    def extract_timestamp(self, value, record_timestamp):
        try:
            event = json.loads(value)
            return int(event.get("timestamp", record_timestamp))
        except Exception:
            return record_timestamp


class DropLateEvents(ProcessFunction):
    """
    Supprime les evenements dont le timestamp est anterieur au watermark.
    Watermark = max_event_time - 3 minutes.
    Un evenement avec event_time < watermark est considere en retard et rejete.
    """
    def process_element(self, value, ctx):
        try:
            event = json.loads(value)
            event_ts = int(event.get("timestamp", 0))
            watermark = ctx.timer_service().current_watermark()

            if watermark > 0 and event_ts < watermark:
                logger.warning(f"Late event DROPPED: taxi={event.get('taxi_id')} ts={event_ts} wm={watermark}")
                return
        except Exception:
            pass

        yield value


class ValidateAndFilterGps(FilterFunction):
    """
    Filtre les evenements GPS invalides :
      - JSON malforme
      - Coordonnees hors bounding box Casablanca
      - Vitesse > 150 km/h (signal errone)
      - Champs obligatoires manquants (taxi_id, lat, lon)
    """
    def filter(self, value):
        try:
            event = json.loads(value)

            if not all(k in event for k in ("taxi_id", "timestamp", "lat", "lon")):
                logger.warning(f"Evenement GPS incomplet rejete : {value[:80]}")
                return False

            lat = float(event["lat"])
            lon = float(event["lon"])
            speed = float(event.get("speed", 0.0))

            if not (CASA_LAT_MIN <= lat <= CASA_LAT_MAX):
                logger.debug(f"Lat hors bbox Casablanca: {lat}")
                return False
            if not (CASA_LON_MIN <= lon <= CASA_LON_MAX):
                logger.debug(f"Lon hors bbox Casablanca: {lon}")
                return False

            if speed > MAX_SPEED_KMH:
                logger.warning(f"Vitesse aberrante rejetee: {speed} km/h pour taxi {event['taxi_id']}")
                return False

            return True

        except (json.JSONDecodeError, ValueError, TypeError) as e:
            logger.warning(f"Evenement GPS malforme ignore : {e}")
            return False


class NormalizeAndAnonymizeGps(MapFunction):
    """
    Transformation principale :
      1. Parse le JSON
      2. Assigne la zone Casablanca
      3. ANONYMISE : remplace lat/lon bruts par le centre de zone
      4. Retourne un JSON normalise pour Cassandra et processed.gps
    """
    def map(self, value):
        event = json.loads(value)

        lat   = float(event["lat"])
        lon   = float(event["lon"])
        speed = float(event.get("speed", 0.0))

        zone_result = find_zone(lat, lon)
        if zone_result is None:
            zone_id, anon_lat, anon_lon = 1, ZONES[0][4], ZONES[0][5]
        else:
            zone_id, anon_lat, anon_lon = zone_result

        normalized = {
            "taxi_id":    str(event["taxi_id"]),
            "timestamp":  int(event["timestamp"]),
            "zone_id":    zone_id,
            "lat":        anon_lat,
            "lon":        anon_lon,
            "speed":      round(speed, 2),
            "status":     event.get("status", "available"),
            "city":       "Casablanca"
        }

        return json.dumps(normalized)


# ─────────────────────────────────────────────
# SINK CASSANDRA
# ─────────────────────────────────────────────

class CassandraSinkFunction(MapFunction):
    """
    Ecrit chaque position normalisee dans Cassandra.
    Utilise cassandra-driver Python (installe dans l'image PyFlink).

    Table cible : vehicle_positions
      PRIMARY KEY ((city, zone_id), event_time)
    """
    def __init__(self):
        self.session = None

    def open(self, runtime_context):
        from cassandra.cluster import Cluster
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        self.session = cluster.connect(KEYSPACE)
        logger.info("Connexion Cassandra etablie")

        self.insert_stmt = self.session.prepare("""
            INSERT INTO vehicle_positions
              (city, zone_id, event_time, taxi_id, lat, lon, speed, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            USING TTL 3600
        """)

    def map(self, value):
        try:
            pos = json.loads(value)
            self.session.execute(self.insert_stmt, (
                pos["city"],
                int(pos["zone_id"]),
                int(pos["timestamp"]),
                pos["taxi_id"],
                float(pos["lat"]),
                float(pos["lon"]),
                float(pos["speed"]),
                pos["status"]
            ))
        except Exception as e:
            logger.error(f"Erreur ecriture Cassandra : {e}")
        return value


# ─────────────────────────────────────────────
# MAIN : Construction du pipeline Flink
# ─────────────────────────────────────────────

def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    env.enable_checkpointing(60_000)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(30_000)
    env.get_checkpoint_config().set_checkpoint_timeout(120_000)

    env.set_parallelism(2)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(TOPIC_INPUT)
        .set_group_id("flink-job1-gps-normalizer")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_minutes(3))
        .with_timestamp_assigner(GpsEventTimestampAssigner())
    )

    raw_stream = env.from_source(
        kafka_source,
        watermark_strategy,
        "Kafka Source - raw.gps"
    )

    processed_stream = (
        raw_stream
        # ETAPE 0 : Supprimer les evenements en retard (> watermark)
        .process(DropLateEvents(), output_type=Types.STRING())
        .name("Drop Late Events")
        # ETAPE 1 : Validation + filtrage des coordonnees invalides
        .filter(ValidateAndFilterGps())
        .name("Validation GPS")
        # ETAPE 2 : Normalisation + anonymisation + zone mapping
        .map(NormalizeAndAnonymizeGps(), output_type=Types.STRING())
        .name("Normalisation & Anonymisation")
        # ETAPE 3 : Ecriture Cassandra
        .map(CassandraSinkFunction(), output_type=Types.STRING())
        .name("Sink Cassandra - vehicle_positions")
    )

    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(TOPIC_OUTPUT)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    processed_stream.sink_to(kafka_sink).name("Sink Kafka - processed.gps")

    logger.info("Demarrage TaaSim Job 1 - GPS Normalizer")
    logger.info(f"   Source  : Kafka topic '{TOPIC_INPUT}'")
    logger.info(f"   Sink 1  : Cassandra table 'vehicle_positions'")
    logger.info(f"   Sink 2  : Kafka topic '{TOPIC_OUTPUT}'")
    logger.info(f"   Watermark : 3 minutes allowed lateness")
    logger.info(f"   Checkpoint : toutes les 60s -> MinIO")

    env.execute("TaaSim - Job1 GPS Normalizer")


if __name__ == "__main__":
    main()