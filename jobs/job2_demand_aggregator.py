# job2_demand_aggregator.py - V7
import json
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction, KeySelector, KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy, Duration
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DemandAggregator")

WINDOW_MS = 30000
LATE_MS = 10000

class ParseGPS(MapFunction):
    def map(self, value):
        try:
            return json.loads(value)
        except:
            return None

class ZoneKeySelector(KeySelector):
    def get_key(self, value):
        if value is None:
            return "0"
        return str(value.get("zone_id", 0))

class DemandZoneAggregator(KeyedProcessFunction):
    def __init__(self):
        self.session = None
        self.insert_stmt = None
        self.acc = None

    def open(self, ctx):
        from cassandra.cluster import Cluster
        cluster = Cluster(["cassandra"])
        self.session = cluster.connect("taasim")
        self.insert_stmt = self.session.prepare(
            "INSERT INTO demand_zones (zone_id, window_start, window_end, vehicle_count, event_count, avg_speed) VALUES (?, ?, ?, ?, ?, ?)"
        )
        self.acc = ctx.get_state(ValueStateDescriptor("wacc", str))
        logger.info("DemandZoneAggregator opened")

    def process_element(self, value, ctx):
        try:
            ts = int(value.get("timestamp", 0))
            vid = str(value.get("taxi_id", ""))
            speed = float(value.get("speed", 0))
            ws = (ts // WINDOW_MS) * WINDOW_MS
            we = ws + WINDOW_MS
            raw = self.acc.value()
            aw = json.loads(raw) if raw else {}
            wk = str(ws)
            if wk not in aw:
                aw[wk] = {"vids": [], "count": 0, "speed_sum": 0.0}
            w = aw[wk]
            if vid not in w["vids"]:
                w["vids"].append(vid)
            w["count"] += 1
            w["speed_sum"] += speed
            self.acc.update(json.dumps(aw))
            ctx.timer_service().register_event_time_timer(we + LATE_MS)
        except Exception as e:
            logger.error("process_element: " + str(e))

    def on_timer(self, timestamp, ctx):
        try:
            zone_id = ctx.get_current_key()
            ws = ((timestamp - LATE_MS) // WINDOW_MS) * WINDOW_MS
            we = ws + WINDOW_MS
            wk = str(ws)
            raw = self.acc.value()
            if not raw:
                return
            aw = json.loads(raw)
            if wk not in aw:
                return
            w = aw[wk]
            if w["count"] > 0:
                vc = len(w["vids"])
                ec = w["count"]
                avg = round(w["speed_sum"] / ec, 2)
                self.session.execute(self.insert_stmt, (int(zone_id), ws, we, vc, ec, avg))
                logger.info("ZONE " + str(zone_id) + " veh=" + str(vc) + " evt=" + str(ec))
            del aw[wk]
            self.acc.update(json.dumps(aw))
        except Exception as e:
            logger.error("on_timer: " + str(e))

def main():
    logger.info("TaaSim Job 2 V7")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    src = (KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_topics("processed.gps")
        .set_group_id("demand-v7")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build())
    stream = env.from_source(src, WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10)), "KafkaSource")
    parsed = stream.map(ParseGPS()).filter(lambda x: x is not None)
    parsed.key_by(ZoneKeySelector()).process(DemandZoneAggregator())
    env.execute("TaaSim-DemandAggregator-V7")

if __name__ == "__main__":
    main()
