"""
TaaSim — Semaine 3
Script de test : Late Events & Watermarks

Objectif (livrable W3) :
  "Watermark test: late event handled correctly (documented)"

Ce script envoie 3 séquences de messages vers raw.gps :
  - Séquence A : messages normaux (ordre chronologique)
  - Séquence B : messages en retard de 2 min (< watermark 3min → acceptés)
  - Séquence C : messages en retard de 5 min (> watermark 3min → rejetés/ignorés)

Comment utiliser :
  1. S'assurer que Job 1 tourne
  2. Ouvrir un terminal Cassandra : docker exec -it cassandra cqlsh
  3. Lancer ce script depuis Jupyter
  4. Vérifier dans Cassandra que Séquence C n'apparaît PAS
"""

import json
import time
from confluent_kafka import Producer

KAFKA_BROKER = "kafka:9092"
TOPIC        = "raw.gps"

producer = Producer({"bootstrap.servers": KAFKA_BROKER})

def send_event(taxi_id, lat, lon, timestamp_ms, status="available", label=""):
    message = {
        "taxi_id":   str(taxi_id),
        "timestamp": timestamp_ms,
        "lat":       lat,
        "lon":       lon,
        "speed":     30.0,
        "status":    status
    }
    producer.produce(TOPIC, key=str(taxi_id), value=json.dumps(message))
    producer.poll(0)
    ts_str = time.strftime('%H:%M:%S', time.gmtime(timestamp_ms / 1000))
    print(f"  [{label}] taxi={taxi_id} | event_time={ts_str} | lat={lat:.4f}, lon={lon:.4f}")


def run_watermark_test():
    now_ms = int(time.time() * 1000)

    print("=" * 60)
    print("TEST WATERMARKS — TaaSim Job 1")
    print("=" * 60)

    # ─────────────────────────────────────────────
    # SÉQUENCE A : Messages normaux
    # Attendu : tous acceptés et écrits dans Cassandra
    # ─────────────────────────────────────────────
    print("\n📌 Séquence A — Messages normaux (doivent apparaître dans Cassandra)")
    for i in range(5):
        ts = now_ms + (i * 4000)  # toutes les 4 secondes
        send_event(
            taxi_id=f"TAXI_A_{i}",
            lat=33.58 + (i * 0.01),
            lon=-7.60 + (i * 0.01),
            timestamp_ms=ts,
            label="NORMAL"
        )
        time.sleep(0.5)

    print("  → Attente 3 secondes...")
    time.sleep(3)

    # ─────────────────────────────────────────────
    # SÉQUENCE B : Messages en retard de 2 minutes
    # Attendu : acceptés (2min < watermark 3min)
    # ─────────────────────────────────────────────
    print("\n⏰ Séquence B — Retard 2 min (doivent être acceptés, < watermark 3min)")
    two_min_ago = now_ms - (2 * 60 * 1000)
    for i in range(3):
        ts = two_min_ago + (i * 4000)
        send_event(
            taxi_id=f"TAXI_B_{i}",
            lat=33.56 + (i * 0.01),
            lon=-7.62 + (i * 0.01),
            timestamp_ms=ts,
            label="LATE-2MIN"
        )
        time.sleep(0.5)

    print("  → Attente 3 secondes...")
    time.sleep(3)

    # ─────────────────────────────────────────────
    # SÉQUENCE C : Messages en retard de 5 minutes
    # Attendu : ignorés par Flink (5min > watermark 3min)
    # ─────────────────────────────────────────────
    print("\n🚫 Séquence C — Retard 5 min (doivent être IGNORÉS, > watermark 3min)")
    five_min_ago = now_ms - (5 * 60 * 1000)
    for i in range(3):
        ts = five_min_ago + (i * 4000)
        send_event(
            taxi_id=f"TAXI_C_{i}",
            lat=33.54 + (i * 0.01),
            lon=-7.64 + (i * 0.01),
            timestamp_ms=ts,
            label="LATE-5MIN ← DOIT ÊTRE REJETÉ"
        )
        time.sleep(0.5)

    producer.flush()
    print("\n✅ Tous les messages envoyés.")

    # ─────────────────────────────────────────────
    # INSTRUCTIONS DE VÉRIFICATION
    # ─────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("VÉRIFICATION CASSANDRA (attendre ~15 secondes)")
    print("=" * 60)
    print("""
1. Ouvre un terminal et lance :
   docker exec -it cassandra cqlsh

2. Dans cqlsh, exécute :

   USE taasim;

   -- Doit montrer les taxis TAXI_A_* et TAXI_B_*
   SELECT taxi_id, zone_id, event_time
   FROM vehicle_positions
   WHERE city = 'Casablanca' AND zone_id = 7
   LIMIT 20;

   -- Vérifie que TAXI_C_* n'apparaît PAS
   -- Si TAXI_C_* apparaît = problème de watermark !

3. Résultat attendu :
   ✅ TAXI_A_* présents  → messages normaux acceptés
   ✅ TAXI_B_* présents  → late events < 3min acceptés
   ❌ TAXI_C_* absents   → late events > 3min correctement rejetés
""")

    # ─────────────────────────────────────────────
    # Vérification automatique via cassandra-driver
    # ─────────────────────────────────────────────
    print("Vérification automatique dans 20 secondes...")
    time.sleep(20)

    try:
        from cassandra.cluster import Cluster
        cluster = Cluster(["cassandra"], port=9042)
        session = cluster.connect("taasim")

        # Cherche les TAXI_C dans toutes les zones
        rows = session.execute(
            "SELECT taxi_id, zone_id FROM vehicle_positions WHERE city = 'Casablanca' ALLOW FILTERING"
        )
        all_taxis = {row.taxi_id for row in rows}

        taxi_a_found = any(t.startswith("TAXI_A_") for t in all_taxis)
        taxi_b_found = any(t.startswith("TAXI_B_") for t in all_taxis)
        taxi_c_found = any(t.startswith("TAXI_C_") for t in all_taxis)

        print("\n📊 RÉSULTATS DU TEST :")
        print(f"  TAXI_A (normaux)    : {'✅ PRÉSENTS' if taxi_a_found else '❌ ABSENTS (problème!)'}")
        print(f"  TAXI_B (retard 2min): {'✅ PRÉSENTS' if taxi_b_found else '❌ ABSENTS (problème!)'}")
        print(f"  TAXI_C (retard 5min): {'✅ ABSENTS (correct!)' if not taxi_c_found else '❌ PRÉSENTS — watermark ne fonctionne pas!'}")

        if taxi_a_found and taxi_b_found and not taxi_c_found:
            print("\n🎉 TEST WATERMARK RÉUSSI — Livrable W3 validé !")
        else:
            print("\n⚠️  Vérifier la configuration du watermark dans job1_gps_normalizer.py")

        cluster.shutdown()

    except Exception as e:
        print(f"Erreur vérification Cassandra : {e}")
        print("Vérification manuelle requise (voir instructions ci-dessus)")


if __name__ == "__main__":
    run_watermark_test()
