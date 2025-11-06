from kafka_spout_logic import KafkaBorsaSpoutLogic
from postgres_bolt_logic import PostgresWriterBoltLogic
import time
import os

print("--- APACHE STORM TOPOLOGY SİMÜLASYONU BAŞLADI ---")
print("Kafka'dan (Simüle), Storm Mantığı ile PostgreSQL'e yazım testi.")

spout = KafkaBorsaSpoutLogic()
bolt = PostgresWriterBoltLogic()

def run_storm_flow(iterations=15):
    if not bolt.is_connected:
        print("\nFATAL: Veritabanı bağlantısı kurulamadı. Lütfen Docker'ı ve şifreyi kontrol edin.")
        return

    for i in range(iterations):
        new_tuple = spout.get_next_tuple()
        spout.log(f"Spout Okundu: {new_tuple[0]}")

        bolt.process_tuple(new_tuple)

        time.sleep(0.5)

    print(f"--- {iterations} İŞLEM BAŞARILI. STORM TOPOLOGY SİMÜLASYONU TAMAMLANDI. ---")

if __name__ == '__main__':
    run_storm_flow()