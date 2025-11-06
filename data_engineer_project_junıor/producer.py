import yfinance as yf
import json
from kafka import KafkaProducer
import time
import datetime

try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except Exception as e:
    print(f"HATA: Kafka'ya bağlanılamadı. {e}")
    exit()

KAFKA_TOPIC = "borsa_verileri"
HISSE = "AAPL"

while True:
    try:
        data = yf.Ticker(HISSE).history(period='1d', interval='1m')

        if not data.empty:
            son_veri = data.iloc[-1]

            veri_paketi = {
                "hisse_kodu": HISSE,
                "zaman_damgasi": son_veri.name.isoformat(),
                "fiyat": float(son_veri['Close']),
                "hacim": int(son_veri['Volume'])
            }

            producer.send(KAFKA_TOPIC, veri_paketi)
            producer.flush()

        time.sleep(5)
    except KeyboardInterrupt:
        break
    except Exception as e:
        print(f"Hata: {e}");
        time.sleep(10)

producer.close()