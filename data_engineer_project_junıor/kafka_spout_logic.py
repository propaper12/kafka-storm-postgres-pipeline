import random
from datetime import datetime
import time
class KafkaBorsaSpoutLogic:
    def __init__(self):
        self.log("Kafka Spout  başlatılıyor.")
    def get_next_tuple(self):
        hisse = random.choice(["AAPL", "MSFT", "GOOG"])
        fiyat = round(random.uniform(150.0, 350.0), 2)
        hacim = random.randint(1000, 50000)
        zaman_damgasi_str = datetime.now().isoformat()
        return [hisse, zaman_damgasi_str, fiyat, hacim]
    def log(self, message):
        print(f"[SPOUT LOG] {datetime.now().strftime('%H:%M:%S')} - {message}")
    def sleep(self, seconds):
        time.sleep(seconds)