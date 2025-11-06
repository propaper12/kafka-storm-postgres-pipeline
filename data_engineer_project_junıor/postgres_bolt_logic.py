import psycopg2
from datetime import datetime
import time

PG_PASSWORD = '12345'
DB_NAME = "borsa_projesi_db"
DB_USER = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"

class PostgresWriterBoltLogic:
    def __init__(self):
        self.log("PostgreSQL Bolt Mantığı başlatılıyor...")
        self.conn = None
        self.cursor = None
        self.is_connected = False

        try:
            self.conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password=PG_PASSWORD,
                host=DB_HOST, port=DB_PORT
            )
            self.cursor = self.conn.cursor()
            self.log("PostgreSQL bağlantısı başarılı.")
            self.is_connected = True
        except Exception as e:
            self.log(f"HATA: PostgreSQL bağlantısı kurulamadı. Hata: {e}", level='error')
    def process_tuple(self, tup_values):

        if not self.is_connected:
            time.sleep(1)
            return

        hisse_kodu, zaman_damgasi_str, fiyat, hacim = tup_values
        try:
            zaman_damgasi = datetime.fromisoformat(zaman_damgasi_str.split('+')[0])
            insert_query = """
                INSERT INTO anlik_veriler (hisse_kodu, zaman_damgasi, fiyat, hacim)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (hisse_kodu, zaman_damgasi) DO NOTHING;
            """
            self.cursor.execute(insert_query, (hisse_kodu, zaman_damgasi, fiyat, hacim))
            self.conn.commit()
            self.log(f"Storm/Postgres Yazma Başarılı: {hisse_kodu} @ {fiyat}")
        except Exception as e:
            self.log(f"SQL Yazma Hatası: {e}", level='error')
            if self.conn:
                self.conn.rollback()
    def log(self, message, level='info'):
        print(f"[BOLT LOG] {datetime.now().strftime('%H:%M:%S')} - {level.upper()} - {message}")