<img width="2777" height="1478" alt="image" src="https://github.com/user-attachments/assets/cef2d1f0-4d65-429b-9059-35a26458aae6" /># kafka-storm-postgres-pipeline
Bu proje, Data Engineer kariyerime başlarken sıfırdan tasarlayıp kurduğum, gerçek zamanlı (real-time) veri akış altyapısını uygulamaktadır. Amaç, finansal piyasa verilerini kaynaktan depoya güvenilir ve dağıtık bir şekilde taşımaktır.
Servisleri Başlatma: (docker-compose.yml dosyasının bulunduğu dizinde) docker-compose up -d
Veritabanı Tablosu Oluşturma:docker-compose exec -it postgres-db psql -U postgres -d borsa_projesi_db
CREATE TABLE anlik_veriler (...); # Tabloyu oluşturun

pipeline hattını baslatma:# 
Veri Üretici (Kafka'ya yazar)
python producer.py
Storm Akış İşleyicisi (PostgreSQL'e yazar)
python storm_simulator.py

Analiz Arayüzünü Başlatma:
streamlit run analysis_dashboard.py
