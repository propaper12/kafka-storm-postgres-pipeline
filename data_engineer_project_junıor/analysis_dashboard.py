import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import KMeans
import numpy as np

PG_PASSWORD = '12345'
DB_HOST = "localhost"
DB_NAME = "borsa_projesi_db"
DB_USER = "postgres"
DB_PORT = "5432"


@st.cache_data
def veriyi_cek():
    veritabani_baglantisi = None
    try:
        veritabani_baglantisi = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=PG_PASSWORD,
            host=DB_HOST, port=DB_PORT
        )
        data_frame = pd.read_sql(
            'SELECT hisse_kodu, zaman_damgasi, fiyat, hacim FROM anlik_veriler ORDER BY zaman_damgasi',
            veritabani_baglantisi)
        data_frame['zaman_damgasi'] = pd.to_datetime(data_frame['zaman_damgasi'])
        return data_frame
    except Exception as e:
        st.error(
            f"Hata: DB bağlantısı kurulamadı. Storm'un çalıştığından emin olun. Hata: {e}")
        return pd.DataFrame()
    finally:
        if veritabani_baglantisi:
            veritabani_baglantisi.close()

def on_isleme_yap(data_frame, scaler_secimi):
    if data_frame.empty:
        return data_frame
    data_frame_islenmis = data_frame.copy()
    if scaler_secimi == "MinMaxScaler (0-1)":
        scaler = MinMaxScaler()
        data_frame_islenmis['fiyat_normalize'] = scaler.fit_transform(data_frame_islenmis[['fiyat']])
        st.success("Veri başarıyla normalize edildi.")
    data_frame_islenmis['hacim_rank'] = data_frame_islenmis.groupby('hisse_kodu')['hacim'].rank(method='dense',
                                                                                                ascending=False)
    return data_frame_islenmis


def ml_uygula(data_frame, model_secimi):
    if data_frame.empty or len(data_frame) < 5:
        return data_frame, "Yeterli veri yok (En az 5 satır lazım)."
    ozellikler = data_frame[['fiyat']].values

    if model_secimi == "K-Means Kümeleme (K=3)":
        try:
            kmeans = KMeans(n_clusters=3, random_state=0, n_init=10)
            data_frame['cluster'] = kmeans.fit_predict(ozellikler)
            st.success("K-Means başarıyla uygulandı.")
            return data_frame, "Kümeleme başarıyla yapıldı."
        except Exception as e:
            return data_frame, f"K-Means Hatası: {e}"

    return data_frame, "Model uygulanmadı."


st.set_page_config(layout="wide")
st.title('Storm Projesi: Borsa Akış Analiz Ekranı')

df_all = veriyi_cek()

if not df_all.empty:
    st.sidebar.header("Kullanıcı Seçimleri (Analiz Ayarları)")

    unique_tickers = df_all['hisse_kodu'].unique()
    selected_tickers = st.sidebar.multiselect(
        '1. Hisseleri Seçin:',
        options=unique_tickers,
        default=unique_tickers
    )

    df_filtered = df_all[df_all['hisse_kodu'].isin(selected_tickers)]

    st.sidebar.subheader("2. Veri Ön İşleme")
    preprocessing_option = st.sidebar.radio(
        'Veriye hangi dönüşüm uygulansın?',
        ('Yok', 'MinMaxScaler (0-1)')
    )

    df_processed = on_isleme_yap(df_filtered, preprocessing_option)

    st.sidebar.subheader("3. Makine Öğrenmesi")
    ml_model = st.sidebar.radio(
        'Uygulanacak Algoritmayı Seçin:',
        ('Yok', 'K-Means Kümeleme (K=3)')
    )
    df_analyzed, ml_status = ml_uygula(df_processed, ml_model)
    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Fiyat ve Küme Analiz Grafiği")

        color_col = 'cluster' if 'cluster' in df_analyzed.columns else 'hisse_kodu'
        y_col = 'fiyat_normalize' if 'fiyat_normalize' in df_analyzed.columns else 'fiyat'

        fig = px.line(df_analyzed,
                      x='zaman_damgasi',
                      y=y_col,
                      color=color_col,
                      title=f'Hisse Fiyatları Grafiği (Renk: {color_col})')
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Analiz Sonuçları ve Veri Özeti")

        st.markdown(f"**ML Durumu:** :green[{ml_status}]")
        if 'cluster' in df_analyzed.columns:
            st.write("**Küme Merkezi Özet Tablosu:**")
            st.dataframe(
                df_analyzed.groupby('cluster')['fiyat'].agg(['mean', 'min', 'max', 'count']).reset_index().rename(
                    columns={'count': 'üye_sayısı'}))
        st.subheader("Hacim Sıralaması (Rank Sistemi)")
        st.dataframe(
            df_analyzed.sort_values(by=['hisse_kodu', 'hacim_rank']).head(10)[
                ['hisse_kodu', 'zaman_damgasi', 'hacim', 'hacim_rank']],
            use_container_width=True,
            hide_index=True
        )

        st.caption(f"Veritabanından çekilen toplam satır sayısı: {len(df_all)}")
else:
    st.warning(
        "Veri çekilemedi. Storm Simülatörünün çalışıp PostgreSQL'e veri yazdığından emin olun.")