import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
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
            f"Hata: DB bağlantısı kurulamadı {e}")
        return pd.DataFrame()
    finally:
        if veritabani_baglantisi:
            veritabani_baglantisi.close()

def on_isleme_yap(data_frame, scaler_secimi):
    if data_frame.empty:
        return data_frame

    data_frame_islenmis = data_frame.copy()

    data_frame_islenmis['hacim_rank'] = data_frame_islenmis.groupby('hisse_kodu')['hacim'].rank(method='dense',
                                                                                                ascending=False)
    data_frame_islenmis['volatilite'] = data_frame_islenmis.groupby('hisse_kodu')['fiyat'].transform(
        lambda x: x.pct_change().rolling(window=2).std()
    )
    data_frame_islenmis = data_frame_islenmis.fillna(0)

    if scaler_secimi == "StandardScaler (Z-Skor)":
        scaler = StandardScaler()
        data_frame_islenmis[['fiyat_scaled', 'volatilite_scaled']] = scaler.fit_transform(
            data_frame_islenmis[['fiyat', 'volatilite']]
        )
        st.success("Veri Z-Skoru ile normalize edildi (StandardScaler).")

    return data_frame_islenmis

def ml_uygula(data_frame, model_secimi):
    if data_frame.empty or len(data_frame) < 5:
        return data_frame, "Yeterli veri yo."

    if model_secimi == "Lineer Regresyon":

        if 'volatilite_scaled' in data_frame.columns:
            X = data_frame[['volatilite_scaled']].values
        else:
            X = data_frame[['volatilite']].values

        y = data_frame['fiyat'].values

        try:
            model = LinearRegression()
            model.fit(X, y)

            data_frame['tahmin_fiyat'] = model.predict(X)

            r2 = r2_score(y, data_frame['tahmin_fiyat'])
            coef = model.coef_[0]
            intercept = model.intercept_

            st.success("Lineer Regresyon başarıyla uygulandı.")

            results = {
                'R2_Skoru': f"{r2:.4f}",
                'Katsayı (Volatilite)': f"{coef:.4f}",
                'Kesişim (Intercept)': f"{intercept:.4f}"
            }

            return data_frame, results

        except Exception as e:
            return data_frame, f"Lineer Regresyon Hatası: {e}"

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

    st.sidebar.subheader("2. Veri Ön İşleme (DS Hazırlığı)")
    preprocessing_option = st.sidebar.radio(
        'Veriye hangi dönüşüm uygulansın?',
        ('Yok', 'StandardScaler (Z-Skor)')
    )

    df_processed = on_isleme_yap(df_filtered, preprocessing_option)

    st.sidebar.subheader("3. Makine Öğrenmesi")
    ml_model = st.sidebar.radio(
        'Uygulanacak Algoritmayı Seçin:',
        ('Yok', 'Lineer Regresyon')
    )
    df_analyzed, ml_status_output = ml_uygula(df_processed, ml_model)
    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Lineer Regresyon Analizi: Gerçek vs. Tahmin Edilen Fiyat")
        if ml_model == "Lineer Regresyon (Volatilite ile Fiyat Tahmini)" and isinstance(ml_status_output, dict):

            fig = px.scatter(
                df_analyzed.tail(50),
                x='volatilite',
                y='fiyat',
                color='hisse_kodu',
                hover_data=['zaman_damgasi'],
                title='Volatiliteye Göre Fiyat Dağılımı (Lineer Regresyon pipeline)'
            )
            fig_tahmin = px.line(df_analyzed.tail(50).sort_values(by='volatilite'),
                                 x='volatilite',
                                 y='tahmin_fiyat',
                                 line_dash_sequence=['dot'],
                                 color_discrete_sequence=['red'])

            fig.add_trace(fig_tahmin.data[0])

            st.plotly_chart(fig, use_container_width=True)

        else:
            y_col = 'fiyat_scaled' if 'fiyat_scaled' in df_analyzed.columns else 'fiyat'
            fig_line = px.line(df_analyzed,
                               x='zaman_damgasi',
                               y=y_col,
                               color='hisse_kodu',
                               title=f'Hisse Fiyatları Zaman Serisi Grafiği ({y_col})')
            st.plotly_chart(fig_line, use_container_width=True)

    with col2:
        st.subheader("Analiz Sonuçları ve Veri Özeti")
        if ml_model == "Lineer Regresyon (Volatilite ile Fiyat Tahmini)" and isinstance(ml_status_output, dict):
            st.markdown(f"**Regresyon Durumu:** :green[Başarılı]")
            st.write("**Model Performans Metrikleri:**")
            df_results = pd.DataFrame(list(ml_status_output.items()), columns=['Metrik', 'Değer'])
            st.dataframe(df_results, hide_index=True, use_container_width=True)

        else:
            st.markdown(f"**Model Durumu:** :orange[{ml_status_output}]")

        st.markdown("---")
        st.subheader("Hacim Sıralaması")
        st.dataframe(
            df_analyzed.sort_values(by=['hisse_kodu', 'hacim_rank']).head(10)[
                ['hisse_kodu', 'zaman_damgasi', 'hacim', 'hacim_rank', 'volatilite']],
            use_container_width=True,
            hide_index=True
        )
else:
    st.warning(
        "Veri çekilemedi. Storm Simülatörünün çalışıp PostgreSQL'e veri yazdığından emin olun.")