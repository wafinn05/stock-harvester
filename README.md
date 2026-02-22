# Stock Harvester (Worker) ⚒️📡

Repositori ini berfungsi sebagai unit pengumpul data (Worker) harian untuk proyek **StockForecast AI**. 

## 🚀 Fungsi Utama
- **Mining Harian**: Mengambil data harga saham terbaru dari Yahoo Finance setiap hari jam 20:00 WIB.
- **Kecerdasan Makro**: Mengambil data indikator ekonomi makro (IHSG, Emas, USD, Minyak).
- **Analisis Sentimen**: Mengambil berita terbaru dan menghitung skor sentimen menggunakan model **IndoBERT**.
- **Indikator Teknikal**: Menghitung RSI, MACD, dan indikator teknikal lainnya secara otomatis.
- **Data Sync**: Menyimpan seluruh data hasil olahan ke database **Supabase**.

## ⚙️ Setup GitHub Secrets
Agar robot harian (`mining.yml`) bisa berjalan, pastikan kamu sudah mengatur **Secrets** di repo ini:
- `DB_USER`
- `DB_PASSWORD`
- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `HF_TOKEN` (Opsional, untuk download model IndoBERT)

## 📅 Jadwal Otomatis
Robot harian berjalan secara otomatis setiap hari **Senis - Jumat pukul 20:00 WIB** (13:00 UTC).

---
*Proyek ini adalah bagian dari ekosistem StockForecast AI untuk efisiensi kuota GitHub Actions.*
