# 🎵 SA Artists Multi-Source Data Lake

A data engineering portfolio project combining **3 data sources** to analyze top South African Amapiano & Afrobeats artists.

## 🏗️ Architecture

```
YouTube API ──────┐
Last.fm API ───────┼──► Data Lake (CSV/S3) ──► Combined Dataset ──► Visualizations
MusicBrainz API ──┘
```

## 🎤 Artists Tracked
- Kabza De Small
- DJ Maphorisa
- Burna Boy
- Davido
- Masters KG

## 📊 Data Sources

| Source | Data | API Key Required |
|--------|------|-----------------|
| YouTube Data API v3 | Videos, views, likes, comments | ✅ Yes |
| Last.fm API | Play counts, listeners, top tracks, tags | ✅ Yes |
| MusicBrainz API | Artist metadata, country, genres, career start | ❌ No |

## 🔍 Key Insights
- Which artist dominates YouTube vs Last.fm?
- Does YouTube views correlate with Last.fm listeners?
- Where are these artists from and what genres do they span?
- Who has the highest fan engagement rate?

## 🛠️ Tech Stack
- Python 3.11
- YouTube Data API v3
- Last.fm API
- MusicBrainz API
- Pandas
- Matplotlib / Seaborn
- Google Colab

## 🚀 Quick Start (Google Colab)
1. Open `sa_artists_multi_source.ipynb` in Google Colab
2. Add secrets to Colab (🔑 icon):
   - `YOUTUBE_API_KEY`
   - `LASTFM_API_KEY`
3. Run all cells

## 📁 Project Structure
```
sa_artists_data_lake/
├── sa_artists_multi_source.ipynb  # Main Colab notebook
├── scripts/
│   └── fetch_all.py               # Standalone Python script
└── README.md
```

## 🔮 Possible Improvements
- Add Reddit API for social mentions
- Schedule with Apache Airflow
- Store in AWS S3 data lake
- Add dbt transformations
- Deploy Streamlit dashboard
