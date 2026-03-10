"""
SA Artists Multi-Source Data Lake
Fetches data from:
  1. YouTube API     — video views, likes, comments
  2. Last.fm API     — play counts, listeners
  3. MusicBrainz API — artist metadata, genres, career info
"""

import os
import time
import requests
import pandas as pd
from googleapiclient.discovery import build

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
LASTFM_API_KEY  = os.getenv("LASTFM_API_KEY")

ARTISTS = [
    "Kabza De Small",
    "DJ Maphorisa",
    "Burna Boy",
    "Davido",
    "Masters KG",
]

YOUTUBE_CHANNEL_IDS = {
    "Kabza De Small": "UCxoVP9cGJHBZaLMexO0fCkw",
    "DJ Maphorisa":   "UCFXyVm03fMlzI4B4sQkDtBA",
    "Burna Boy":      "UCEzDdNqNkT-7rSfSGSr1hWg",
    "Davido":         "UCkBV3nBa0iRdxEGc4DUS3xA",
    "Masters KG":     "UC-Vdvbvw_57gspD6A2m4vkw",
}


# ─────────────────────────────────────────
# 1. YOUTUBE
# ─────────────────────────────────────────
def fetch_youtube_data(artist_name, channel_id, max_results=50):
    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    videos  = []

    try:
        response = youtube.channels().list(
            part="contentDetails", id=channel_id
        ).execute()

        uploads_playlist = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        playlist_response = youtube.playlistItems().list(
            part="contentDetails", playlistId=uploads_playlist, maxResults=max_results
        ).execute()

        video_ids = [item["contentDetails"]["videoId"] for item in playlist_response["items"]]
        stats_response = youtube.videos().list(
            part="snippet,statistics", id=",".join(video_ids)
        ).execute()

        for item in stats_response["items"]:
            stats = item.get("statistics", {})
            videos.append({
                "artist_name":   artist_name,
                "source":        "youtube",
                "video_id":      item["id"],
                "title":         item["snippet"]["title"],
                "published_at":  item["snippet"]["publishedAt"],
                "view_count":    int(stats.get("viewCount", 0)),
                "like_count":    int(stats.get("likeCount", 0)),
                "comment_count": int(stats.get("commentCount", 0)),
            })
    except Exception as e:
        print(f"  ❌ YouTube error for {artist_name}: {e}")

    return videos


# ─────────────────────────────────────────
# 2. LAST.FM
# ─────────────────────────────────────────
def fetch_lastfm_artist(artist_name):
    url = "http://ws.audioscrobbler.com/2.0/"
    try:
        # Artist info
        response = requests.get(url, params={
            "method":  "artist.getinfo",
            "artist":  artist_name,
            "api_key": LASTFM_API_KEY,
            "format":  "json"
        })
        data = response.json().get("artist", {})
        stats = data.get("stats", {})

        return {
            "artist_name":    artist_name,
            "source":         "lastfm",
            "listeners":      int(stats.get("listeners", 0)),
            "play_count":     int(stats.get("playcount", 0)),
            "lastfm_url":     data.get("url", ""),
            "tags":           ", ".join([t["name"] for t in data.get("tags", {}).get("tag", [])[:5]]),
            "bio_summary":    data.get("bio", {}).get("summary", "")[:300],
        }
    except Exception as e:
        print(f"  ❌ Last.fm error for {artist_name}: {e}")
        return None


def fetch_lastfm_top_tracks(artist_name, limit=10):
    url = "http://ws.audioscrobbler.com/2.0/"
    tracks = []
    try:
        response = requests.get(url, params={
            "method":  "artist.gettoptracks",
            "artist":  artist_name,
            "api_key": LASTFM_API_KEY,
            "format":  "json",
            "limit":   limit
        })
        for track in response.json().get("toptracks", {}).get("track", []):
            tracks.append({
                "artist_name": artist_name,
                "source":      "lastfm",
                "track_name":  track["name"],
                "play_count":  int(track.get("playcount", 0)),
                "listeners":   int(track.get("listeners", 0)),
                "rank":        int(track["@attr"]["rank"]),
            })
    except Exception as e:
        print(f"  ❌ Last.fm tracks error for {artist_name}: {e}")
    return tracks


# ─────────────────────────────────────────
# 3. MUSICBRAINZ
# ─────────────────────────────────────────
def fetch_musicbrainz_artist(artist_name):
    headers = {"User-Agent": "sa-artists-analytics/1.0 (portfolio project)"}
    try:
        # Search for artist
        search_url = "https://musicbrainz.org/ws/2/artist/"
        response = requests.get(search_url, params={
            "query":  f"artist:{artist_name}",
            "fmt":    "json",
            "limit":  1
        }, headers=headers)

        artists = response.json().get("artists", [])
        if not artists:
            return None

        artist = artists[0]
        tags   = [t["name"] for t in artist.get("tags", [])[:5]]

        return {
            "artist_name":    artist_name,
            "source":         "musicbrainz",
            "mbid":           artist.get("id", ""),
            "country":        artist.get("area", {}).get("name", "Unknown"),
            "type":           artist.get("type", "Unknown"),
            "gender":         artist.get("gender", "Unknown"),
            "begin_date":     artist.get("life-span", {}).get("begin", "Unknown"),
            "genres":         ", ".join(tags),
            "disambiguation": artist.get("disambiguation", ""),
        }
    except Exception as e:
        print(f"  ❌ MusicBrainz error for {artist_name}: {e}")
        return None

    finally:
        time.sleep(1)  # MusicBrainz rate limit: 1 request/second


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def fetch_all_data():
    youtube_data    = []
    lastfm_artists  = []
    lastfm_tracks   = []
    musicbrainz_data = []

    for artist_name in ARTISTS:
        print(f"\n🎵 Fetching: {artist_name}")

        # YouTube
        print("  📺 YouTube...")
        channel_id = YOUTUBE_CHANNEL_IDS.get(artist_name)
        if channel_id:
            videos = fetch_youtube_data(artist_name, channel_id)
            youtube_data.extend(videos)
            print(f"     ✅ {len(videos)} videos")

        # Last.fm
        print("  🎵 Last.fm...")
        artist_info = fetch_lastfm_artist(artist_name)
        if artist_info:
            lastfm_artists.append(artist_info)
            print(f"     ✅ {artist_info['listeners']:,} listeners")

        tracks = fetch_lastfm_top_tracks(artist_name)
        lastfm_tracks.extend(tracks)
        print(f"     ✅ {len(tracks)} top tracks")

        # MusicBrainz
        print("  🎼 MusicBrainz...")
        mb_data = fetch_musicbrainz_artist(artist_name)
        if mb_data:
            musicbrainz_data.append(mb_data)
            print(f"     ✅ Found: {mb_data['country']}, {mb_data['genres']}")

    # Convert to DataFrames
    df_youtube     = pd.DataFrame(youtube_data)
    df_lastfm      = pd.DataFrame(lastfm_artists)
    df_tracks      = pd.DataFrame(lastfm_tracks)
    df_musicbrainz = pd.DataFrame(musicbrainz_data)

    return df_youtube, df_lastfm, df_tracks, df_musicbrainz


if __name__ == "__main__":
    print("🚀 SA Artists Multi-Source Data Lake")
    print("=====================================")
    df_yt, df_lf, df_tr, df_mb = fetch_all_data()
    print(f"\n✅ YouTube videos:      {len(df_yt)}")
    print(f"✅ Last.fm artists:     {len(df_lf)}")
    print(f"✅ Last.fm top tracks:  {len(df_tr)}")
    print(f"✅ MusicBrainz artists: {len(df_mb)}")

    # Save CSVs
    df_yt.to_csv("data/youtube_videos.csv", index=False)
    df_lf.to_csv("data/lastfm_artists.csv", index=False)
    df_tr.to_csv("data/lastfm_tracks.csv", index=False)
    df_mb.to_csv("data/musicbrainz_artists.csv", index=False)
    print("\n✅ All data saved to data/ folder")
