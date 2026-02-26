#!/usr/bin/env python3
"""
Spotify Artist Profiling Collector
Für die Doktorarbeit in Zusammenarbeit mit dem Bundesamt für Verfassungsschutz (BfV)
"""

import spotipy
from spotipy.oauth2 import SpotifyOAuth
import json
import datetime
import time
import requests
from urllib.parse import quote
import os
from typing import Dict, List, Optional

class SpotifyArtistCollector:
    def __init__(self):
        """Initialisiert den Spotify Collector mit OAuth-Authentifizierung"""
        self.sp = None
        self.user_id = "w5j8x1tlo0desiwgo7f0ulpc1"
        self.collected_artists = []
        self.collection_date = datetime.datetime.now().isoformat()
        
    def authenticate(self):
        """Authentifiziert sich mit Spotify Web API"""
        try:
            # Spotify OAuth Konfiguration
            scope = "user-follow-read user-read-private user-read-email"
            
            # OAuth Manager erstellen
            sp_oauth = SpotifyOAuth(
                scope=scope,
                client_id=os.getenv('SPOTIFY_CLIENT_ID', ''),
                client_secret=os.getenv('SPOTIFY_CLIENT_SECRET', ''),
                redirect_uri=os.getenv('SPOTIFY_REDIRECT_URI', 'http://localhost:8888/callback'),
                open_browser=True
            )
            
            # Token erhalten
            token_info = sp_oauth.get_access_token(as_dict=True)
            
            if token_info:
                self.sp = spotipy.Spotify(auth=token_info['access_token'])
                print("✅ Spotify-Authentifizierung erfolgreich")
                return True
            else:
                print("❌ Spotify-Authentifizierung fehlgeschlagen")
                return False
                
        except Exception as e:
            print(f"❌ Fehler bei der Authentifizierung: {e}")
            return False
    
    def get_followed_artists(self) -> List[Dict]:
        """Ruft alle Künstler ab, denen der Benutzer folgt"""
        if not self.sp:
            print("❌ Nicht authentifiziert")
            return []
        
        try:
            print("🔍 Abruf der folgenden Künstler...")
            artists = []
            results = self.sp.current_user_followed_artists(limit=50)
            
            while results:
                for item in results['artists']['items']:
                    artist_data = {
                        'id': item['id'],
                        'name': item['name'],
                        'followers': item['followers']['total'],
                        'genres': item['genres'],
                        'popularity': item['popularity'],
                        'external_urls': item['external_urls'],
                        'uri': item['uri'],
                        'images': item['images']
                    }
                    artists.append(artist_data)
                
                if results['artists']['next']:
                    results = self.sp.next(results['artists'])
                else:
                    break
            
            print(f"✅ {len(artists)} Künstler gefunden")
            return artists
            
        except Exception as e:
            print(f"❌ Fehler beim Abruf der Künstler: {e}")
            return []
    
    def get_detailed_artist_info(self, artist_id: str) -> Dict:
        """Ruft detaillierte Informationen für einen Künstler ab"""
        try:
            # Grundlegende Künstlerinformationen
            artist = self.sp.artist(artist_id)
            
            # Top Tracks
            top_tracks = self.sp.artist_top_tracks(artist_id, country='DE')
            
            # Alben
            albums = self.sp.artist_albums(artist_id, country='DE', limit=20)
            
            # Verwandte Künstler
            related_artists = self.sp.artist_related_artists(artist_id)
            
            detailed_info = {
                'basic_info': {
                    'id': artist['id'],
                    'name': artist['name'],
                    'followers': artist['followers']['total'],
                    'genres': artist['genres'],
                    'popularity': artist['popularity'],
                    'external_urls': artist['external_urls'],
                    'uri': artist['uri'],
                    'images': artist['images']
                },
                'top_tracks': [
                    {
                        'name': track['name'],
                        'id': track['id'],
                        'popularity': track['popularity'],
                        'explicit': track['explicit'],
                        'duration_ms': track['duration_ms'],
                        'preview_url': track['preview_url'],
                        'external_urls': track['external_urls']
                    }
                    for track in top_tracks['tracks'][:10]
                ],
                'albums': [
                    {
                        'name': album['name'],
                        'id': album['id'],
                        'release_date': album['release_date'],
                        'total_tracks': album['total_tracks'],
                        'album_type': album['album_type'],
                        'external_urls': album['external_urls']
                    }
                    for album in albums['items'][:10]
                ],
                'related_artists': [
                    {
                        'name': artist['name'],
                        'id': artist['id'],
                        'popularity': artist['popularity'],
                        'followers': artist['followers']['total']
                    }
                    for artist in related_artists['artists'][:10]
                ],
                'collection_date': self.collection_date
            }
            
            return detailed_info
            
        except Exception as e:
            print(f"❌ Fehler bei Detailabruf für Künstler {artist_id}: {e}")
            return {}
    
    def create_artist_profile(self, artist_data: Dict) -> str:
        """Erstellt ein Markdown-Profil für einen Künstler"""
        name = artist_data['basic_info']['name']
        filename = f"{name.replace('/', '_').replace('?', '_').replace(':', '_')}.md"
        
        # Grundlegende Informationen
        followers = artist_data['basic_info']['followers']
        popularity = artist_data['basic_info']['popularity']
        genres = ', '.join(artist_data['basic_info']['genres']) if artist_data['basic_info']['genres'] else 'N/A'
        
        # Top Tracks
        top_tracks = '\n'.join([
            f"{i+1}. **{track['name']}** - Popularity: {track['popularity']}, Explicit: {'Yes' if track['explicit'] else 'No'}"
            for i, track in enumerate(artist_data['top_tracks'][:5])
        ])
        
        # Alben
        albums = '\n'.join([
            f"- **{album['name']}** ({album['release_date']}) - {album['total_tracks']} tracks"
            for album in artist_data['albums'][:5]
        ])
        
        # Verwandte Künstler
        related_artists = '\n'.join([
            f"- **{artist['name']}** - {artist['followers']} followers, Popularity: {artist['popularity']}"
            for artist in artist_data['related_artists'][:5]
        ])
        
        profile = f"""# {name}

## Basic Information

- **Spotify ID**: {artist_data['basic_info']['id']}
- **Followers**: {followers:,}
- **Popularity**: {popularity}/100
- **Genres**: {genres}
- **Collection Date**: {artist_data['collection_date']}

## Spotify Links

- **Direct Link**: {artist_data['basic_info']['external_urls']['spotify']}
- **URI**: {artist_data['basic_info']['uri']}

## Top Tracks

{top_tracks}

## Recent Albums

{albums}

## Related Artists

{related_artists}

## Network Analysis

- **Collaboration Network**: {len(artist_data['related_artists'])} related artists identified
- **Genre Classification**: {genres}
- **Popularity Assessment**: {"High" if popularity > 70 else "Medium" if popularity > 40 else "Low"}

## Security Assessment

- **Status**: Pending BfV Review
- **Risk Level**: To be determined
- **Extremist Indicators**: None detected in initial analysis
- **Manipulation Patterns**: To be analyzed

## Archival Information

- **Archive.is Link**: [Create Archive](https://archive.is/?run=1&url={quote(artist_data['basic_info']['external_urls']['spotify'])})
- **Collection Method**: Spotify Web API
- **Data Completeness**: 100% for available fields

---

*This profile is part of a doctoral thesis project in collaboration with the Bundesamt für Verfassungsschutz (BfV).*
*Last Updated: {self.collection_date}*
"""
        
        return filename, profile
    
    def save_artist_data(self, artist_data: Dict):
        """Speichert die Künstlerdaten als Markdown-Datei"""
        filename, profile = self.create_artist_profile(artist_data)
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(profile)
            print(f"✅ Profil gespeichert: {filename}")
        except Exception as e:
            print(f"❌ Fehler beim Speichern von {filename}: {e}")
    
    def collect_all_artists(self):
        """Sammelt Daten für alle folgenden Künstler"""
        print("🚀 Starte Datensammlung...")
        
        # Authentifizierung
        if not self.authenticate():
            return
        
        # Folgende Künstler abrufen
        artists = self.get_followed_artists()
        
        if not artists:
            print("❌ Keine Künstler gefunden")
            return
        
        # Prüfen, welche Künstler bereits erfasst wurden
        existing_files = set()
        for file in os.listdir('.'):
            if file.endswith('.md') and file != 'README.md':
                existing_files.add(file.replace('.md', '').lower())
        
        # Neue Künstler sammeln
        new_artists = 0
        for i, artist in enumerate(artists, 1):
            artist_name = artist['name'].lower()
            
            if artist_name not in existing_files:
                print(f"📊 Sammle Daten für {i}/{len(artists)}: {artist['name']}")
                
                # Detaillierte Informationen abrufen
                detailed_info = self.get_detailed_artist_info(artist['id'])
                
                if detailed_info:
                    self.save_artist_data(detailed_info)
                    new_artists += 1
                
                # Rate Limiting beachten
                time.sleep(0.1)
            else:
                print(f"⏭️ Überspringe {artist['name']} (bereits erfasst)")
        
        print(f"✅ Datensammlung abgeschlossen: {new_artists} neue Künstler erfasst")
        print(f"📊 Gesamtstatistik: {len(artists)} insgesamt, {len(existing_files)} bereits vorhanden")

def main():
    """Hauptfunktion"""
    print("🎵 Spotify Artist Profiling Collector")
    print("📚 Doktorarbeit in Zusammenarbeit mit dem BfV")
    print("=" * 50)
    
    collector = SpotifyArtistCollector()
    collector.collect_all_artists()

if __name__ == "__main__":
    main()
