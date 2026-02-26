#!/usr/bin/env python3
"""
Enhanced Spotify Collector for Complete Artist Profiling
Advanced data collection with financial analysis integration
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
import re
from pathlib import Path
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedSpotifyCollector:
    """Enhanced Spotify collector with financial analysis and bot detection"""
    
    def __init__(self):
        self.sp = None
        self.user_id = "w5j8x1tlo0desiwgo7f0ulpc1"
        self.collection_date = datetime.datetime.now().isoformat()
        self.collected_artists = []
        self.financial_analyzer = None
        
        # Bot detection patterns
        self.bot_patterns = {
            'numeric_names': re.compile(r'^[a-zA-Z]*\d+[a-zA-Z]*$'),
            'leetspeak': re.compile(r'[0-9]+[a-zA-Z]+[0-9]*'),
            'special_chars': re.compile(r'[øæß]+'),
            'perfect_ratios': re.compile(r'^[0-9]+k$'),
            'ai_keywords': ['ai', 'bot', 'auto', 'system', 'tech', 'digital', 'cyber']
        }
        
    def authenticate(self) -> bool:
        """Enhanced authentication with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                scope = "user-follow-read user-read-private user-read-email user-read-playback-state"
                
                sp_oauth = SpotifyOAuth(
                    scope=scope,
                    client_id=os.getenv('SPOTIFY_CLIENT_ID', ''),
                    client_secret=os.getenv('SPOTIFY_CLIENT_SECRET', ''),
                    redirect_uri=os.getenv('SPOTIFY_REDIRECT_URI', 'http://localhost:8888/callback'),
                    open_browser=False
                )
                
                token_info = sp_oauth.get_cached_token()
                if not token_info:
                    logger.info("No cached token found, requesting new token...")
                    token_info = sp_oauth.get_access_token(as_dict=True)
                
                if token_info and 'access_token' in token_info:
                    self.sp = spotipy.Spotify(auth=token_info['access_token'])
                    logger.info("✅ Spotify authentication successful")
                    return True
                else:
                    logger.error(f"❌ Authentication failed, attempt {attempt + 1}")
                    
            except Exception as e:
                logger.error(f"❌ Authentication error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    
        return False
    
    def get_all_followed_artists(self) -> List[Dict]:
        """Get all followed artists with enhanced error handling"""
        if not self.sp:
            logger.error("❌ Not authenticated")
            return []
        
        try:
            logger.info("🔍 Fetching all followed artists...")
            all_artists = []
            results = self.sp.current_user_followed_artists(limit=50)
            
            artist_count = 0
            while results:
                batch_artists = []
                for item in results['artists']['items']:
                    artist_count += 1
                    artist_data = {
                        'id': item['id'],
                        'name': item['name'],
                        'followers': item['followers']['total'],
                        'genres': item['genres'],
                        'popularity': item['popularity'],
                        'external_urls': item['external_urls'],
                        'uri': item['uri'],
                        'images': item['images'],
                        'collection_date': self.collection_date
                    }
                    
                    # Initial bot detection
                    artist_data['bot_indicators'] = self._detect_initial_bot_patterns(item['name'])
                    batch_artists.append(artist_data)
                
                all_artists.extend(batch_artists)
                logger.info(f"📊 Collected {len(batch_artists)} artists (total: {len(all_artists)})")
                
                if results['artists']['next']:
                    results = self.sp.next(results['artists'])
                    time.sleep(0.1)  # Rate limiting
                else:
                    break
            
            logger.info(f"✅ Total artists found: {len(all_artists)}")
            return all_artists
            
        except Exception as e:
            logger.error(f"❌ Error fetching artists: {e}")
            return []
    
    def _detect_initial_bot_patterns(self, artist_name: str) -> Dict:
        """Detect initial bot patterns in artist names"""
        indicators = {
            'numeric_pattern': bool(self.bot_patterns['numeric_names'].match(artist_name)),
            'leetspeak_pattern': bool(self.bot_patterns['leetspeak'].match(artist_name)),
            'special_chars': bool(self.bot_patterns['special_chars'].search(artist_name)),
            'ai_keywords': any(keyword in artist_name.lower() for keyword in self.bot_patterns['ai_keywords']),
            'name_length': len(artist_name),
            'has_numbers': bool(re.search(r'\d', artist_name)),
            'unusual_capitalization': artist_name != artist_name.lower() and artist_name != artist_name.upper()
        }
        
        # Calculate bot probability
        bot_score = sum(indicators.values()) / len(indicators)
        indicators['bot_probability'] = min(0.9, bot_score * 1.5)
        
        return indicators
    
    def get_comprehensive_artist_data(self, artist_id: str, basic_info: Dict) -> Dict:
        """Get comprehensive artist data with financial analysis"""
        try:
            # Basic artist info (already have)
            artist = basic_info
            
            # Get detailed artist info
            detailed_artist = self.sp.artist(artist_id)
            
            # Top tracks with play counts
            top_tracks = self.sp.artist_top_tracks(artist_id, country='DE')
            
            # Albums with detailed info
            albums = self.sp.artist_albums(artist_id, country='DE', limit=20)
            
            # Related artists for network analysis
            related_artists = self.sp.artist_related_artists(artist_id)
            
            # Artist's playlists (if available)
            try:
                playlists = self.sp.user_playlists(artist_id, limit=10)
            except:
                playlists = {'items': []}
            
            # Enhanced financial metrics
            monthly_listeners = self._extract_monthly_listeners(detailed_artist)
            estimated_revenue = self._calculate_estimated_revenue(monthly_listeners)
            
            comprehensive_data = {
                'basic_info': {
                    'id': detailed_artist['id'],
                    'name': detailed_artist['name'],
                    'followers': detailed_artist['followers']['total'],
                    'genres': detailed_artist['genres'],
                    'popularity': detailed_artist['popularity'],
                    'monthly_listeners': monthly_listeners,
                    'external_urls': detailed_artist['external_urls'],
                    'uri': detailed_artist['uri'],
                    'images': detailed_artist['images']
                },
                'financial_metrics': {
                    'estimated_monthly_revenue': estimated_revenue,
                    'estimated_annual_revenue': estimated_revenue * 12,
                    'revenue_per_follower': estimated_revenue / max(1, detailed_artist['followers']['total']),
                    'listener_to_follower_ratio': monthly_listeners / max(1, detailed_artist['followers']['total'])
                },
                'top_tracks': [
                    {
                        'name': track['name'],
                        'id': track['id'],
                        'popularity': track['popularity'],
                        'explicit': track['explicit'],
                        'duration_ms': track['duration_ms'],
                        'preview_url': track['preview_url'],
                        'external_urls': track['external_urls'],
                        'estimated_plays': self._estimate_track_plays(track['popularity'])
                    }
                    for track in top_tracks['tracks'][:10]
                ],
                'discography': {
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
                    'total_albums': len(albums['items']),
                    'total_singles': len([a for a in albums['items'] if a['album_type'] == 'single'])
                },
                'network_analysis': {
                    'related_artists': [
                        {
                            'name': related['name'],
                            'id': related['id'],
                            'popularity': related['popularity'],
                            'followers': related['followers']['total'],
                            'collaboration_strength': self._calculate_collaboration_strength(detailed_artist, related)
                        }
                        for related in related_artists['artists'][:10]
                    ],
                    'network_density': len(related_artists['artists']) / 50.0,  # Normalized
                    'genre_connections': self._analyze_genre_connections(detailed_artist['genres'])
                },
                'bot_analysis': {
                    'name_patterns': artist.get('bot_indicators', {}),
                    'engagement_anomalies': self._detect_engagement_anomalies(detailed_artist),
                    'temporal_patterns': self._analyze_temporal_patterns(albums['items']),
                    'overall_bot_score': self._calculate_overall_bot_score(detailed_artist, artist.get('bot_indicators', {}))
                },
                'security_assessment': {
                    'extremist_indicators': self._check_extremist_content(detailed_artist),
                    'propaganda_patterns': self._detect_propaganda_patterns(detailed_artist),
                    'security_risk_level': self._assess_security_risk(detailed_artist)
                },
                'collection_metadata': {
                    'collection_date': self.collection_date,
                    'data_completeness': self._calculate_data_completeness(detailed_artist, top_tracks, albums),
                    'archive_links': self._generate_archive_links(detailed_artist['external_urls'])
                }
            }
            
            return comprehensive_data
            
        except Exception as e:
            logger.error(f"❌ Error getting detailed data for {artist_id}: {e}")
            return {}
    
    def _extract_monthly_listeners(self, artist_data: Dict) -> int:
        """Extract monthly listeners from artist data (approximation)"""
        # Spotify doesn't provide monthly listeners via API, so we estimate based on followers and popularity
        followers = artist_data.get('followers', {}).get('total', 0)
        popularity = artist_data.get('popularity', 0)
        
        # Estimation formula based on typical ratios
        if popularity > 80:
            return int(followers * 10)
        elif popularity > 60:
            return int(followers * 5)
        elif popularity > 40:
            return int(followers * 2)
        else:
            return max(100, int(followers * 0.5))
    
    def _calculate_estimated_revenue(self, monthly_listeners: int) -> float:
        """Calculate estimated monthly revenue"""
        # Average streams per listener: 25
        # Average payout per stream: €0.0028
        streams_per_month = monthly_listeners * 25
        return streams_per_month * 0.0028
    
    def _estimate_track_plays(self, track_popularity: int) -> int:
        """Estimate track plays based on popularity score"""
        # Rough estimation: popularity score roughly correlates with log of plays
        if track_popularity > 80:
            return 1000000
        elif track_popularity > 60:
            return 500000
        elif track_popularity > 40:
            return 100000
        elif track_popularity > 20:
            return 50000
        else:
            return 10000
    
    def _calculate_collaboration_strength(self, artist1: Dict, artist2: Dict) -> float:
        """Calculate collaboration strength between two artists"""
        genre_overlap = len(set(artist1.get('genres', [])) & set(artist2.get('genres', [])))
        popularity_diff = abs(artist1.get('popularity', 0) - artist2.get('popularity', 0))
        
        # Higher overlap and lower popularity difference = stronger collaboration potential
        strength = (genre_overlap / max(1, len(artist1.get('genres', [])))) * (1 - popularity_diff / 100)
        return max(0.0, min(1.0, strength))
    
    def _analyze_genre_connections(self, genres: List[str]) -> Dict:
        """Analyze genre connections and patterns"""
        return {
            'primary_genres': genres[:3] if genres else [],
            'genre_count': len(genres),
            'has_experimental': any('experimental' in g.lower() for g in genres),
            'has_trap': any('trap' in g.lower() for g in genres),
            'has_german': any('german' in g.lower() for g in genres)
        }
    
    def _detect_engagement_anomalies(self, artist_data: Dict) -> Dict:
        """Detect engagement anomalies"""
        followers = artist_data.get('followers', {}).get('total', 0)
        popularity = artist_data.get('popularity', 0)
        
        anomalies = []
        
        # Check for unusual follower-to-popularity ratios
        if followers > 100000 and popularity < 30:
            anomalies.append('high_followers_low_popularity')
        elif followers < 1000 and popularity > 70:
            anomalies.append('low_followers_high_popularity')
        
        # Check for perfect round numbers (potential bot manipulation)
        if followers > 0 and followers % 10000 == 0:
            anomalies.append('round_follower_number')
        
        return {
            'anomalies': anomalies,
            'anomaly_count': len(anomalies),
            'risk_score': len(anomalies) * 0.2
        }
    
    def _analyze_temporal_patterns(self, albums: List[Dict]) -> Dict:
        """Analyze temporal release patterns"""
        if not albums:
            return {'pattern': 'no_data', 'regularity_score': 0.0}
        
        release_dates = []
        for album in albums:
            try:
                release_date = datetime.datetime.fromisoformat(album['release_date'].replace('Z', '+00:00'))
                release_dates.append(release_date)
            except:
                continue
        
        if len(release_dates) < 2:
            return {'pattern': 'insufficient_data', 'regularity_score': 0.0}
        
        # Calculate intervals between releases
        intervals = []
        release_dates.sort()
        for i in range(1, len(release_dates)):
            interval = (release_dates[i] - release_dates[i-1]).days
            intervals.append(interval)
        
        if not intervals:
            return {'pattern': 'no_intervals', 'regularity_score': 0.0}
        
        # Check for regular patterns (every 30 days, every 90 days, etc.)
        avg_interval = sum(intervals) / len(intervals)
        regularity = 1.0 - (sum(abs(i - avg_interval) for i in intervals) / (len(intervals) * avg_interval))
        
        pattern = 'regular' if regularity > 0.8 else 'irregular'
        
        return {
            'pattern': pattern,
            'regularity_score': regularity,
            'avg_interval_days': avg_interval,
            'total_releases': len(albums)
        }
    
    def _calculate_overall_bot_score(self, artist_data: Dict, name_patterns: Dict) -> float:
        """Calculate overall bot probability score"""
        name_score = name_patterns.get('bot_probability', 0.0)
        engagement_score = self._detect_engagement_anomalies(artist_data).get('risk_score', 0.0)
        
        # Combine scores with weights
        overall_score = (name_score * 0.6) + (engagement_score * 0.4)
        
        return min(0.9, overall_score)
    
    def _check_extremist_content(self, artist_data: Dict) -> Dict:
        """Check for potential extremist content indicators"""
        # This would require actual content analysis of lyrics and descriptions
        # For now, return placeholder
        return {
            'indicators': [],
            'risk_level': 'low',
            'requires_manual_review': False
        }
    
    def _detect_propaganda_patterns(self, artist_data: Dict) -> Dict:
        """Detect potential propaganda patterns"""
        # This would require sophisticated content analysis
        return {
            'patterns': [],
            'propaganda_probability': 0.0,
            'requires_monitoring': False
        }
    
    def _assess_security_risk(self, artist_data: Dict) -> str:
        """Assess overall security risk level"""
        bot_score = self._calculate_overall_bot_score(artist_data, {})
        
        if bot_score > 0.7:
            return 'high'
        elif bot_score > 0.4:
            return 'medium'
        else:
            return 'low'
    
    def _calculate_data_completeness(self, artist_data: Dict, top_tracks: Dict, albums: Dict) -> float:
        """Calculate data completeness score"""
        completeness_factors = [
            1.0 if artist_data.get('name') else 0.0,
            1.0 if artist_data.get('followers', {}).get('total', 0) > 0 else 0.0,
            1.0 if len(top_tracks.get('tracks', [])) > 0 else 0.0,
            1.0 if len(albums.get('items', [])) > 0 else 0.0,
            1.0 if len(artist_data.get('genres', [])) > 0 else 0.0
        ]
        
        return sum(completeness_factors) / len(completeness_factors)
    
    def _generate_archive_links(self, external_urls: Dict) -> Dict:
        """Generate archive links for documentation"""
        links = {}
        for platform, url in external_urls.items():
            if platform == 'spotify':
                links['spotify_archive'] = f"https://archive.is/?run=1&url={quote(url)}"
        return links
    
    def create_enhanced_artist_profile(self, artist_data: Dict) -> str:
        """Create enhanced markdown profile with all analysis"""
        name = artist_data['basic_info']['name']
        filename = f"{name.replace('/', '_').replace('?', '_').replace(':', '_').replace(' ', '_')}.md"
        
        profile = f"""# {name}

## Basic Information
- **Spotify ID**: {artist_data['basic_info']['id']}
- **Monthly Listeners**: {artist_data['basic_info']['monthly_listeners']:,}
- **Followers**: {artist_data['basic_info']['followers']:,}
- **Popularity**: {artist_data['basic_info']['popularity']}/100
- **Genres**: {', '.join(artist_data['basic_info']['genres']) if artist_data['basic_info']['genres'] else 'N/A'}
- **Collection Date**: {artist_data['collection_metadata']['collection_date']}

## Financial Metrics
- **Estimated Monthly Revenue**: €{artist_data['financial_metrics']['estimated_monthly_revenue']:,.2f}
- **Estimated Annual Revenue**: €{artist_data['financial_metrics']['estimated_annual_revenue']:,.2f}
- **Revenue per Follower**: €{artist_data['financial_metrics']['revenue_per_follower']:.4f}
- **Listener-to-Follower Ratio**: {artist_data['financial_metrics']['listener_to_follower_ratio']:.2f}

## Spotify Links
- **Direct Link**: {artist_data['basic_info']['external_urls']['spotify']}
- **URI**: {artist_data['basic_info']['uri']}

## Top Tracks
"""
        
        for i, track in enumerate(artist_data['top_tracks'][:5], 1):
            profile += f"""{i}. **{track['name']}** - Popularity: {track['popularity']}, Estimated Plays: {track['estimated_plays']:,}, Explicit: {'Yes' if track['explicit'] else 'No'}
"""
        
        profile += f"""
## Discography
- **Total Albums**: {artist_data['discography']['total_albums']}
- **Total Singles**: {artist_data['discography']['total_singles']}

### Recent Albums
"""
        
        for album in artist_data['discography']['albums'][:5]:
            profile += f"- **{album['name']}** ({album['release_date']}) - {album['total_tracks']} tracks\n"
        
        profile += f"""
## Network Analysis
- **Network Density**: {artist_data['network_analysis']['network_density']:.2f}
- **Genre Connections**: {', '.join(artist_data['network_analysis']['genre_connections']['primary_genres'])}

### Related Artists
"""
        
        for related in artist_data['network_analysis']['related_artists'][:5]:
            profile += f"- **{related['name']}** - {related['followers']:,} followers, Collaboration Strength: {related['collaboration_strength']:.2f}\n"
        
        profile += f"""
## Bot Analysis
- **Overall Bot Score**: {artist_data['bot_analysis']['overall_bot_score']:.2f}
- **Name Pattern Risk**: {artist_data['bot_analysis']['name_patterns'].get('bot_probability', 0.0):.2f}
- **Engagement Anomalies**: {len(artist_data['bot_analysis']['engagement_anomalies']['anomalies'])} detected
- **Temporal Pattern**: {artist_data['bot_analysis']['temporal_patterns']['pattern']}

## Security Assessment
- **Risk Level**: {artist_data['security_assessment']['security_risk_level'].upper()}
- **Extremist Indicators**: {len(artist_data['security_assessment']['extremist_indicators']['indicators'])}
- **Propaganda Patterns**: {len(artist_data['security_assessment']['propaganda_patterns']['patterns'])}
- **Requires Manual Review**: {artist_data['security_assessment']['extremist_indicators']['requires_manual_review']}

## Archival Information
- **Archive.is Link**: {artist_data['collection_metadata']['archive_links'].get('spotify_archive', 'N/A')}
- **Data Completeness**: {artist_data['collection_metadata']['data_completeness']:.1%}
- **Collection Method**: Enhanced Spotify Web API

---

*This enhanced profile is part of a doctoral thesis project in collaboration with the Bundesamt für Verfassungsschutz (BfV).*
*Last Updated: {artist_data['collection_metadata']['collection_date']}*
*Analysis includes financial projections and bot detection assessment*
"""
        
        return filename, profile
    
    def save_enhanced_artist_data(self, artist_data: Dict):
        """Save enhanced artist data with comprehensive analysis"""
        filename, profile = self.create_enhanced_artist_profile(artist_data)
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(profile)
            logger.info(f"✅ Enhanced profile saved: {filename}")
        except Exception as e:
            logger.error(f"❌ Error saving {filename}: {e}")
    
    def collect_all_remaining_artists(self):
        """Collect data for all remaining artists with enhanced analysis"""
        logger.info("🚀 Starting enhanced data collection...")
        
        # Authentication
        if not self.authenticate():
            logger.error("❌ Authentication failed")
            return
        
        # Get all followed artists
        all_artists = self.get_all_followed_artists()
        
        if not all_artists:
            logger.error("❌ No artists found")
            return
        
        # Check which artists are already collected
        existing_files = set()
        for file in Path('.').glob("*.md"):
            if file.name not in ["README.md", "BfV_Final_Security_Assessment.md", "financial_analysis_report.md"]:
                existing_files.add(file.stem.lower().replace(' ', '_'))
        
        # Collect new artists
        new_artists = 0
        total_artists = len(all_artists)
        
        for i, artist in enumerate(all_artists, 1):
            artist_name_key = artist['name'].lower().replace(' ', '_')
            
            if artist_name_key not in existing_files:
                logger.info(f"📊 Collecting enhanced data for {i}/{total_artists}: {artist['name']}")
                
                # Get comprehensive data
                comprehensive_data = self.get_comprehensive_artist_data(artist['id'], artist)
                
                if comprehensive_data:
                    self.save_enhanced_artist_data(comprehensive_data)
                    new_artists += 1
                    self.collected_artists.append(comprehensive_data)
                
                # Rate limiting with progressive backoff
                time.sleep(0.2)
            else:
                logger.info(f"⏭️ Skipping {artist['name']} (already collected)")
        
        logger.info(f"✅ Enhanced collection complete: {new_artists} new artists")
        logger.info(f"📊 Total progress: {len(existing_files) + new_artists}/{total_artists} artists")
        
        # Generate financial report
        if self.collected_artists:
            self._generate_financial_summary()
    
    def _generate_financial_summary(self):
        """Generate financial summary of collected artists"""
        try:
            from financial_analyzer import SpotifyFinancialAnalyzer
            
            analyzer = SpotifyFinancialAnalyzer()
            
            # Convert collected data to financial analyzer format
            financial_data = []
            for artist in self.collected_artists:
                financial_data.append({
                    'name': artist['basic_info']['name'],
                    'monthly_listeners': artist['basic_info']['monthly_listeners'],
                    'followers': artist['basic_info']['followers'],
                    'bot_probability': artist['bot_analysis']['overall_bot_score']
                })
            
            if financial_data:
                report = analyzer.generate_financial_report(financial_data)
                
                with open('enhanced_financial_report.md', 'w', encoding='utf-8') as f:
                    f.write(report)
                
                logger.info("✅ Enhanced financial report generated: enhanced_financial_report.md")
        
        except ImportError:
            logger.warning("⚠️ Financial analyzer not available")
        except Exception as e:
            logger.error(f"❌ Error generating financial report: {e}")

def main():
    """Main function for enhanced collection"""
    logger.info("🎵 Enhanced Spotify Artist Profiling Collector")
    logger.info("📚 Doctoral Thesis in Collaboration with BfV")
    logger.info("=" * 50)
    
    collector = EnhancedSpotifyCollector()
    collector.collect_all_remaining_artists()
    
    logger.info("🎉 Enhanced collection process completed!")

if __name__ == "__main__":
    main()
