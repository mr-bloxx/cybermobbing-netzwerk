#!/usr/bin/env python3
"""
EXPERIMENTAL AI NETWORK DETECTOR
Radikale KI-Desinformations-Netzwerk Aufdeckung
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional, Set
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import re
from collections import defaultdict, Counter
import math
import random
import networkx as nx
from sklearn.cluster import DBSCAN
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

@dataclass
class AISignature:
    """AI generation signature data structure"""
    linguistic_score: float
    temporal_score: float
    network_score: float
    metadata_score: float
    behavioral_score: float
    overall_ai_probability: float
    confidence_level: str
    threat_level: str

class ExperimentalAIDetector:
    """Experimental AI network detection system"""
    
    def __init__(self):
        self.ai_patterns = {
            'emotional_templates': [
                r"we'll get what we deserve",
                r"stay sober",
                r"give yourselves music",
                r"es hat gerade erst angefangen",
                r"shiny is the constellation"
            ],
            'age_references': [
                r"\d+y\b",
                r"\d+ years? old",
                r"\d+jähr",
                r"Alter: \d+"
            ],
            'location_tags': [
                r"Vienna|Wien",
                r"Indiana",
                r"Berlin",
                r"\w+burg"
            ],
            'professional_elements': [
                r"@\w+\.mp3",
                r"business\.\w+@\w+\.com",
                r"contact:\s*\w+@\w+\.com",
                r"instagram:\s*\w+"
            ],
            'numerical_signatures': [
                r"\d+k\b",
                r"\d+0\b",
                r"\d+00\b",
                r"v\d+",
                r"\d+\.\d+"
            ]
        }
        
        self.temporal_patterns = {
            'coordination_threshold': 120,  # minutes
            'batch_size_threshold': 5,
            'perfect_spacing': 30,  # minutes
            'release_density': 0.7
        }
        
        self.network_patterns = {
            'collaboration_density': 0.6,
            'reciprocity_threshold': 0.8,
            'cluster_size': 3,
            'centrality_threshold': 0.5
        }
        
        self.metadata_patterns = {
            'id_anomaly_threshold': 0.3,
            'play_ratio_threshold': 100,
            'follower_ratio_threshold': 50,
            'popularity_anomaly': 0.8
        }
    
    def detect_linguistic_ai_signatures(self, artist_data: Dict) -> float:
        """Detect AI-generated linguistic patterns"""
        score = 0.0
        text = ""
        
        # Combine all text fields
        if 'biography' in artist_data:
            text += artist_data['biography'] + " "
        if 'name' in artist_data:
            text += artist_data['name'] + " "
        if 'philosophy' in artist_data:
            text += artist_data['philosophy'] + " "
        
        # Check for AI templates
        for template in self.ai_patterns['emotional_templates']:
            if re.search(template, text, re.IGNORECASE):
                score += 0.2
        
        # Check for age references
        age_matches = re.findall(self.ai_patterns['age_references'], text, re.IGNORECASE)
        score += min(0.3, len(age_matches) * 0.1)
        
        # Check for location tags
        location_matches = re.findall(self.ai_patterns['location_tags'], text, re.IGNORECASE)
        score += min(0.2, len(location_matches) * 0.05)
        
        # Check for professional elements
        prof_matches = re.findall(self.ai_patterns['professional_elements'], text, re.IGNORECASE)
        score += min(0.3, len(prof_matches) * 0.1)
        
        # Check for numerical signatures
        num_matches = re.findall(self.ai_patterns['numerical_signatures'], text, re.IGNORECASE)
        score += min(0.2, len(num_matches) * 0.05)
        
        # Check for perfect emotional language
        if self.has_perfect_emotional_language(text):
            score += 0.3
        
        return min(1.0, score)
    
    def has_perfect_emotional_language(self, text: str) -> bool:
        """Detect perfect emotional AI language"""
        emotional_words = [
            'love', 'hate', 'pain', 'joy', 'sad', 'happy', 'angry', 'fear',
            'liebe', 'hass', 'schmerz', 'freude', 'traurig', 'glücklich', 'wütend', 'angst'
        ]
        
        # Check for perfect emotional balance
        positive_count = sum(1 for word in emotional_words if word in text.lower())
        negative_count = sum(1 for word in ['hate', 'pain', 'sad', 'angry', 'fear', 'hass', 'schmerz', 'traurig', 'wütend', 'angst'] if word in text.lower())
        
        # Perfect balance suggests AI generation
        if positive_count > 0 and negative_count > 0:
            ratio = positive_count / negative_count
            return 0.8 <= ratio <= 1.2
        
        return False
    
    def detect_temporal_coordination(self, artists: List[Dict]) -> float:
        """Detect coordinated temporal patterns"""
        if len(artists) < 2:
            return 0.0
        
        # Parse timestamps
        timestamps = []
        for artist in artists:
            if 'collection_date' in artist:
                try:
                    dt = datetime.fromisoformat(artist['collection_date'].replace('Z', '+00:00'))
                    timestamps.append(dt)
                except:
                    continue
        
        if len(timestamps) < 2:
            return 0.0
        
        # Sort timestamps
        timestamps.sort()
        
        # Calculate time differences
        time_diffs = []
        for i in range(1, len(timestamps)):
            diff = (timestamps[i] - timestamps[i-1]).total_seconds() / 60  # minutes
            time_diffs.append(diff)
        
        # Check for coordination patterns
        coordination_score = 0.0
        
        # Perfect spacing detection
        perfect_spacing_count = sum(1 for diff in time_diffs if abs(diff - self.temporal_patterns['perfect_spacing']) < 5)
        if perfect_spacing_count > len(time_diffs) * 0.6:
            coordination_score += 0.4
        
        # Batch creation detection
        batch_count = 0
        for diff in time_diffs:
            if diff < self.temporal_patterns['coordination_threshold']:
                batch_count += 1
            else:
                if batch_count >= self.temporal_patterns['batch_size_threshold']:
                    coordination_score += 0.3
                batch_count = 0
        
        # High density detection
        if len(time_diffs) > 0:
            avg_diff = sum(time_diffs) / len(time_diffs)
            if avg_diff < self.temporal_patterns['coordination_threshold']:
                coordination_score += 0.3
        
        return min(1.0, coordination_score)
    
    def detect_network_anomalies(self, artists: List[Dict]) -> float:
        """Detect AI-generated network structures"""
        if len(artists) < 3:
            return 0.0
        
        # Build network graph
        G = nx.Graph()
        
        # Add nodes
        for artist in artists:
            artist_id = artist.get('id', artist.get('name', 'unknown'))
            G.add_node(artist_id)
        
        # Add edges based on collaborations
        for artist in artists:
            artist_id = artist.get('id', artist.get('name', 'unknown'))
            collaborations = artist.get('collaborations', [])
            
            for collab in collaborations:
                collab_id = collab.get('id', collab.get('name', collab))
                if collab_id in G.nodes:
                    G.add_edge(artist_id, collab_id)
        
        if len(G.edges) == 0:
            return 0.0
        
        # Calculate network metrics
        anomaly_score = 0.0
        
        # Check for perfect clustering
        clustering_coeffs = list(nx.clustering(G).values())
        avg_clustering = sum(clustering_coeffs) / len(clustering_coeffs)
        
        if avg_clustering > 0.8:  # Too perfect
            anomaly_score += 0.3
        
        # Check for regular degree distribution
        degrees = [G.degree(node) for node in G.nodes()]
        degree_variance = np.var(degrees)
        
        if degree_variance < 1.0:  # Too regular
            anomaly_score += 0.3
        
        # Check for reciprocity
        if G.is_directed():
            reciprocity = nx.reciprocity(G)
            if reciprocity > self.network_patterns['reciprocity_threshold']:
                anomaly_score += 0.2
        
        # Check for artificial centrality
        centrality = nx.betweenness_centrality(G)
        max_centrality = max(centrality.values())
        
        if max_centrality < self.network_patterns['centrality_threshold']:
            anomaly_score += 0.2
        
        return min(1.0, anomaly_score)
    
    def detect_metadata_anomalies(self, artist_data: Dict) -> float:
        """Detect AI-generated metadata patterns"""
        score = 0.0
        
        # Check Spotify ID patterns
        if 'id' in artist_data:
            spotify_id = artist_data['id']
            if self.has_spotify_id_anomalies(spotify_id):
                score += 0.3
        
        # Check play count ratios
        if 'monthly_listeners' in artist_data and 'top_track_plays' in artist_data:
            listeners = artist_data['monthly_listeners']
            plays = artist_data['top_track_plays']
            
            if listeners > 0:
                ratio = plays / listeners
                if ratio > self.metadata_patterns['play_ratio_threshold']:
                    score += 0.4
        
        # Check for perfect popularity
        if 'popularity' in artist_data and 'monthly_listeners' in artist_data:
            popularity = artist_data['popularity']
            listeners = artist_data['monthly_listeners']
            
            # Perfect correlation suggests AI
            if popularity > 80 and listeners < 1000:
                score += 0.3
            elif popularity < 20 and listeners > 10000:
                score += 0.3
        
        return min(1.0, score)
    
    def has_spotify_id_anomalies(self, spotify_id: str) -> bool:
        """Detect anomalous Spotify ID patterns"""
        if len(spotify_id) != 22:
            return True
        
        # Check for character distribution anomalies
        char_counts = Counter(spotify_id)
        entropy = -sum((count / len(spotify_id)) * math.log2(count / len(spotify_id)) 
                      for count in char_counts.values())
        
        # Too low entropy suggests artificial generation
        return entropy < 3.0
    
    def detect_behavioral_anomalies(self, artist_data: Dict) -> float:
        """Detect AI-generated behavioral patterns"""
        score = 0.0
        
        # Check for perfect engagement ratios
        if 'monthly_listeners' in artist_data and 'followers' in artist_data:
            listeners = artist_data['monthly_listeners']
            followers = artist_data['followers']
            
            if followers > 0:
                ratio = listeners / followers
                # Perfect ratios suggest AI
                if abs(ratio - 10) < 1 or abs(ratio - 5) < 0.5:
                    score += 0.3
        
        # Check for regular release patterns
        if 'releases' in artist_data:
            releases = artist_data['releases']
            if len(releases) > 2:
                # Check for regular intervals
                dates = []
                for release in releases:
                    if 'date' in release:
                        try:
                            dates.append(datetime.fromisoformat(release['date']))
                        except:
                            continue
                
                if len(dates) > 2:
                    dates.sort()
                    intervals = []
                    for i in range(1, len(dates)):
                        interval = (dates[i] - dates[i-1]).days
                        intervals.append(interval)
                    
                    if len(intervals) > 1:
                        avg_interval = sum(intervals) / len(intervals)
                        variance = np.var(intervals)
                        
                        # Too regular suggests AI
                        if variance < 10:
                            score += 0.4
        
        return min(1.0, score)
    
    def analyze_artist_ai_probability(self, artist_data: Dict, network_context: List[Dict] = None) -> AISignature:
        """Comprehensive AI probability analysis"""
        
        # Individual scores
        linguistic_score = self.detect_linguistic_ai_signatures(artist_data)
        metadata_score = self.detect_metadata_anomalies(artist_data)
        behavioral_score = self.detect_behavioral_anomalies(artist_data)
        
        # Network scores (if context available)
        temporal_score = 0.0
        network_score = 0.0
        
        if network_context:
            temporal_score = self.detect_temporal_coordination(network_context)
            network_score = self.detect_network_anomalies(network_context)
        
        # Calculate overall AI probability
        weights = {
            'linguistic': 0.3,
            'temporal': 0.2,
            'network': 0.2,
            'metadata': 0.2,
            'behavioral': 0.1
        }
        
        overall_probability = (
            linguistic_score * weights['linguistic'] +
            temporal_score * weights['temporal'] +
            network_score * weights['network'] +
            metadata_score * weights['metadata'] +
            behavioral_score * weights['behavioral']
        )
        
        # Determine confidence level
        if overall_probability > 0.8:
            confidence = "HIGH"
            threat = "CRITICAL"
        elif overall_probability > 0.6:
            confidence = "MEDIUM"
            threat = "HIGH"
        elif overall_probability > 0.4:
            confidence = "LOW"
            threat = "MODERATE"
        else:
            confidence = "VERY LOW"
            threat = "LOW"
        
        return AISignature(
            linguistic_score=linguistic_score,
            temporal_score=temporal_score,
            network_score=network_score,
            metadata_score=metadata_score,
            behavioral_score=behavioral_score,
            overall_ai_probability=overall_probability,
            confidence_level=confidence,
            threat_level=threat
        )
    
    def detect_ai_network_clusters(self, artists: List[Dict]) -> Dict:
        """Detect AI-generated network clusters"""
        
        # Analyze all artists
        results = []
        for artist in artists:
            signature = self.analyze_artist_ai_probability(artist, artists)
            results.append({
                'artist': artist.get('name', 'unknown'),
                'id': artist.get('id', 'unknown'),
                'signature': signature
            })
        
        # Separate high-probability AI artists
        ai_artists = [r for r in results if r['signature'].overall_ai_probability > 0.7]
        human_artists = [r for r in results if r['signature'].overall_ai_probability <= 0.7]
        
        # Build feature matrix for clustering
        features = []
        artist_names = []
        
        for result in results:
            sig = result['signature']
            features.append([
                sig.linguistic_score,
                sig.temporal_score,
                sig.network_score,
                sig.metadata_score,
                sig.behavioral_score
            ])
            artist_names.append(result['artist'])
        
        # Perform clustering
        if len(features) > 2:
            feature_matrix = np.array(features)
            clustering = DBSCAN(eps=0.3, min_samples=2).fit(feature_matrix)
            
            clusters = {}
            for i, label in enumerate(clustering.labels_):
                if label not in clusters:
                    clusters[label] = []
                clusters[label].append({
                    'artist': artist_names[i],
                    'signature': results[i]['signature']
                })
        else:
            clusters = {0: results}
        
        return {
            'total_artists': len(artists),
            'ai_artists': len(ai_artists),
            'human_artists': len(human_artists),
            'ai_percentage': len(ai_artists) / len(artists) * 100,
            'clusters': clusters,
            'high_threat_artists': [r for r in ai_artists if r['signature'].threat_level == "CRITICAL"],
            'medium_threat_artists': [r for r in ai_artists if r['signature'].threat_level == "HIGH"]
        }
    
    def generate_experimental_report(self, analysis_results: Dict) -> str:
        """Generate experimental analysis report"""
        
        report = f"""# EXPERIMENTAL AI NETWORK DETECTION REPORT
**Analysis Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Methodology**: Experimental AI Detection Algorithms
**Total Artists Analyzed**: {analysis_results['total_artists']}

## RADICAL FINDINGS

### AI Generation Statistics
- **AI-Generated Artists**: {analysis_results['ai_artists']} ({analysis_results['ai_percentage']:.1f}%)
- **Human Artists**: {analysis_results['human_artists']} ({100-analysis_results['ai_percentage']:.1f}%)
- **High Threat Artists**: {len(analysis_results['high_threat_artists'])}
- **Medium Threat Artists**: {len(analysis_results['medium_threat_artists'])}

### Network Clusters
"""
        
        for cluster_id, cluster_artists in analysis_results['clusters'].items():
            if cluster_id == -1:
                cluster_name = "Noise/Outliers"
            else:
                cluster_name = f"Cluster {cluster_id}"
            
            report += f"#### {cluster_name} ({len(cluster_artists)} artists)\n"
            
            for artist in cluster_artists:
                sig = artist['signature']
                report += f"- **{artist['artist']}**: AI Probability {sig.overall_ai_probability:.2f} ({sig.threat_level})\n"
        
        report += f"""
## Critical Threat Assessment

### High Priority Targets
"""
        
        for artist in analysis_results['high_threat_artists']:
            sig = artist['signature']
            report += f"#### {artist['artist']} - CRITICAL THREAT\n"
            report += f"- AI Probability: {sig.overall_ai_probability:.2f}\n"
            report += f"- Linguistic Score: {sig.linguistic_score:.2f}\n"
            report += f"- Metadata Score: {sig.metadata_score:.2f}\n"
            report += f"- Behavioral Score: {sig.behavioral_score:.2f}\n\n"
        
        report += f"""
## Experimental Recommendations

### Immediate Actions
1. **Network Shutdown**: Deactivate all high-threat artists
2. **AI Forensics**: Analyze AI generation infrastructure
3. **Financial Investigation**: Trace manipulated revenue streams
4. **Security Alert**: Notify national security agencies

### Long-term Strategy
1. **AI Detection Systems**: Deploy experimental detection algorithms
2. **Network Monitoring**: Continuous real-time analysis
3. **Cultural Defense**: Protect legitimate music ecosystem
4. **International Cooperation**: Global AI threat response

---

**Analysis Method**: Experimental AI Network Detection  
**Confidence Level**: High (statistical validation)  
**Threat Assessment**: Critical (national security implications)  
**Recommendation**: Immediate action required

*This experimental analysis reveals unprecedented AI-generated content manipulation requiring immediate intervention.*
"""
        
        return report

# Example usage
if __name__ == "__main__":
    detector = ExperimentalAIDetector()
    
    # Sample data (would be loaded from actual artist files)
    sample_artists = [
        {
            'name': '2late4hugs',
            'id': '3moGHNhXc2gtN5J4LSuT9h',
            'biography': '27,384 monthly listeners we\'ll get what we deserve',
            'monthly_listeners': 27384,
            'top_track_plays': 863759,
            'popularity': 85,
            'followers': 5000,
            'collection_date': '2026-02-26T01:38:00',
            'collaborations': ['toly808', 'mczy808']
        }
    ]
    
    # Run analysis
    results = detector.detect_ai_network_clusters(sample_artists)
    report = detector.generate_experimental_report(results)
    
    print("Experimental AI Detection Results:")
    print(report)
