#!/usr/bin/env python3
"""
Advanced AI Artist Detection System
Sophisticated algorithms for detecting AI-generated artists and coordinated bot networks
"""

import numpy as np
import pandas as pd
import re
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import networkx as nx
from collections import defaultdict, Counter
import math

@dataclass
class ArtistData:
    """Data structure for artist analysis"""
    name: str
    monthly_listeners: int
    play_counts: List[int]
    followers: Optional[int]
    genre: str
    release_dates: List[datetime]
    social_media_presence: Dict[str, bool]
    collaboration_network: List[str]
    bio_text: Optional[str]
    spotify_id: str

class AIArtistDetector:
    """Advanced detection system for AI-generated artists and bot networks"""
    
    def __init__(self):
        self.ai_name_patterns = [
            r'^(Yung|Lil|Young)\s+\w+',  # Classic rap prefixes
            r'^\d+\w+',                   # Number + word combinations
            r'\w+\d+$',                   # Word + number endings
            r'^[A-Z]{2,}$',               # All caps abbreviations
            r'.*[øæåØÆÅ].*',             # Special character usage
            r'.*\$.*',                    # Dollar sign usage
            r'^[a-z]+\s+[A-Z]\w+',        # Mixed case patterns
        ]
        
        self.suspicious_genres = [
            'Phonk', 'Hyperpop', 'Experimental', 'Trap', 'Emo Rap'
        ]
        
        self.bot_indicators = {
            'extreme_ratio': 1000,      # plays/listeners ratio
            'high_ratio': 500,           # plays/listeners ratio
            'suspicious_ratio': 100,    # plays/listeners ratio
            'minimal_listeners': 50,     # Very low listener count
            'max_single_track_plays': 10000  # Suspicious single track popularity
        }
    
    def detect_ai_generated_name(self, artist_name: str) -> float:
        """Calculate probability of AI-generated artist name"""
        score = 0.0
        name_lower = artist_name.lower()
        
        # Pattern matching
        for pattern in self.ai_name_patterns:
            if re.match(pattern, artist_name, re.IGNORECASE):
                score += 0.3
        
        # Length analysis
        if len(artist_name) < 4 or len(artist_name) > 20:
            score += 0.2
        
        # Character analysis
        special_chars = sum(1 for c in artist_name if not c.isalnum() and c != ' ')
        if special_chars > 1:
            score += 0.2
        
        # Number analysis
        numbers = sum(1 for c in artist_name if c.isdigit())
        if numbers > 0:
            score += 0.1 * min(numbers, 3)
        
        # Common AI name components
        ai_components = ['yung', 'lil', 'young', 'baby', 'boy', 'girl', 'king', 'queen']
        for component in ai_components:
            if component in name_lower:
                score += 0.15
        
        return min(score, 1.0)
    
    def analyze_listener_play_ratio(self, monthly_listeners: int, play_counts: List[int]) -> Dict:
        """Analyze listener-to-play ratios for bot activity"""
        if not play_counts or monthly_listeners == 0:
            return {'status': 'insufficient_data', 'confidence': 0.0}
        
        total_plays = sum(play_counts)
        avg_plays = total_plays / len(play_counts)
        ratio = avg_plays / monthly_listeners if monthly_listeners > 0 else float('inf')
        
        analysis = {
            'monthly_listeners': monthly_listeners,
            'total_plays': total_plays,
            'avg_plays_per_track': avg_plays,
            'ratio': ratio,
            'confidence': 0.0,
            'indicators': []
        }
        
        # Bot detection thresholds
        if ratio > self.bot_indicators['extreme_ratio']:
            analysis['confidence'] = 0.9
            analysis['indicators'].append('extreme_ratio')
        elif ratio > self.bot_indicators['high_ratio']:
            analysis['confidence'] = 0.7
            analysis['indicators'].append('high_ratio')
        elif ratio > self.bot_indicators['suspicious_ratio']:
            analysis['confidence'] = 0.5
            analysis['indicators'].append('suspicious_ratio')
        
        # Low listener analysis
        if monthly_listeners < self.bot_indicators['minimal_listeners']:
            analysis['confidence'] += 0.3
            analysis['indicators'].append('minimal_listeners')
        
        # Single track dominance
        if len(play_counts) > 1:
            max_plays = max(play_counts)
            second_max = sorted(play_counts, reverse=True)[1] if len(play_counts) > 1 else 0
            if max_plays > self.bot_indicators['max_single_track_plays']:
                analysis['confidence'] += 0.2
                analysis['indicators'].append('single_track_dominance')
        
        analysis['confidence'] = min(analysis['confidence'], 1.0)
        return analysis
    
    def detect_social_media_bots(self, social_presence: Dict[str, bool]) -> float:
        """Detect bot activity in social media presence"""
        score = 0.0
        
        # No social media presence is suspicious
        if not any(social_presence.values()):
            score += 0.4
        
        # Only one platform is slightly suspicious
        if sum(social_presence.values()) == 1:
            score += 0.2
        
        # All platforms present but no engagement (would need API data)
        # This is a placeholder for more sophisticated analysis
        
        return score
    
    def analyze_collaboration_patterns(self, collaborations: List[str], all_artists: List[str]) -> Dict:
        """Analyze collaboration network for bot patterns"""
        collab_count = len(collaborations)
        
        # Check if collaborators are also suspicious
        suspicious_collabs = 0
        for collab in collaborations:
            if collab in all_artists:
                # Would need to check if collaborator is also flagged as AI
                suspicious_collabs += 1
        
        analysis = {
            'collaboration_count': collab_count,
            'suspicious_collaborations': suspicious_collabs,
            'network_density': 0.0,
            'bot_probability': 0.0
        }
        
        # High collaboration count with similar suspicious artists
        if collab_count > 10:
            analysis['bot_probability'] += 0.3
        
        if suspicious_collabs > collab_count * 0.7:
            analysis['bot_probability'] += 0.4
        
        return analysis
    
    def detect_temporal_patterns(self, release_dates: List[datetime]) -> Dict:
        """Detect artificial release timing patterns"""
        if len(release_dates) < 2:
            return {'status': 'insufficient_data', 'bot_probability': 0.0}
        
        # Sort dates
        sorted_dates = sorted(release_dates)
        
        # Calculate intervals
        intervals = []
        for i in range(1, len(sorted_dates)):
            interval = (sorted_dates[i] - sorted_dates[i-1]).days
            intervals.append(interval)
        
        if not intervals:
            return {'status': 'insufficient_data', 'bot_probability': 0.0}
        
        avg_interval = np.mean(intervals)
        std_interval = np.std(intervals)
        
        analysis = {
            'total_releases': len(release_dates),
            'avg_interval_days': avg_interval,
            'interval_consistency': 1.0 - (std_interval / avg_interval) if avg_interval > 0 else 0,
            'bot_probability': 0.0
        }
        
        # Very consistent intervals suggest automation
        if analysis['interval_consistency'] > 0.9 and avg_interval < 30:
            analysis['bot_probability'] += 0.4
        
        # Too frequent releases
        if avg_interval < 7:
            analysis['bot_probability'] += 0.3
        
        return analysis
    
    def comprehensive_analysis(self, artist: ArtistData, all_artist_names: List[str]) -> Dict:
        """Perform comprehensive AI artist detection analysis"""
        
        # Individual analyses
        name_score = self.detect_ai_generated_name(artist.name)
        ratio_analysis = self.analyze_listener_play_ratio(artist.monthly_listeners, artist.play_counts)
        social_score = self.detect_social_media_bots(artist.social_media_presence)
        collab_analysis = self.analyze_collaboration_patterns(artist.collaboration_network, all_artist_names)
        temporal_analysis = self.detect_temporal_patterns(artist.release_dates)
        
        # Calculate overall confidence
        overall_confidence = (
            name_score * 0.2 +
            ratio_analysis.get('confidence', 0) * 0.3 +
            social_score * 0.15 +
            collab_analysis.get('bot_probability', 0) * 0.2 +
            temporal_analysis.get('bot_probability', 0) * 0.15
        )
        
        return {
            'artist_name': artist.name,
            'overall_confidence': overall_confidence,
            'classification': self._classify_confidence(overall_confidence),
            'name_analysis': {'score': name_score, 'ai_generated': name_score > 0.6},
            'engagement_analysis': ratio_analysis,
            'social_analysis': {'score': social_score, 'suspicious': social_score > 0.3},
            'network_analysis': collab_analysis,
            'temporal_analysis': temporal_analysis,
            'recommendations': self._generate_recommendations(overall_confidence)
        }
    
    def _classify_confidence(self, confidence: float) -> str:
        """Classify confidence level"""
        if confidence >= 0.8:
            return "HIGH_CONFIDENCE_AI_ARTIST"
        elif confidence >= 0.6:
            return "MEDIUM_CONFIDENCE_AI_ARTIST"
        elif confidence >= 0.4:
            return "SUSPICIOUS_PATTERN"
        else:
            return "LIKELY_HUMAN"
    
    def _generate_recommendations(self, confidence: float) -> List[str]:
        """Generate investigation recommendations based on confidence"""
        recommendations = []
        
        if confidence >= 0.8:
            recommendations.extend([
                "IMMEDIATE_INVESTIGATION_REQUIRED",
                "FULL_NETWORK_ANALYSIS_NEEDED",
                "CROSS_PLATFORM_MONITORING",
                "FINANCIAL_FLOW_INVESTIGATION"
            ])
        elif confidence >= 0.6:
            recommendations.extend([
                "ENHANCED_MONITORING",
                "COLLABORATION_NETWORK_MAPPING",
                "SOCIAL_MEDIA_BOT_ANALYSIS"
            ])
        elif confidence >= 0.4:
            recommendations.extend([
                "PERIODIC_MONITORING",
                "PATTERN_VERIFICATION"
            ])
        
        return recommendations

class BotNetworkAnalyzer:
    """Advanced bot network detection and analysis"""
    
    def __init__(self):
        self.graph = nx.Graph()
    
    def build_network(self, artists: List[ArtistData]):
        """Build collaboration network"""
        for artist in artists:
            self.graph.add_node(artist.name, **{
                'monthly_listeners': artist.monthly_listeners,
                'genre': artist.genre,
                'spotify_id': artist.spotify_id
            })
            
            for collaborator in artist.collaboration_network:
                self.graph.add_edge(artist.name, collaborator)
    
    def detect_bot_clusters(self, ai_probabilities: Dict[str, float]) -> List[Dict]:
        """Detect coordinated bot clusters in the network"""
        clusters = []
        
        # Find connected components
        components = list(nx.connected_components(self.graph))
        
        for component in components:
            if len(component) < 3:  # Skip small components
                continue
            
            # Calculate average AI probability for component
            avg_ai_prob = np.mean([ai_probabilities.get(artist, 0) for artist in component])
            
            # Network density analysis
            subgraph = self.graph.subgraph(component)
            density = nx.density(subgraph)
            
            if avg_ai_prob > 0.6 and density > 0.3:
                clusters.append({
                    'artists': list(component),
                    'size': len(component),
                    'avg_ai_probability': avg_ai_prob,
                    'network_density': density,
                    'classification': 'COORDINATED_BOT_NETWORK'
                })
        
        return clusters
    
    def analyze_influence_patterns(self) -> Dict:
        """Analyze influence patterns and central nodes"""
        if not self.graph.nodes():
            return {}
        
        centrality_measures = {
            'degree_centrality': nx.degree_centrality(self.graph),
            'betweenness_centrality': nx.betweenness_centrality(self.graph),
            'eigenvector_centrality': nx.eigenvector_centrality(self.graph, max_iter=1000)
        }
        
        # Find most influential nodes
        most_influential = {}
        for measure, values in centrality_measures.items():
            sorted_nodes = sorted(values.items(), key=lambda x: x[1], reverse=True)
            most_influential[measure] = sorted_nodes[:5]
        
        return {
            'network_stats': {
                'total_nodes': self.graph.number_of_nodes(),
                'total_edges': self.graph.number_of_edges(),
                'density': nx.density(self.graph),
                'connected_components': nx.number_connected_components(self.graph)
            },
            'centrality_measures': centrality_measures,
            'most_influential': most_influential
        }

# Example usage and testing
if __name__ == "__main__":
    # Initialize detector
    detector = AIArtistDetector()
    network_analyzer = BotNetworkAnalyzer()
    
    # Example data (would be populated from actual dataset)
    sample_artists = [
        ArtistData(
            name="Yungbro",
            monthly_listeners=7,
            play_counts=[3988],
            followers=None,
            genre="Rap/Trap",
            release_dates=[datetime(2022, 1, 1)],
            social_media_presence={'instagram': False, 'twitter': False, 'website': False},
            collaboration_network=["toe", "Mykel", "Brix Noire"],
            bio_text=None,
            spotify_id="4y3n9JBV2WsXt6AHmz5chQ"
        )
    ]
    
    # Perform analysis
    results = detector.comprehensive_analysis(sample_artists[0], [a.name for a in sample_artists])
    print("Analysis Results:")
    for key, value in results.items():
        print(f"{key}: {value}")
