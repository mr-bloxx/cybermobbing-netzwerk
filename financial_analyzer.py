#!/usr/bin/env python3
"""
Financial Analysis System for Spotify Artist Profiling
Advanced revenue estimation and financial projection modeling
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import math
from pathlib import Path

@dataclass
class FinancialMetrics:
    """Financial metrics for an artist"""
    monthly_listeners: int
    avg_streams_per_listener: float
    streaming_revenue_per_stream: float  # in EUR
    monthly_revenue: float
    annual_revenue: float
    growth_rate: float
    bot_adjusted_revenue: float  # Revenue after bot detection adjustment

class SpotifyFinancialAnalyzer:
    """Advanced financial analysis for Spotify streaming data"""
    
    def __init__(self):
        # Spotify payout rates (average estimates for German market)
        self.payout_rates = {
            'premium_stream': 0.004,  # 0.4 EUR per stream
            'free_stream': 0.001,     # 0.1 EUR per stream
            'premium_ratio': 0.4,     # 40% premium users in Germany
            'effective_payout': 0.0028 # Weighted average
        }
        
        # Growth assumptions
        self.growth_assumptions = {
            'organic_growth': 0.05,    # 5% monthly organic growth
            'viral_growth': 0.15,      # 15% monthly viral growth
            'decline_rate': 0.02,      # 2% monthly decline for established artists
            'bot_inflation': 0.3       # 30% of streams may be bot-generated
        }
        
        # Market factors
        self.market_factors = {
            'german_market_size': 15_000_000,  # 15M Spotify users in Germany
            'trap_market_share': 0.15,         # 15% market share for trap/rap
            'seasonal_variations': {
                'summer': 1.2,      # 20% increase in summer
                'winter': 0.9,      # 10% decrease in winter
                'spring': 1.1,      # 10% increase in spring
                'fall': 1.0         # baseline in fall
            }
        }
    
    def calculate_monthly_revenue(self, monthly_listeners: int, 
                                streams_per_listener: float = 25.0,
                                bot_adjustment: float = 0.0) -> float:
        """Calculate monthly streaming revenue with bot adjustments"""
        
        # Base stream calculation
        total_streams = monthly_listeners * streams_per_listener
        
        # Apply bot adjustment
        adjusted_streams = total_streams * (1 - bot_adjustment)
        
        # Calculate revenue using effective payout rate
        monthly_revenue = adjusted_streams * self.payout_rates['effective_payout']
        
        return monthly_revenue
    
    def project_revenue_multi_year(self, current_monthly_listeners: int,
                                 growth_scenario: str = 'moderate',
                                 bot_adjustment: float = 0.0,
                                 years: int = 3) -> Dict[str, float]:
        """Project revenue for multiple years with different scenarios"""
        
        growth_rates = {
            'conservative': 0.03,   # 3% monthly growth
            'moderate': 0.05,       # 5% monthly growth
            'aggressive': 0.08,     # 8% monthly growth
            'viral': 0.15           # 15% monthly growth (viral hit scenario)
        }
        
        monthly_growth = growth_rates.get(growth_scenario, 0.05)
        projections = {}
        
        for year in range(1, years + 1):
            # Compound monthly growth
            months = year * 12
            projected_listeners = current_monthly_listeners * ((1 + monthly_growth) ** months)
            
            # Calculate yearly revenue
            yearly_revenue = 0
            for month in range(12):
                month_listeners = current_monthly_listeners * ((1 + monthly_growth) ** (year * 12 + month))
                month_revenue = self.calculate_monthly_revenue(
                    int(month_listeners), 
                    bot_adjustment=bot_adjustment
                )
                yearly_revenue += month_revenue
            
            projections[f'year_{year}_listeners'] = int(projected_listeners)
            projections[f'year_{year}_revenue'] = yearly_revenue
        
        return projections
    
    def analyze_artist_portfolio(self, artists_data: List[Dict]) -> Dict:
        """Analyze entire portfolio of artists"""
        
        total_monthly_revenue = 0
        total_annual_revenue = 0
        artist_breakdown = []
        
        for artist in artists_data:
            monthly_listeners = artist.get('monthly_listeners', 0)
            bot_adjustment = artist.get('bot_probability', 0.0)
            
            # Calculate financial metrics
            monthly_revenue = self.calculate_monthly_revenue(
                monthly_listeners, 
                bot_adjustment=bot_adjustment
            )
            annual_revenue = monthly_revenue * 12
            
            artist_metrics = {
                'name': artist.get('name', 'Unknown'),
                'monthly_listeners': monthly_listeners,
                'monthly_revenue': monthly_revenue,
                'annual_revenue': annual_revenue,
                'bot_adjustment': bot_adjustment,
                'adjusted_revenue': monthly_revenue * (1 - bot_adjustment)
            }
            
            artist_breakdown.append(artist_metrics)
            total_monthly_revenue += monthly_revenue
            total_annual_revenue += annual_revenue
        
        # Sort by revenue
        artist_breakdown.sort(key=lambda x: x['annual_revenue'], reverse=True)
        
        return {
            'portfolio_summary': {
                'total_artists': len(artists_data),
                'total_monthly_revenue': total_monthly_revenue,
                'total_annual_revenue': total_annual_revenue,
                'avg_monthly_revenue_per_artist': total_monthly_revenue / len(artists_data) if artists_data else 0
            },
            'top_artists': artist_breakdown[:10],
            'artist_breakdown': artist_breakdown
        }
    
    def detect_financial_anomalies(self, artist_data: Dict) -> Dict:
        """Detect financial anomalies that might indicate manipulation"""
        
        monthly_listeners = artist_data.get('monthly_listeners', 0)
        followers = artist_data.get('followers', 0)
        top_track_plays = artist_data.get('top_track_plays', 0)
        
        anomalies = []
        
        # 1. Listener-to-follower ratio anomaly
        if followers > 0:
            listener_follower_ratio = monthly_listeners / followers
            if listener_follower_ratio > 100:  # Extremely high ratio
                anomalies.append({
                    'type': 'HIGH_LISTENER_FOLLOWER_RATIO',
                    'value': listener_follower_ratio,
                    'severity': 'HIGH',
                    'description': 'Unusual listener-to-follower ratio suggests bot activity'
                })
        
        # 2. Track play anomaly
        if monthly_listeners > 0 and top_track_plays > 0:
            play_listener_ratio = top_track_plays / monthly_listeners
            if play_listener_ratio > 50:  # Each listener plays top track 50+ times
                anomalies.append({
                    'type': 'EXCESSIVE_TOP_TRACK_PLAYS',
                    'value': play_listener_ratio,
                    'severity': 'MEDIUM',
                    'description': 'Unusually high top track play count'
                })
        
        # 3. Revenue anomaly based on listener count
        estimated_monthly_revenue = self.calculate_monthly_revenue(monthly_listeners)
        if estimated_monthly_revenue > 10000:  # >10K EUR monthly
            anomalies.append({
                'type': 'HIGH_REVENUE_ARTIST',
                'value': estimated_monthly_revenue,
                'severity': 'MEDIUM',
                'description': 'High-revenue artist requiring verification'
            })
        
        return {
            'anomalies': anomalies,
            'risk_score': len([a for a in anomalies if a['severity'] == 'HIGH']) * 3 + 
                         len([a for a in anomalies if a['severity'] == 'MEDIUM']) * 1,
            'bot_probability': min(1.0, len(anomalies) * 0.2)
        }
    
    def generate_financial_report(self, artists_data: List[Dict]) -> str:
        """Generate comprehensive financial report"""
        
        portfolio_analysis = self.analyze_artist_portfolio(artists_data)
        
        # Calculate projections
        total_current_listeners = sum(a.get('monthly_listeners', 0) for a in artists_data)
        
        projections = {
            'conservative': self.project_revenue_multi_year(
                total_current_listeners, 'conservative', years=3
            ),
            'moderate': self.project_revenue_multi_year(
                total_current_listeners, 'moderate', years=3
            ),
            'aggressive': self.project_revenue_multi_year(
                total_current_listeners, 'aggressive', years=3
            )
        }
        
        report = f"""
# Spotify Artist Portfolio Financial Analysis
**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Total Artists**: {len(artists_data)}

## Portfolio Summary
- **Total Monthly Listeners**: {total_current_listeners:,}
- **Estimated Monthly Revenue**: €{portfolio_analysis['portfolio_summary']['total_monthly_revenue']:,.2f}
- **Estimated Annual Revenue**: €{portfolio_analysis['portfolio_summary']['total_annual_revenue']:,.2f}
- **Average Revenue per Artist**: €{portfolio_analysis['portfolio_summary']['avg_monthly_revenue_per_artist']:,.2f}

## Top 10 Artists by Revenue
"""
        
        for i, artist in enumerate(portfolio_analysis['top_artists'], 1):
            report += f"""
{i}. **{artist['name']}**
   - Monthly Listeners: {artist['monthly_listeners']:,}
   - Monthly Revenue: €{artist['monthly_revenue']:,.2f}
   - Annual Revenue: €{artist['annual_revenue']:,.2f}
   - Bot Adjustment: {artist['bot_adjustment']*100:.1f}%
"""
        
        report += """
## 3-Year Revenue Projections

### Conservative Scenario (3% monthly growth)
"""
        for year in range(1, 4):
            listeners = projections['conservative'][f'year_{year}_listeners']
            revenue = projections['conservative'][f'year_{year}_revenue']
            report += f"- Year {year}: {listeners:,} listeners, €{revenue:,.2f} revenue\n"
        
        report += "\n### Moderate Scenario (5% monthly growth)\n"
        for year in range(1, 4):
            listeners = projections['moderate'][f'year_{year}_listeners']
            revenue = projections['moderate'][f'year_{year}_revenue']
            report += f"- Year {year}: {listeners:,} listeners, €{revenue:,.2f} revenue\n"
        
        report += "\n### Aggressive Scenario (8% monthly growth)\n"
        for year in range(1, 4):
            listeners = projections['aggressive'][f'year_{year}_listeners']
            revenue = projections['aggressive'][f'year_{year}_revenue']
            report += f"- Year {year}: {listeners:,} listeners, €{revenue:,.2f} revenue\n"
        
        report += f"""
## Key Financial Insights
- **Current Portfolio Value**: €{portfolio_analysis['portfolio_summary']['total_annual_revenue']:,.2f} annually
- **3-Year Projection (Moderate)**: €{projections['moderate']['year_3_revenue']:,.2f} annually
- **Growth Potential**: {((projections['moderate']['year_3_revenue'] / portfolio_analysis['portfolio_summary']['total_annual_revenue']) - 1) * 100:.1f}% over 3 years

## Risk Assessment
- **Bot Detection Required**: {len([a for a in artists_data if a.get('bot_probability', 0) > 0.3])} artists show bot indicators
- **Financial Anomalies**: Recommend detailed audit of high-revenue artists
- **Market Saturation**: German trap/rap market shows moderate growth potential

---
*Analysis conducted using Spotify Financial Analyzer v1.0*
*Payout rates based on German market averages*
*Bot adjustments applied where detected*
"""
        
        return report

def load_artist_data_from_files(directory: str = ".") -> List[Dict]:
    """Load artist data from markdown files"""
    artists = []
    
    for file_path in Path(directory).glob("*.md"):
        if file_path.name in ["README.md", "BfV_Final_Security_Assessment.md"]:
            continue
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Parse basic information from markdown
            artist = {'name': file_path.stem}
            
            # Extract monthly listeners
            lines = content.split('\n')
            for line in lines:
                if 'Monthly Listeners' in line:
                    try:
                        listeners_str = line.split('(')[1].split(')')[0].replace(',', '')
                        artist['monthly_listeners'] = int(listeners_str)
                        break
                    except:
                        artist['monthly_listeners'] = 0
            
            # Extract followers if available
            for line in lines:
                if 'Followers' in line and 'Not available' not in line:
                    try:
                        followers_str = line.split(':')[1].strip().replace(',', '')
                        artist['followers'] = int(followers_str)
                        break
                    except:
                        artist['followers'] = 0
            
            # Extract top track plays
            for line in lines:
                if 'Plays' in line and '-' in line:
                    try:
                        plays_str = line.split('Plays')[0].split('-')[-1].strip().replace(',', '')
                        artist['top_track_plays'] = int(plays_str)
                        break
                    except:
                        artist['top_track_plays'] = 0
            
            # Default bot probability (would be enhanced with actual bot detection)
            artist['bot_probability'] = 0.0
            
            if artist.get('monthly_listeners', 0) > 0:
                artists.append(artist)
                
        except Exception as e:
            print(f"Error parsing {file_path}: {e}")
    
    return artists

if __name__ == "__main__":
    # Load existing artist data
    artists = load_artist_data_from_files()
    
    # Initialize analyzer
    analyzer = SpotifyFinancialAnalyzer()
    
    # Generate report
    report = analyzer.generate_financial_report(artists)
    
    # Save report
    with open('financial_analysis_report.md', 'w', encoding='utf-8') as f:
        f.write(report)
    
    print("✅ Financial analysis report generated: financial_analysis_report.md")
    print(f"📊 Analyzed {len(artists)} artists")
