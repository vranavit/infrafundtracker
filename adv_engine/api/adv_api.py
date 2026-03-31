"""
ADV Buying Signal Engine Flask REST API
Provides endpoints for accessing leads, signals, and analytics
"""

import logging
import json
from datetime import datetime, timedelta
from io import StringIO
import csv

from flask import Flask, jsonify, request, send_file
from flask_cors import CORS

from models import Tier, FirmRecord, Platform
from scorer import FirmScorer

logger = logging.getLogger(__name__)


# In-memory database for testing/demo purposes
class SimpleDatabase:
    """Simple in-memory database"""
    def __init__(self):
        self.firms = {}
        self.signals = []
    
    def upsert_firm(self, firm):
        self.firms[firm.sec_file_number] = firm
    
    def get_firm(self, sec_file_number):
        return self.firms.get(sec_file_number)
    
    def get_all_firms(self):
        return list(self.firms.values())
    
    def get_firms_by_tier(self, tier):
        return [f for f in self.firms.values() if f.tier == tier]
    
    def get_filtered_leads(self, tier=None, state=None, min_aum=None, max_aum=None, limit=100, offset=0):
        results = list(self.firms.values())
        if tier:
            results = [f for f in results if f.tier.value == tier]
        if state:
            results = [f for f in results if f.state == state]
        if min_aum:
            results = [f for f in results if f.aum >= min_aum]
        if max_aum:
            results = [f for f in results if f.aum <= max_aum]
        total = len(results)
        return sorted(results, key=lambda x: x.score, reverse=True)[offset:offset+limit], total
    
    def add_signal(self, signal):
        self.signals.append(signal)
    
    def get_recent_signals(self, days=7):
        return self.signals
    
    def get_tier_stats(self):
        stats = {}
        for firm in self.firms.values():
            tier_name = firm.tier.name
            stats[tier_name] = stats.get(tier_name, 0) + 1
        return stats
    
    def get_platform_stats(self):
        stats = {}
        for firm in self.firms.values():
            platform = firm.platform.value
            stats[platform] = stats.get(platform, 0) + 1
        return stats
    
    def get_geography_stats(self):
        stats = {}
        for firm in self.firms.values():
            state = firm.state
            if state:
                stats[state] = stats.get(state, 0) + 1
        return stats


def create_app(db_path: str = None, database: SimpleDatabase = None):
    """Create and configure Flask application"""
    
    app = Flask(__name__)
    CORS(app)
    
    # Initialize database
    db = database or SimpleDatabase()
    
    # ============================================================================
    # GET /api/adv/daily-brief
    # ============================================================================
    @app.route('/api/adv/daily-brief', methods=['GET'])
    def get_daily_brief():
        """Get today's full daily brief"""
        try:
            today = datetime.now()
            
            tier1 = db.get_firms_by_tier(Tier.TIER_1)
            tier2 = db.get_firms_by_tier(Tier.TIER_2)
            signals = db.get_recent_signals(days=1)
            
            tier_stats = db.get_tier_stats()
            platform_stats = db.get_platform_stats()
            
            brief = {
                'date': today.isoformat(),
                'summary': {
                    'tier1_count': len(tier1),
                    'tier2_count': len(tier2),
                    'new_signals_count': len(signals),
                    'total_leads': tier_stats,
                    'platform_distribution': platform_stats,
                },
                'tier1_leads': [lead.to_dict() for lead in tier1[:10]],
                'tier2_leads': [lead.to_dict() for lead in tier2[:10]],
                'new_signals': [sig.to_dict() if hasattr(sig, 'to_dict') else sig.__dict__ for sig in signals[:5]],
            }
            
            return jsonify(brief), 200
            
        except Exception as e:
            logger.error(f"Error generating daily brief: {e}")
            return jsonify({'error': str(e)}), 500

    # ============================================================================
    # GET /api/adv/leads - Filtered leads with pagination
    # ============================================================================
    @app.route('/api/adv/leads', methods=['GET'])
    def get_leads():
        """Get filtered leads"""
        try:
            tier = request.args.get('tier', type=int)
            state = request.args.get('state', type=str)
            min_aum = request.args.get('min_aum', type=int)
            max_aum = request.args.get('max_aum', type=int)
            limit = request.args.get('limit', default=50, type=int)
            offset = request.args.get('offset', default=0, type=int)
            
            limit = min(limit, 500)
            offset = max(0, offset)
            
            leads, total = db.get_filtered_leads(
                tier=tier,
                state=state,
                min_aum=min_aum,
                max_aum=max_aum,
                limit=limit,
                offset=offset
            )
            
            return jsonify({
                'leads': [lead.to_dict() for lead in leads],
                'pagination': {
                    'total': total,
                    'limit': limit,
                    'offset': offset,
                    'pages': (total + limit - 1) // limit
                }
            }), 200
            
        except Exception as e:
            logger.error(f"Error retrieving leads: {e}")
            return jsonify({'error': str(e)}), 500

    # ============================================================================
    # GET /api/adv/lead/<sec_file_number> - Single lead card
    # ============================================================================
    @app.route('/api/adv/lead/<sec_file_number>', methods=['GET'])
    def get_lead(sec_file_number):
        """Get full lead card for a specific firm"""
        try:
            lead = db.get_firm(sec_file_number)
            if not lead:
                return jsonify({'error': 'Lead not found'}), 404
            
            return jsonify(lead.to_dict()), 200
            
        except Exception as e:
            logger.error(f"Error retrieving lead {sec_file_number}: {e}")
            return jsonify({'error': str(e)}), 500

    # ============================================================================
    # GET /api/adv/signals/new - Recent signals
    # ============================================================================
    @app.route('/api/adv/signals/new', methods=['GET'])
    def get_new_signals():
        """Get new signals in last N days"""
        try:
            days = request.args.get('days', default=7, type=int)
            days = min(days, 90)
            
            signals = db.get_recent_signals(days=days)
            
            return jsonify({
                'days': days,
                'count': len(signals),
                'signals': [sig.to_dict() if hasattr(sig, 'to_dict') else sig.__dict__ for sig in signals]
            }), 200
            
        except Exception as e:
            logger.error(f"Error retrieving signals: {e}")
            return jsonify({'error': str(e)}), 500

    # ============================================================================
    # GET /api/adv/platform-summary - Leads by platform tier
    # ============================================================================
    @app.route('/api/adv/platform-summary', methods=['GET'])
    def get_platform_summary():
        """Get lead distribution by platform"""
        try:
            platform_stats = db.get_platform_stats()
            
            return jsonify({
                'platform_distribution': platform_stats,
                'total_leads': sum(platform_stats.values())
            }), 200
            
        except Exception as e:
            logger.error(f"Error retrieving platform summary: {e}")
            return jsonify({'error': str(e)}), 500

    # ============================================================================
    # GET /api/adv/geography-heat - Lead counts by state
    # ============================================================================
    @app.route('/api/adv/geography-heat', methods=['GET'])
    def get_geography_heat():
        """Get lead distribution by geography"""
        try:
            geo_stats = db.get_geography_stats()
            
            return jsonify({
                'by_state': geo_stats,
                'total_states': len(geo_stats),
                'total_leads': sum(geo_stats.values())
            }), 200
            
        except Exception as e:
            logger.error(f"Error retrieving geography heat: {e}")
            return jsonify({'error': str(e)}), 500

    # ============================================================================
    # GET /api/adv/stats - Overall statistics
    # ============================================================================
    @app.route('/api/adv/stats', methods=['GET'])
    def get_stats():
        """Get overall statistics"""
        try:
            all_firms = db.get_all_firms()
            tier_stats = db.get_tier_stats()
            
            total_aum = sum(firm.aum for firm in all_firms)
            avg_score = sum(firm.score for firm in all_firms) / len(all_firms) if all_firms else 0
            
            stats = {
                'total_leads': len(all_firms),
                'by_tier': tier_stats,
                'coverage': {
                    'total_aum': total_aum,
                    'avg_score': round(avg_score, 2),
                    'avg_num_advisors': round(sum(firm.num_advisors for firm in all_firms) / len(all_firms), 1) if all_firms else 0,
                },
                'run_status': {
                    'status': 'ready',
                    'last_run': None
                }
            }
            
            return jsonify(stats), 200
            
        except Exception as e:
            logger.error(f"Error retrieving stats: {e}")
            return jsonify({'error': str(e)}), 500

    # ============================================================================
    # POST /api/adv/refresh - Trigger immediate data refresh
    # ============================================================================
    @app.route('/api/adv/refresh', methods=['POST'])
    def trigger_refresh():
        """Trigger immediate data refresh (async)"""
        try:
            return jsonify({
                'message': 'Data refresh initiated',
                'run_id': 1,
                'status': 'running'
            }), 202
            
        except Exception as e:
            logger.error(f"Error triggering refresh: {e}")
            return jsonify({'error': str(e)}), 500

    # ============================================================================
    # GET /api/adv/export - Export data as CSV
    # ============================================================================
    @app.route('/api/adv/export', methods=['GET'])
    def export_data():
        """Export leads as CSV"""
        try:
            format_type = request.args.get('format', default='csv', type=str)
            tier = request.args.get('tier', type=int)
            state = request.args.get('state', type=str)
            
            if format_type not in ['csv', 'json']:
                return jsonify({'error': 'Invalid format. Use csv or json.'}), 400
            
            leads, _ = db.get_filtered_leads(tier=tier, state=state, limit=10000)
            
            if format_type == 'json':
                return jsonify({
                    'leads': [lead.to_dict() for lead in leads]
                }), 200
            
            # CSV format
            output = StringIO()
            if leads:
                writer = csv.DictWriter(output, fieldnames=leads[0].to_dict().keys())
                writer.writeheader()
                for lead in leads:
                    writer.writerow(lead.to_dict())
            
            output.seek(0)
            return send_file(
                StringIO(output.getvalue()),
                mimetype='text/csv',
                as_attachment=True,
                download_name=f'adv_leads_export_{datetime.now().strftime("%Y%m%d")}.csv'
            ), 200
            
        except Exception as e:
            logger.error(f"Error exporting data: {e}")
            return jsonify({'error': str(e)}), 500

    # ============================================================================
    # Error handlers
    # ============================================================================
    @app.errorhandler(404)
    def not_found(error):
        return jsonify({'error': 'Not found'}), 404

    @app.errorhandler(500)
    def internal_error(error):
        return jsonify({'error': 'Internal server error'}), 500

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True, port=5000)
