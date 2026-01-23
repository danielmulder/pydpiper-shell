"""
PydPiper Studio - Report Server
Main entry point for the Flask-based SEO reporting interface.
"""

import argparse
import os
import sys
import logging
from flask import Flask

# --- PATH CONFIGURATION ---
# Ensure the 'src' directory is in the PYTHONPATH for absolute imports
current_dir = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.abspath(os.path.join(current_dir, '../../..'))

if src_path not in sys.path:
    sys.path.insert(0, src_path)

# --- INTERNAL IMPORTS ---
from auditor.server.routers.report_api_router import report_api_router
from auditor.server.routers.page_router import page_router
from auditor.controllers.report_controller import ReportController
from pydpiper_shell.core.managers.database_manager import DatabaseManager
from auditor.managers.audit_data_manager import AuditorDataManager

# Configure logging for the Flask server
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_app(project_id: int) -> Flask:
    """
    Application factory to initialize the Flask instance with required controllers.
    """
    flask_app = Flask(__name__)

    # 1. Initialize Data Layers
    db_manager = DatabaseManager()
    audit_data_manager = AuditorDataManager(db_manager)

    # 2. Initialize Controller with Project Context
    report_controller = ReportController(project_id, audit_data_manager)

    # 3. Inject Controller into App Config for Blueprint access
    flask_app.config['REPORT_CONTROLLER'] = report_controller
    flask_app.config['PROJECT_ID'] = project_id

    # 4. Register Blueprints
    flask_app.register_blueprint(report_api_router, url_prefix='/api')
    flask_app.register_blueprint(page_router)

    return flask_app


def main():
    """
    Main execution block to parse arguments and start the server.
    """
    parser = argparse.ArgumentParser(description="PydPiper Studio Report Server")
    parser.add_argument("--project-id", type=int, required=True, help="ID of the project to serve")
    parser.add_argument("--db-path", type=str, required=False, help="Explicit path to SQLite database")
    parser.add_argument("--port", type=int, default=5000, help="Port to bind the server to")

    # Binding to 0.0.0.0 is essential for Docker container accessibility from the Host
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Host interface to bind to (use 0.0.0.0 for Docker/External access)"
    )

    args = parser.parse_args()

    # Initialize the application
    app = create_app(args.project_id)

    # Informative PEP-style startup message
    print("\n" + "=" * 50)
    print(f"🚀  PYDPIPER STUDIO | Project: {args.project_id}")
    print("=" * 50)
    print(f"📡  Internal Container IP: http://{args.host}:{args.port}")
    print(f"🌍  Host Browser Access:   http://localhost:{args.port}")
    print("-" * 50)

    print("\n🔍 API ROUTE MAPPING:")
    for rule in app.url_map.iter_rules():
        if "api" in str(rule):
            print(f"   ✅ {rule}")
    print("-" * 50 + "\n")

    # Start the Flask development server
    # use_reloader=False prevents double-initialization in detached shell environments
    app.run(
        debug=True,
        host=args.host,
        port=args.port,
        use_reloader=False
    )


if __name__ == '__main__':
    main()