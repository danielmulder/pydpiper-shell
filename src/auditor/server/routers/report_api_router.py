import logging
from flask import Blueprint, jsonify, request, send_file, current_app

from pydpiper_shell.core.managers.project_manager import ProjectManager
from pydpiper_shell.core.managers.report_manager import ReportManager

# Configure logger for API visibility
logger = logging.getLogger('werkzeug')
logger.setLevel(logging.DEBUG)

report_api_router = Blueprint('report_api_router', __name__)


# --- HELPER FUNCTION ---

def get_report_controller():
    """Retrieves the report controller from the Flask application context."""
    controller = current_app.config.get('REPORT_CONTROLLER')
    if not controller:
        raise RuntimeError("ReportController is not set in app.config['REPORT_CONTROLLER']")
    return controller


# --- API ROUTES ---

@report_api_router.route('/report', methods=['GET'])
def get_report():
    """
    Generic endpoint to fetch the latest report for a project.
    Uses ProjectManager to ensure correct database context.
    """
    try:
        # 1. Retrieve the active Project ID
        project_id = current_app.config.get('PROJECT_ID')
        if not project_id:
            return jsonify({"error": "No Project ID configured"}), 500

        # 2. Extract Query Parameters
        name = request.args.get('name')
        lib = request.args.get('lib')
        category = request.args.get('cat') or request.args.get('category')

        if not (name or (lib and category)):
            return jsonify({"error": "Missing required parameters"}), 400

        # 3. Use ProjectManager to fetch the specific project instance
        pm = ProjectManager()
        project = pm.get_project_by_id(project_id)
        if not project:
            return jsonify({"error": f"Project {project_id} not found"}), 404

        # 4. Use the ProjectManager's DB connection for the ReportManager
        report_manager = ReportManager(pm.db_manager)

        report = report_manager.get_latest_report(
            project_id=project_id,
            lib=lib,
            category=category,
            name=name
        )

        if not report:
            return jsonify({"message": "Report not found", "data": None}), 404

        return jsonify(report)

    except Exception as e:
        logger.error(f"Error fetching report: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500





@report_api_router.route('/issues/tree')
def get_issue_tree():
    """API endpoint for the grouped Issue Tree structure."""
    try:
        project_id = current_app.config.get('PROJECT_ID')
        if not project_id:
            return jsonify({"error": "No Project ID"}), 500

        pm = ProjectManager()
        report_manager = ReportManager(pm.db_manager)

        data = report_manager.get_issue_tree_structure(project_id)
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error getting issue tree: {e}")
        return jsonify({"error": str(e)}), 500


@report_api_router.route('/issues/urls')
def get_issue_urls():
    """API endpoint for the Issue Drilldown (affected URLs)."""
    try:
        project_id = current_app.config.get('PROJECT_ID')
        cat = request.args.get('cat')
        code = request.args.get('code')

        if not project_id:
            return jsonify({"error": "No Project ID"}), 500

        pm = ProjectManager()
        report_manager = ReportManager(pm.db_manager)

        data = report_manager.get_urls_for_issue(project_id, cat, code)
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error getting issue URLs: {e}")
        return jsonify({"error": str(e)}), 500


# --- LEGACY CONTROLLER ROUTES ---

@report_api_router.route('/tree')
def get_tree():
    """Fetches the hierarchical site structure tree."""
    try:
        data = get_report_controller().generate_report_data()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@report_api_router.route('/page/<int:page_id>')
def get_page_details(page_id):
    """Fetches specific audit and content details for a single page ID."""
    try:
        details = get_report_controller().get_page_details(page_id)
        return jsonify(details)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@report_api_router.route('/export')
def export_excel():
    """Generates and returns the Excel audit report file."""
    try:
        path = get_report_controller().create_excel_export()
        return send_file(path, as_attachment=True)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@report_api_router.route('/config', methods=['GET', 'POST'])
def config_route():
    """Retrieves or saves UI configuration, such as hidden/ignored issue codes."""
    try:
        controller = get_report_controller()
        if request.method == 'POST':
            json_data = request.json
            controller.save_config(json_data.get('hidden', []))
            return jsonify({"status": "success"})
        else:
            data = controller.get_config_data()
            return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500