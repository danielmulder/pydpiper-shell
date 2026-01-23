import logging
from flask import Blueprint, render_template, current_app

from pydpiper_shell.core.managers.project_manager import ProjectManager
from pydpiper_shell.core.managers.report_manager import ReportManager

logger = logging.getLogger(__name__)

# Blueprint for handling HTML page rendering
page_router = Blueprint('page_router', __name__)


@page_router.route('/')
def index():
    """
    Renders the main dashboard for a specific project.
    Fetches project metadata and the latest audit summary.
    """
    # 1. Retrieve the active Project ID from the Flask configuration
    project_id = current_app.config.get('PROJECT_ID')

    if not project_id:
        logger.error("Page router accessed without a PROJECT_ID in config.")
        return "Error: No project ID configured.", 500

    # 2. Use ProjectManager to fetch metadata (name, base URL, etc.)
    pm = ProjectManager()
    project = pm.get_project_by_id(project_id)

    if not project:
        logger.error(f"Project metadata for ID {project_id} not found.")
        return f"Error: Project {project_id} could not be loaded.", 404

    # 3. Retrieve the latest Audit Report
    report_manager = ReportManager()
    audit_report = report_manager.get_latest_report(
        project_id=project_id,
        name="scan_result",
        category="audit_summary"
    )

    # 4. Render the dashboard template with the gathered data
    return render_template(
        'index.html',
        project=project,
        audit_report=audit_report
    )





@page_router.route('/issues')
def issues_view():
    """
    Renders the issue overview page.
    The actual issue data is usually fetched asynchronously via the API router.
    """
    project_id = current_app.config.get('PROJECT_ID')
    return render_template('issues.html', project_id=project_id)