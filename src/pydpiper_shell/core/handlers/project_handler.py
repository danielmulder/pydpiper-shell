# src/pydpiper_shell/core/handlers/project_handler.py
import logging
import argparse
import json
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse

from pydpiper_shell.core.context.shell_context import ShellContext
from pydpiper_shell.core.managers.project_manager import ProjectManager
from pydpiper_shell.core.services.project_info_service import ProjectInfoService

logger = logging.getLogger(__name__)

# --- HELP TEXT AND HIERARCHY ---
project_help_text = """
PROJECT MANAGEMENT:
  project create <url> [--mode <mode>] [--name <name>]
                      Creates a new project and loads it into the context.
  project load <id>   Loads an existing project into the context.
  project list        Shows a list of all available projects.
  project status      Shows the details of the currently loaded project.
  project delete <id> Permanently deletes a project and all associated data.
  project reload      Rescans the cache directory and reloads all projects.
  project info <id> [--pages total|--pages avg_size]
                      Provides statistical insights into project data.
""".strip()

COMMAND_HIERARCHY: Dict[str, Optional[Dict[str, Any]]] = {
    "create": None, "load": None, "list": None,
    "status": None, "info": None, "delete": None, "reload": None,
}


# -------------------------------

def _handle_create(args: argparse.Namespace, ctx: ShellContext) -> int:
    # --- LOGICA VOOR NAAM GENERATIE ---
    name = args.name
    if not name:
        try:
            parsed = urlparse(args.url)
            name = parsed.netloc or parsed.path
            if name.startswith("www."):
                name = name[4:]
        except Exception:
            name = "unnamed_project"

    project = ctx.project_manager.create_project(
        name=name,
        start_url=args.url,
        mode=args.mode
    )

    if not project:
        print(f"❌ Error: Could not create project for URL '{args.url}'. Check logs.")
        return 1

    # --- FIX: Gebruik methodes op context, niet op manager ---
    ctx.set_project(project)
    ctx.export_project_variables(project)
    # ---------------------------------------------------------

    print(
        f"✅ Project '{project.name}' created with ID {project.id} "
        f"and loaded into context."
    )
    return 0


def _handle_load(args: argparse.Namespace, ctx: ShellContext) -> int:
    project = ctx.project_manager.get_project_by_id(args.id)

    if not project:
        print(f"❌ Error: Project with ID {args.id} not found.")
        return 1

    # --- FIX: Gebruik methodes op context ---
    ctx.set_project(project)
    ctx.export_project_variables(project)
    # ----------------------------------------

    print(f"✅ Project {args.id} ('{project.name}') loaded into context.")
    return 0


def _handle_list(_args: argparse.Namespace, ctx: ShellContext) -> int:
    projects = ctx.project_manager.get_all_projects()
    if not projects:
        print("No projects found.")
        return 0

    print(f"\n{'ID':<5} | {'Name':<30} | {'URL':<40} | {'Mode':<10}")
    print("-" * 90)

    for p in sorted(projects, key=lambda prj: prj.id):
        active_marker = "*" if str(ctx.get("project.id")) == str(p.id) else " "
        print(f"{active_marker}{p.id:<4} | {p.name:<30} | {str(p.start_url):<40} | {p.run_mode:<10}")
    print()
    return 0


def _handle_status(_args: argparse.Namespace, ctx: ShellContext) -> int:
    project_id_str = ctx.get("project.id")
    if not project_id_str:
        print("No project is currently loaded.")
        return 1
    try:
        project_id = int(project_id_str)
        project = ctx.project_manager.get_project_by_id(project_id)
        if not project:
            print(f"Error: Active project {project_id_str} not found in manager.")
            return 1

        print(f"--- Status for Active Project: {project.name} (ID: {project.id}) ---")
        for k, v in project.model_dump(mode='json').items():
            print(f"  {k:<20} = {v}")
        return 0
    except (ValueError, TypeError):
        print(f"Error: Invalid project ID '{project_id_str}' in context.")
        return 1


def _handle_delete(args: argparse.Namespace, ctx: ShellContext) -> int:
    project_id_to_delete = args.id
    current_active_id = ctx.get("project.id")

    if ctx.project_manager.delete_project(project_id_to_delete):
        print(f"✅ Project {project_id_to_delete} deleted.")
        if current_active_id == str(project_id_to_delete):
            # Clear context if active project deleted
            # We doen dit handmatig omdat ctx.set_project(None) misschien niet alles wist
            ctx._vars = {
                k: v for k, v in ctx._vars.items()
                if not k.startswith('project.')
            }
            ctx.current_project = None
            print("   Active project context has been cleared.")
        return 0
    else:
        print(f"❌ Failed to delete project {project_id_to_delete}. It may not exist or an error occurred.")
        return 1


def _handle_info(args: argparse.Namespace, ctx: ShellContext) -> int:
    project_id = args.id
    if not ctx.project_manager.get_project_by_id(project_id):
        print(f"Error: Project with ID {project_id} not found.")
        return 1

    try:
        # ctx.db_manager is de naam in app.py
        info_service = ProjectInfoService(project_id, ctx.db_manager)
    except Exception as e:
        logger.error(f"Failed to initialize ProjectInfoService for project {project_id}: {e}", exc_info=True)
        print(f"Error initializing info service: {e}")
        return 1

    try:
        if not args.info_type:
            stats = info_service.get_all_stats()
            print(json.dumps(stats, indent=2))
        elif args.info_type == "total_pages":
            print(f"Total pages: {info_service.get_total_pages()}")
        elif args.info_type == "avg_size":
            avg_kb = info_service.get_avg_page_size('KB')
            print(f"Average page size: {avg_kb} KB")
        return 0
    except Exception as e:
        logger.error(f"Error retrieving project info for {project_id}: {e}", exc_info=True)
        print(f"Error getting project statistics: {e}")
        return 1


def _handle_reload(_args: argparse.Namespace, ctx: ShellContext) -> int:
    print("Rescanning disk and reloading projects from databases...")
    try:
        # Reload door nieuwe manager te maken
        ctx.project_manager = ProjectManager()
        count = len(ctx.project_manager.get_all_projects())
        print(f"✅ Reload complete. Found {count} projects.")

        active_id_str = ctx.get("project.id")
        if active_id_str:
            try:
                active_id = int(active_id_str)
                if not ctx.project_manager.get_project_by_id(active_id):
                    ctx._vars = {
                        k: v for k, v in ctx._vars.items() if not k.startswith('project.')
                    }
                    ctx.current_project = None
                    print(f"⚠️ Active project {active_id} not found after reload. Context cleared.")
            except (ValueError, TypeError):
                pass

        _handle_list(_args, ctx)
        return 0
    except Exception as e:
        logger.error(f"Error during project reload: {e}", exc_info=True)
        print(f"Error reloading projects: {e}")
        return 1


# --- Main Handler Function ---

def handle_project(args: List[str], ctx: ShellContext, _stdin: Optional[str] = None) -> int:
    if not hasattr(ctx, "project_manager") or ctx.project_manager is None:
        try:
            ctx.project_manager = ProjectManager()
            logger.info("ProjectManager initialized successfully.")
        except Exception as e:
            logger.critical(f"Failed to initialize ProjectManager: {e}", exc_info=True)
            print(f"FATAL ERROR: Could not initialize Project Manager. Check logs.")
            return 1

    parser = argparse.ArgumentParser(prog="project", description="Manage projects.")
    subparsers = parser.add_subparsers(dest="subcommand", help="Sub-command help")

    parsers = {
        "create": subparsers.add_parser("create", help="Create a new project."),
        "load": subparsers.add_parser("load", help="Load a project by ID."),
        "list": subparsers.add_parser("list", help="List all projects."),
        "status": subparsers.add_parser("status", help="Show active project status."),
        "delete": subparsers.add_parser("delete", help="Delete a project by ID."),
        "info": subparsers.add_parser("info", help="Get project statistics by ID."),
        "reload": subparsers.add_parser("reload", help="Rescan cache and reload projects."),
    }

    parsers["create"].add_argument("url", type=str, help="The starting URL.")
    parsers["create"].add_argument("--mode", default="discovery", help="Crawl mode.")
    parsers["create"].add_argument("--name", help="Optional name for the project.")

    parsers["load"].add_argument("id", type=int, help="Project ID.")
    parsers["delete"].add_argument("id", type=int, help="Project ID.")
    parsers["info"].add_argument("id", type=int, help="Project ID.")
    parsers["info"].add_argument("info_type", nargs='?', choices=['total_pages', 'avg_size'],
                                 help="Specific statistic.")

    parsers["create"].set_defaults(func=_handle_create)
    parsers["load"].set_defaults(func=_handle_load)
    parsers["list"].set_defaults(func=_handle_list)
    parsers["status"].set_defaults(func=_handle_status)
    parsers["delete"].set_defaults(func=_handle_delete)
    parsers["info"].set_defaults(func=_handle_info)
    parsers["reload"].set_defaults(func=_handle_reload)

    try:
        if not args:
            parser.print_help()
            return 0

        parsed_args = parser.parse_args(args)

        if hasattr(parsed_args, 'func'):
            return parsed_args.func(parsed_args, ctx)
        else:
            parser.print_help()
            return 1

    except argparse.ArgumentError as e:
        print(f"Argument Error: {e}")
        return 1
    except SystemExit:
        return 1
    except Exception as e:
        logger.error(f"Error executing project command: {e}", exc_info=True)
        print(f"An unexpected error occurred: {e}")
        return 1