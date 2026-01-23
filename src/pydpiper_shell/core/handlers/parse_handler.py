# ============================================
# file: src/pydpiper_shell/core/handlers/parse_handler.py
# ============================================
from __future__ import annotations

import argparse
import logging
import os
from typing import List, Optional

from pydpiper_shell.core.context.shell_context import ShellContext
from parser.controllers.parse_controller import ParseController
from pydpiper_shell.core.managers.config_manager import config_manager  # <— ADD

logger = logging.getLogger(__name__)

parse_help_text = """
  parse run [<id>] [--all|--elements <...>] [--workers <N>] [--no-img]
      Parses crawled pages for a project and stores results in 'page_elements'.
      Images are parsed by default unless --no-img is provided.
""".strip()
COMMAND_HIERARCHY = {"run": None}


def handle_parse(args: List[str], ctx: ShellContext, _stdin: Optional[str] = None) -> int:
    parser = argparse.ArgumentParser(prog="parse", description="Parse crawled pages from DB.")
    subs = parser.add_subparsers(dest="subcommand", help="Sub-command help")

    p_run = subs.add_parser("run", help="Run the parser on a project.")
    p_run.add_argument("project_id_pos", metavar="PROJECT_ID", type=int, nargs="?", default=None,
                       help="Optional: Project ID (defaults to active).")
    p_run.add_argument("--workers", type=int, default=os.cpu_count() or 4,
                       help=f"Number of parallel processes (default: {os.cpu_count() or 4}).")
    g = p_run.add_mutually_exclusive_group(required=False)
    g.add_argument("--all", action="store_true", help="Parse all elements (default).")
    g.add_argument("--elements", type=str, help="Comma-separated list (e.g., page_title,headings).")
    p_run.add_argument("--no-img", action="store_true", help="Skip parsing <img> data.")  # <— ADD

    if not args:
        parser.print_help()
        return 0

    try:
        pargs = parser.parse_args(args)
        if pargs.subcommand != "run":
            print(f"Unknown command: {pargs.subcommand}")
            parser.print_help()
            return 1
    except SystemExit:
        return 1
    except argparse.ArgumentError as e:
        print(f"Argument Error: {e}")
        return 1

    # resolve project id
    if pargs.project_id_pos is not None:
        project_id = int(pargs.project_id_pos)
    else:
        pid_str = ctx.get("project.id")
        if not pid_str:
            print("❌ Error: No active project.")
            return 1
        try:
            project_id = int(pid_str)
        except (ValueError, TypeError):
            print(f"❌ Error: Invalid active project ID '{pid_str}'.")
            return 1

    if not getattr(ctx, "project_manager", None) or not ctx.project_manager.get_project_by_id(project_id):
        print(f"❌ Error: Project {project_id} not found.")
        return 1

    elements = None
    if pargs.elements:
        elements = [e.strip().lower() for e in pargs.elements.split(",")]
    elif pargs.all or not pargs.elements:
        elements = None  # None == parse alle beschikbare elementen in workers

    # include_images: CLI > config > default(True)
    cfg_parse_img = config_manager.get_nested("parser.parse_img", True)
    include_images = False if pargs.no_img else bool(cfg_parse_img)  # <— ADD

    controller = ParseController(default_workers=pargs.workers)

    try:
        stats = controller.parse_project(
            project_id=project_id,
            ctx=ctx,
            elements=elements,
            workers=pargs.workers,
            show_progress=True,
            include_images=include_images,   # <— ADD
        )
    except Exception as e:
        logger.error("Parse failed: %s", e, exc_info=True)
        print(f"❌ Parse error: {e}")
        return 1

    print(
        f"✅ Parsed project {stats['project_id']}: {stats['pages_success']}/{stats['pages_total']} pages, "
        f"saved {stats['elements_saved']} elements, {stats['images_saved']} images "
        f"in {stats['duration_s']}s ({stats['pages_per_s']} p/s)."
    )
    return 0
