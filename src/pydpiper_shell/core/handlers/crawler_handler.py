# src/pydpiper_shell/core/handlers/crawler_handler.py
import argparse
import logging
import json
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse

from crawler.controllers.async_crawl_controller import AsyncCrawlController
from crawler.model import CrawlSettings
from pydpiper_shell.core.context.shell_context import ShellContext
from pydpiper_shell.core.filter_registry import FilterRegistry
from pydpiper_shell.core.loop_runner import run_on_main_loop
from pydpiper_shell.core.managers.config_manager import config_manager
from pydpiper_shell.model import Project
from crawler.services.async_page_fetcher_service import PageFetcherService
from crawler.services.generate_default_user_agent_service import generate_default_user_agent
from crawler.utils.url_utils import UrlUtils

logger = logging.getLogger(__name__)

# --- HELP TEXT ---

crawler_help_text = """
  crawler run [<target>] [--max-pages <n>] [--sanitize] [--page-filter <name>]
                      Executes a crawl. <target> can be a Project ID or a URL.
                      If no target is provided, the active project is used.

  crawler test request
                      Performs a diagnostic HTTP request to verify headers 
                      and connectivity.
""".strip()


# --- TASK HELPERS ---

async def _run_crawl_task(
        project: Project, args: argparse.Namespace, ctx: ShellContext
) -> AsyncCrawlController:
    """
    Async task that orchestrates the crawl execution via the AsyncCrawlController.
    """

    # 1. Configuration & Settings
    concurrency = int(config_manager.get_nested("crawler.default_concurrency", 25))
    respect_robots = config_manager.get_nested("robots_txt.enabled", False)

    settings = CrawlSettings(
        max_pages=args.max_pages,
        concurrency=concurrency,
        sanitize=args.sanitize
    )

    # 2. Database Cleanup
    logger.debug(f"Clearing previous crawl data for project {project.id}...")

    tables_to_clear = [
        "pages",
        "links",
        "requests",
        "page_elements",
        "plugin_page_metrics",
        "images",
        "audit_issues"
    ]

    ctx.db_manager.clear_tables(project.id, tables_to_clear)

    # 3. Initialize Controller
    crawler_config = config_manager.get_nested("crawler", {})
    session_config = config_manager.get_nested("session", {})

    crawler_config["session"] = session_config

    controller = AsyncCrawlController(
        project_id=project.id,
        start_url=str(project.start_url),
        run_mode=project.run_mode,
        db_manager=ctx.db_manager,
        config=crawler_config,
        strict_mode=getattr(ctx, "strict_mode", True),
        respect_robots_txt=respect_robots,
        page_filter_name=args.page_filter,
    )

    # 4. Execute Crawl
    try:
        await controller.run(settings)
    finally:
        await controller.shutdown()

    return controller


async def _perform_header_test() -> Optional[Dict[str, Any]]:
    """
    Performs a test request to an external echo service to verify header configuration.
    """
    target_url = "https://postman-echo.com/headers"
    logger.info(f"Performing header test request to: {target_url}")

    fetcher_config = config_manager.get_nested("crawler", {})
    session_config = config_manager.get_nested("session", {})
    fetcher_config["session"] = session_config

    user_agent = generate_default_user_agent()
    url_utils = UrlUtils()

    fetcher_service = PageFetcherService(
        config=fetcher_config, url_utils=url_utils, user_agent=user_agent
    )

    try:
        await fetcher_service.initialize()
        if not fetcher_service.session:
            return None
        async with fetcher_service.session.get(target_url) as response:
            response.raise_for_status()
            return await response.json()
    finally:
        await fetcher_service.close()


# --- HANDLERS ---

def _handle_run(args: argparse.Namespace, ctx: ShellContext) -> int:
    """Handles the 'crawler run' command logic."""
    project = None

    # 1. Determine Project
    if args.project:
        # Explicit ID provided
        project = ctx.project_manager.get_project_by_id(int(args.project))
        if not project:
            print(f"❌ Error: Project with ID {args.project} not found.")
            return 1
    elif args.target:
        # Target can be an ID or a URL
        target = args.target
        if target.isdigit():
            project = ctx.project_manager.get_project_by_id(int(target))
            if not project:
                print(f"❌ Error: Project with ID {target} not found.")
                return 1
        elif target.startswith(("http://", "https://")):
            # Create a new project dynamically
            name = "unnamed_project"
            try:
                name = urlparse(target).netloc
            except Exception:
                pass

            project = ctx.project_manager.create_project(name=name, start_url=target)
            if not project:
                print(f"❌ Error: Could not create project for {target}.")
                return 1
            print(f"✅ Created temporary project '{name}' (ID: {project.id}).")
        else:
            print(f"❌ Error: Invalid target '{target}'.")
            return 1
    else:
        # Fallback to the active project in context
        project = ctx.current_project
        if not project:
            print("❌ Error: No active project found. Use 'project create' or provide a target.")
            return 1

    # 2. Update Context
    ctx.current_project = project
    ctx.export_project_variables(project)

    print(f"🚀 Starting crawl for project '{project.name}' (ID: {project.id})...")

    # 3. Execute Crawl (Async on Main Loop)
    try:
        controller = run_on_main_loop(_run_crawl_task(project, args, ctx))

        # 4. Update Statistics post-crawl
        if controller:
            project.total_time = round(controller.timer.duration, 2)
            project.pages = controller.pages_crawled

            # Persist metadata to DB
            ctx.project_manager.save_project_metadata(project)

            # Calculate pages per second safely
            pps = round(project.pages / project.total_time, 2) if project.total_time > 0 else 0

            print(f"✅ Crawl finished in {project.total_time}s.")
            print(f"   Pages:     {project.pages}")
            print(f"   Pages/sec: {pps}")
            print(f"   Failures:  {controller.request_failures}")

        return 0
    except KeyboardInterrupt:
        print("\n🛑 Crawl interrupted by user.")
        return 1
    except Exception as e:
        logger.error(f"Crawl failed: {e}", exc_info=True)
        print(f"❌ Crawl failed: {e}")
        return 1


def _handle_test_request(args: argparse.Namespace, ctx: ShellContext) -> int:
    """Handles the 'crawler test request' command."""
    try:
        result = run_on_main_loop(_perform_header_test())
        if result:
            print("\n--- Received Headers ---")
            print(json.dumps(result, indent=2))
            return 0
        else:
            print("❌ Header test failed.")
            return 1
    except Exception as e:
        print(f"❌ Error during test: {e}")
        return 1


def handle_crawler(args: List[str], ctx: ShellContext, _stdin: Optional[str] = None) -> int:
    """
    Main entry point for crawler commands.
    Parses arguments and dispatches to specific sub-handlers.
    """
    parser = argparse.ArgumentParser(prog="crawler", description="Manage crawls.", add_help=False)
    subparsers = parser.add_subparsers(dest="subcommand")

    # Command: crawler run
    run_parser = subparsers.add_parser("run", add_help=False)
    run_parser.add_argument("target", nargs="?")
    run_parser.add_argument("--project", type=int)
    run_parser.add_argument("--max-pages", type=int)
    run_parser.add_argument("--sanitize", action="store_true")
    run_parser.add_argument("--page-filter", choices=list(FilterRegistry.keys()))
    run_parser.set_defaults(func=_handle_run)

    # Command: crawler test
    test_parser = subparsers.add_parser("test", add_help=False)
    test_subs = test_parser.add_subparsers(dest="test_type")
    req_parser = test_subs.add_parser("request", add_help=False)
    req_parser.set_defaults(func=_handle_test_request)

    if not args or args[0] in ["help", "-h", "--help"]:
        print(crawler_help_text)
        return 0

    try:
        parsed_args = parser.parse_args(args)
        if hasattr(parsed_args, "func"):
            return parsed_args.func(parsed_args, ctx)
        else:
            print(crawler_help_text)
            return 1
    except SystemExit:
        # Argparse calls sys.exit() on error or help; catch it and show custom help
        print(crawler_help_text)
        return 1
    except Exception as e:
        logger.error(f"Crawler command failed: {e}", exc_info=True)
        return 1