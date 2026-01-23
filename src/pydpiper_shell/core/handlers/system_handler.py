# src/pydpiper_shell/core/handlers/system_handler.py
import json
import logging
import argparse
import asyncio
import time
import aiohttp
import sys
import os
import csv
from collections import deque
from itertools import cycle
from urllib.parse import urlparse
from typing import List, Optional, Dict, Any

# --- SERVICES IMPORTS ---
from pydpiper_shell.core.services.sys_info_service import ram_info, hd_info
from pydpiper_shell.core.services.net_info_service import check_connection, check_host
from pydpiper_shell.core.services.json_service import to_json
from pydpiper_shell.core.services.latency_probe_service import LatencyProbeService, TestSummary

# --- CORE IMPORTS ---
from pydpiper_shell.core.core import expand_context_vars
from pydpiper_shell.core.context.shell_context import ShellContext

logger = logging.getLogger(__name__)

system_help_text = """
SYSTEM & MAINTENANCE:
  system ram_info        Shows information about the RAM memory.
  system hd_info         Shows information about the hard drive partitions.
  system net_info ...    Checks the general internet connection or a specific host.
  system probe <url>     Deep latency diagnostics (ICMP, TCP, DNS, HTTP).
  system benchmark <trg> Raw stress test on a URL or a CSV file list.
""".strip()

COMMAND_HIERARCHY: Dict[str, Optional[Dict[str, Any]]] = {
    "ram_info": None,
    "hd_info": None,
    "net_info": None,
    "probe": None,
    "benchmark": None,
}


class NoExitArgumentParser(argparse.ArgumentParser):
    """Argument parser that raises exceptions instead of exiting the shell."""
    def error(self, message):
        print(f"❌ Argument Error: {message}")
        raise ValueError(message)

    def exit(self, status=0, message=None):
        if message:
            print(message)
        raise ValueError("Help displayed")


# --- HELPER: PROBE PRINTING ---
def _print_probe_summary(summary: TestSummary):
    """Pretty prints the results of a latency probe."""
    verdict = summary.verdict
    print(f"\n--- Latency Probe Verdict ---")
    print(f"Level: {verdict.level}")
    print(f"Reasons: {', '.join(verdict.reasons)}")
    print("-" * 29)

    if summary.ping:
        p = summary.ping
        print(f"\n[ICMP Ping]")
        if p.ok:
            print(f"  Avg: {p.avg_ms:.2f}ms | Min: {p.min_ms:.2f}ms | Max: {p.max_ms:.2f}ms")
            if p.jitter_ms is not None:
                print(f"  Loss: {p.loss_pct:.1f}% | Jitter: {p.jitter_ms:.2f}ms")
            else:
                print(f"  Loss: {p.loss_pct:.1f}%")
        else:
            print(f"  Status: FAILED ({p.error or 'No replies'})")

    if summary.tcp:
        t = summary.tcp
        print(f"\n[TCP Connect - Port {t.port}]")
        if t.ok:
            print(f"  Avg: {t.avg_ms:.2f}ms | Min: {t.min_ms:.2f}ms | Max: {t.max_ms:.2f}ms")
            print(f"  Success: {t.success}/{t.attempts}")
        else:
            print(f"  Status: FAILED ({t.error or 'Connection failed'})")

    if summary.dns:
        d = summary.dns
        print(f"\n[DNS Lookup]")
        if d.ok:
            print(f"  Duration: {d.duration_ms:.2f}ms")
            print(f"  Addresses: {', '.join(d.addresses)}")
        else:
            print(f"  Status: FAILED ({d.error or 'Lookup failed'})")

    if summary.http:
        h = summary.http
        print(f"\n[HTTP HEAD Request]")
        if h.ok:
            print(f"  TTFB: {h.ttfb_ms:.2f}ms | Status Code: {h.status}")
        else:
            print(f"  Status: FAILED ({h.error or 'Request failed'})")
    print()


# --- HELPER: BENCHMARK WORKERS ---
async def _benchmark_worker(session: aiohttp.ClientSession, queue: deque, stats: Dict):
    """Async worker that consumes URLs from the queue and performs requests."""
    while True:
        try:
            url = queue.popleft()
            # Optional: Uncomment below for live feedback (noisy for large lists)
            # print(f"⚡ [Worker] Hitting: {url}")
        except IndexError:
            break

        start = time.time()
        try:
            async with session.get(url, ssl=False) as response:
                await response.read()
                stats['success'] += 1
                stats['bytes'] += len(await response.read())
        except Exception:
            stats['error'] += 1
        finally:
            stats['times'].append(time.time() - start)


async def _run_async_benchmark(urls: List[str], total_requests: int, concurrency: int):
    """
    Runs the asynchronous benchmark.
    Handles both single-target mode and list-rotation mode (CSV).
    """
    queue = deque()

    if len(urls) == 1:
        # Optimization for single URL
        target_url = urls[0]
        queue.extend([target_url] * total_requests)
        print(f"   Mode:        Single Target ({target_url})")
    else:
        # CSV Mode: Cycle through the list to fill the request quota
        url_cycler = cycle(urls)
        for _ in range(total_requests):
            queue.append(next(url_cycler))
        print(f"   Mode:        List Rotation ({len(urls)} unique URLs loaded)")

    stats = {'success': 0, 'error': 0, 'times': [], 'bytes': 0}
    start_global = time.time()

    connector = aiohttp.TCPConnector(limit=concurrency, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [_benchmark_worker(session, queue, stats) for _ in range(concurrency)]
        await asyncio.gather(*tasks)

    duration = time.time() - start_global

    avg_latency = (sum(stats['times']) / len(stats['times']) * 1000) if stats['times'] else 0
    req_per_sec = stats['success'] / duration if duration > 0 else 0
    data_mb = stats['bytes'] / 1024 / 1024

    print("\n📊 BENCHMARK RESULTS")
    print("=" * 60)
    print(f"   Total Requests: {total_requests}")
    print(f"   Total Time:     {duration:.2f}s")
    print(f"   Throughput:     {req_per_sec:.2f} req/s")
    print(f"   Avg Latency:    {avg_latency:.0f} ms")
    print(f"   Data Transfer:  {data_mb:.2f} MB")
    print(f"   Success/Errors: {stats['success']} / {stats['error']}")
    print("=" * 60 + "\n")


# --- MAIN HANDLER ---
def handle_system(args: List[str], ctx: ShellContext, _stdin=None) -> int:
    """Main entry point for 'system' commands."""
    if not args:
        print("Usage: system <ram_info|hd_info|net_info|probe|benchmark ...>")
        return 1

    sub = args[0]

    # --- RAM INFO ---
    if sub == "ram_info":
        try:
            print(to_json(ram_info()))
            return 0
        except Exception as e:
            print(to_json({"status": "error", "error": str(e)}))
            return 1

    # --- DISK INFO ---
    if sub == "hd_info":
        try:
            print(to_json(hd_info()))
            return 0
        except Exception as e:
            print(to_json({"status": "error", "error": str(e)}))
            return 1

    # --- NETWORK INFO ---
    if sub == "net_info":
        if len(args) < 2:
            print("Usage: system net_info <check_con|check_host <url>>")
            return 1
        action = args[1]
        if action == "check_con":
            ok, info = check_connection()
            print(to_json(info))
            return 0 if ok else 1
        elif action == "check_host":
            if len(args) < 3:
                return 1
            url = expand_context_vars(args[2], ctx)
            ok, info = check_host(url)
            print(to_json(info))
            return 0 if ok else 1
        return 1

    # --- LATENCY PROBE ---
    if sub == "probe":
        if len(args) < 2:
            return 1
        url_to_probe = expand_context_vars(args[1], ctx)
        try:
            parsed = urlparse(url_to_probe)
            host = parsed.hostname
            if not host:
                # Handle cases like "google.com" without scheme
                if "." in url_to_probe and "/" not in url_to_probe:
                    host = url_to_probe
                    url_to_probe = "http://" + host
                else:
                    raise ValueError("Invalid hostname")

            print(f"Running latency probe for {host}...")
            svc = LatencyProbeService()
            summary, ec = svc.test(host=host, http_url=url_to_probe)
            _print_probe_summary(summary)
            return ec
        except Exception as e:
            print(f"Probe error: {e}")
            return 1

    # --- BENCHMARK ---
    if sub == "benchmark":
        parser = NoExitArgumentParser(prog="system benchmark", add_help=True)
        parser.add_argument("target", help="Target URL OR path to CSV file")
        parser.add_argument("--concurrency", "-c", type=int, default=100)
        parser.add_argument("--requests", "-n", type=int, default=500)

        try:
            parsed = parser.parse_args(args[1:])
        except ValueError:
            return 1
        except SystemExit:
            return 1

        target_input = expand_context_vars(parsed.target, ctx)
        urls_to_bench = []

        # 1. Check if target is a file (CSV)
        if os.path.isfile(target_input):
            print(f"📂 Loading targets from file: {target_input}")
            try:
                with open(target_input, 'r', encoding='utf-8') as f:
                    # Attempt to read as CSV with 'urls' header
                    reader = csv.DictReader(f)
                    if reader.fieldnames and 'urls' in reader.fieldnames:
                        urls_to_bench = [row['urls'].strip() for row in reader if row['urls'].strip()]
                    else:
                        # Fallback: Read as simple list (1 url per line)
                        f.seek(0)
                        urls_to_bench = [line.strip() for line in f if line.strip()]
            except Exception as e:
                print(f"❌ Error reading file: {e}")
                return 1

            if not urls_to_bench:
                print("❌ File seems empty or could not parse URLs.")
                return 1
            print(f"   Loaded {len(urls_to_bench)} unique URLs.")

        # 2. Otherwise treat as a single URL
        else:
            if not target_input.startswith("http"):
                target_input = "https://" + target_input
            urls_to_bench = [target_input]

        print(f"🚀 Starting RAW Benchmark (c={parsed.concurrency}, n={parsed.requests})")

        try:
            if hasattr(asyncio, 'WindowsSelectorEventLoopPolicy'):
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

            # Run the async benchmark with the list of URLs
            asyncio.run(_run_async_benchmark(urls_to_bench, parsed.requests, parsed.concurrency))
            return 0
        except Exception as e:
            print(f"Benchmark failed: {e}")
            return 1

    print("Unknown system command:", sub)
    return 1