# src/pydpiper_shell/controllers/services/net_info_service.py
import records
import requests
import time
import logging
import json

logger = logging.getLogger(__name__)

def _print_json(jsn):
    return print(json.dumps(json, indent=2, ensure_ascii=False))

def check_connection(timeout: float = 5.0):
    """
    Check if we have internet by calling httpbin.org/ip.
    Returns (ok: bool, info: dict)
    """
    url = "https://postman-echo.com/ip"
    start = time.perf_counter()
    try:
        resp = requests.get(url, timeout=timeout)
        elapsed = (time.perf_counter() - start) * 1000
        resp.raise_for_status()
        data = resp.json()

        ip = json.dumps(data.get("ip"), indent=2, ensure_ascii=False)
        #print(f'Request public IP: {ip.strip("\"")}')
        print(f'Request public IP: {ip}')

        return True, {
            "status": "ok",
            "origin": data.get("origin"),
            "endpoint": url,
            "response_time_ms": round(elapsed, 2),
        }
    except Exception as e:
        elapsed = (time.perf_counter() - start) * 1000
        logger.warning("System connection check failed: %s", e)
        return False, {
            "status": "error",
            "endpoint": url,
            "error": str(e),
            "response_time_ms": round(elapsed, 2),
        }


def check_host(target: str, timeout: float = 5.0):
    """
    Check if a specific host is reachable.
    Returns (ok: bool, info: dict)
    """
    start = time.perf_counter()
    try:
        resp = requests.get(target, timeout=timeout)
        elapsed = (time.perf_counter() - start) * 1000
        resp.raise_for_status()
        return True, {
            "status": "ok",
            "target": target,
            "http_status": resp.status_code,
            "response_time_ms": round(elapsed, 2),
        }
    except Exception as e:
        elapsed = (time.perf_counter() - start) * 1000
        logger.warning("Host connection check failed for %s: %s", target, e)
        return False, {
            "status": "error",
            "target": target,
            "error": str(e),
            "response_time_ms": round(elapsed, 2),
        }
