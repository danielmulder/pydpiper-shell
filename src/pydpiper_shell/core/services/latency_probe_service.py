# pydpiper_shell/core/services/latency_probe_service.py
"""LatencyProbeService (MVP) — host optional: skip ICMP/TCP/DNS if host is None."""
from __future__ import annotations
import platform
import subprocess
import re
import socket
import time
import statistics
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse
import http.client

# --------------------------- Data models ------------------------------------
@dataclass
class PingResult:
    host: str
    sent: int
    received: int
    loss_pct: float
    times_ms: List[float]
    min_ms: Optional[float]
    avg_ms: Optional[float]
    median_ms: Optional[float]
    p95_ms: Optional[float]
    max_ms: Optional[float]
    jitter_ms: Optional[float]
    ok: bool
    error: Optional[str] = None

@dataclass
class TcpResult:
    host: str
    port: int
    attempts: int
    success: int
    times_ms: List[float]
    min_ms: Optional[float]
    avg_ms: Optional[float]
    median_ms: Optional[float]
    p95_ms: Optional[float]
    max_ms: Optional[float]
    jitter_ms: Optional[float]
    ok: bool
    error: Optional[str] = None

@dataclass
class DnsResult:
    host: str
    ok: bool
    duration_ms: Optional[float]
    addresses: List[str]
    error: Optional[str] = None

@dataclass
class HttpResult:
    url: str
    ok: bool
    ttfb_ms: Optional[float]
    status: Optional[int]
    error: Optional[str] = None

@dataclass
class Verdict:
    level: str  # NORMAL | ELEVATED | HIGH | ERROR
    reasons: List[str]

@dataclass
class TestSummary:
    ping: Optional[PingResult]
    tcp: Optional[TcpResult]
    dns: Optional[DnsResult]
    http: Optional[HttpResult]
    verdict: Verdict

# --------------------------- Utilities --------------------------------------
def quantile(values: Sequence[float], q: float) -> Optional[float]:
    if not values:
        return None
    if q <= 0:
        return float(min(values))
    if q >= 1:
        return float(max(values))
    s = sorted(values)
    idx = (len(s) - 1) * q
    lo = int(idx // 1)
    hi = int(-(-idx // 1))  # ceil
    if lo == hi:
        return float(s[lo])
    frac = idx - lo
    return float(s[lo] * (1 - frac) + s[hi] * frac)

# --------------------------- Probes -----------------------------------------
PING_TIME_RE = re.compile(r"time[=<]([0-9]+(?:\.[0-9]+)?)\s*ms", re.I)
LOSS_LINUX_RE = re.compile(r"(\d+)%\s*packet\s*loss", re.I)
LOSS_WIN_RE = re.compile(r"\((\d+)%\s*loss\)", re.I)
RTT_RE = re.compile(r"(?:min/avg/max/(?:mdev|stddev))\s*=\s*([0-9.]+)/([0-9.]+)/([0-9.]+)/([0-9.]+)\s*ms")
WIN_SUM_RE = re.compile(r"Minimum\s*=\s*(\d+)ms,\s*Maximum\s*=\s*(\d+)ms,\s*Average\s*=\s*(\d+)ms", re.I)

def run_ping(host: str, count: int = 6, timeout: float = 1.0, deadline: Optional[float] = None) -> PingResult:
    system = platform.system().lower()
    cmd: List[str]
    if system.startswith("win"):
        cmd = ["ping", "-n", str(count), "-w", str(int(timeout * 1000)), host]
    elif system == "darwin" or system == "freebsd":
        cmd = ["ping", "-n", "-c", str(count), host]
    else:
        dl = str(int(deadline if deadline is not None else max(count + 1, int(timeout * count + 1))))
        cmd = ["ping", "-n", "-c", str(count), "-w", dl, host]

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except FileNotFoundError:
        return PingResult(host=host, sent=count, received=0, loss_pct=100.0, times_ms=[],
                          min_ms=None, avg_ms=None, median_ms=None, p95_ms=None, max_ms=None,
                          jitter_ms=None, ok=False, error="ping not found")

    out = (proc.stdout or "") + "\n" + (proc.stderr or "")
    times = [float(m) for m in PING_TIME_RE.findall(out)]

    loss_pct: Optional[float] = None
    m = LOSS_LINUX_RE.search(out) or LOSS_WIN_RE.search(out)
    if m:
        loss_pct = float(m.group(1))

    min_ms = avg_ms = max_ms = jitter = None
    m = RTT_RE.search(out)
    if m:
        min_ms = float(m.group(1))
        avg_ms = float(m.group(2))
        max_ms = float(m.group(3))
        jitter = float(m.group(4))
    else:
        m = WIN_SUM_RE.search(out)
        if m:
            min_ms = float(m.group(1))
            max_ms = float(m.group(2))
            avg_ms = float(m.group(3))

    received = len(times)
    if loss_pct is None and count > 0:
        loss_pct = max(0.0, min(100.0, 100.0 * (count - received) / count))

    median_ms = quantile(times, 0.5) if times else None
    p95_ms = quantile(times, 0.95) if times else None
    if jitter is None and len(times) >= 2:
        try:
            jitter = statistics.stdev(times)
        except statistics.StatisticsError:
            jitter = None

    ok = received > 0 and (loss_pct is not None and loss_pct < 100.0)
    return PingResult(
        host=host, sent=count, received=received, loss_pct=float(loss_pct if loss_pct is not None else 100.0),
        times_ms=times, min_ms=min_ms, avg_ms=avg_ms, median_ms=median_ms, p95_ms=p95_ms, max_ms=max_ms,
        jitter_ms=jitter, ok=ok, error=None if ok else ("no replies" if received == 0 else None)
    )

def run_tcp_connect(host: str, port: int, attempts: int = 3, timeout: float = 1.0) -> TcpResult:
    times: List[float] = []
    success = 0
    for _ in range(attempts):
        start = time.perf_counter()
        try:
            with socket.create_connection((host, port), timeout=timeout):
                pass
            elapsed = (time.perf_counter() - start) * 1000.0
            times.append(elapsed)
            success += 1
        except Exception:
            continue
    min_ms = min(times) if times else None
    avg_ms = (sum(times) / len(times)) if times else None
    median_ms = quantile(times, 0.5) if times else None
    p95_ms = quantile(times, 0.95) if times else None
    max_ms = max(times) if times else None
    jitter = statistics.stdev(times) if len(times) >= 2 else None
    ok = success > 0
    return TcpResult(
        host=host, port=port, attempts=attempts, success=success, times_ms=times,
        min_ms=min_ms, avg_ms=avg_ms, median_ms=median_ms, p95_ms=p95_ms, max_ms=max_ms,
        jitter_ms=jitter, ok=ok, error=None if ok else "no successful TCP connect"
    )

def run_dns(host: str, timeout: float = 2.0) -> DnsResult:
    orig = socket.getdefaulttimeout()
    socket.setdefaulttimeout(timeout)
    start = time.perf_counter()
    addrs: List[str] = []
    try:
        infos = socket.getaddrinfo(host, None)
        elapsed = (time.perf_counter() - start) * 1000.0
        for fam, _, _, _, sa in infos:
            if fam in (socket.AF_INET, socket.AF_INET6):
                addrs.append(sa[0])
        seen = set(); uniq = []
        for a in addrs:
            if a not in seen:
                seen.add(a); uniq.append(a)
        addrs = uniq
        return DnsResult(host=host, ok=True, duration_ms=elapsed, addresses=addrs)
    except Exception as e:
        return DnsResult(host=host, ok=False, duration_ms=None, addresses=[], error=str(e))
    finally:
        socket.setdefaulttimeout(orig)

def run_http_head(url: str, timeout: float = 3.0) -> HttpResult:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return HttpResult(url=url, ok=False, ttfb_ms=None, status=None, error="unsupported scheme")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    path = parsed.path or "/"
    if parsed.query:
        path += f"?{parsed.query}"

    conn: http.client.HTTPConnection | http.client.HTTPSConnection
    if parsed.scheme == "https":
        conn = http.client.HTTPSConnection(parsed.hostname, port=port, timeout=timeout)
    else:
        conn = http.client.HTTPConnection(parsed.hostname, port=port, timeout=timeout)

    try:
        start = time.perf_counter()
        conn.request("HEAD", path, headers={
            "Host": parsed.hostname or "",
            "User-Agent": "LatencyProbe/0.1",
            "Accept": "*/*",
            "Connection": "close",
        })
        resp = conn.getresponse()
        ttfb_ms = (time.perf_counter() - start) * 1000.0
        status = int(resp.status)
        try:
            resp.read(0)
        finally:
            conn.close()
        return HttpResult(url=url, ok=True, ttfb_ms=ttfb_ms, status=status)
    except Exception as e:
        try:
            conn.close()
        except Exception:
            pass
        return HttpResult(url=url, ok=False, ttfb_ms=None, status=None, error=str(e))

# --------------------------- Verdicting (MVP) -------------------------------
DEFAULT_THRESHOLDS = {
    "latency_ms": {"elevated": 100.0, "high": 200.0},
    "loss_pct": {"elevated": 2.0, "high": 5.0},
    "tcp_ms": {"elevated": 120.0, "high": 250.0},
    "dns_ms": {"elevated": 60.0, "high": 150.0},
    "http_ttfb_ms": {"elevated": 400.0, "high": 900.0},
}

def _classify(current: Optional[float], thresholds: Dict[str, float]) -> str:
    if current is None:
        return "ERROR"
    if current >= thresholds["high"]:
        return "HIGH"
    if current >= thresholds["elevated"]:
        return "ELEVATED"
    return "NORMAL"

def make_verdict(ping: Optional[PingResult], tcp: Optional[TcpResult], dns: Optional[DnsResult], http: Optional[HttpResult]) -> Verdict:
    reasons: List[str] = []
    order = {"NORMAL": 0, "ELEVATED": 1, "HIGH": 2, "ERROR": 3}
    worst = "NORMAL"

    def worsen(level: str):
        nonlocal worst
        if order[level] > order[worst]:
            worst = level

    if ping:
        if ping.received > 0 and ping.avg_ms is not None:
            lvl = _classify(ping.avg_ms, DEFAULT_THRESHOLDS["latency_ms"])
            worsen(lvl)
            if lvl != "NORMAL":
                reasons.append(f"ICMP avg {ping.avg_ms:.0f}ms")
        else:
            worsen("ERROR")
            reasons.append("ICMP failed")
        if ping.loss_pct is not None:
            loss_lvl = _classify(ping.loss_pct, DEFAULT_THRESHOLDS["loss_pct"])
            worsen(loss_lvl)
            if loss_lvl != "NORMAL":
                reasons.append(f"loss {ping.loss_pct:.1f}%")

    if tcp:
        if tcp.avg_ms is not None:
            lvl = _classify(tcp.avg_ms, DEFAULT_THRESHOLDS["tcp_ms"])
            worsen(lvl)
            if lvl != "NORMAL":
                reasons.append(f"TCP avg {tcp.avg_ms:.0f}ms")
        if tcp.success == 0:
            worsen("ERROR")
            reasons.append("TCP connect failed")

    if dns:
        if dns.ok and dns.duration_ms is not None:
            lvl = _classify(dns.duration_ms, DEFAULT_THRESHOLDS["dns_ms"])
            worsen(lvl)
            if lvl != "NORMAL":
                reasons.append(f"DNS {dns.duration_ms:.0f}ms")
        elif not dns.ok:
            worsen("ERROR")
            reasons.append("DNS failed")

    if http:
        if http.ok and http.ttfb_ms is not None:
            lvl = _classify(http.ttfb_ms, DEFAULT_THRESHOLDS["http_ttfb_ms"])
            worsen(lvl)
            if lvl != "NORMAL":
                reasons.append(f"HTTP TTFB {http.ttfb_ms:.0f}ms")
        elif not http.ok:
            worsen("ERROR")
            reasons.append("HTTP failed")

    if not reasons and worst == "NORMAL":
        reasons.append("all signals normal")

    return Verdict(level=worst, reasons=reasons)

# --------------------------- Service ----------------------------------------
class LatencyProbeService:
    """Single entrypoint for the handler to call. Host may be None (skip some probes)."""

    @staticmethod
    def to_dict(summary: TestSummary) -> Dict[str, Any]:
        def maybe(d):
            if d is None:
                return None
            if hasattr(d, "__dict__"):
                return asdict(d)
            return d
        return {
            "ping": maybe(summary.ping),
            "tcp": maybe(summary.tcp),
            "dns": maybe(summary.dns),
            "http": maybe(summary.http),
            "verdict": asdict(summary.verdict),
        }

    def test(self, host: Optional[str] = None, count: int = 6, timeout: float = 1.0, port: int = 443, http_url: Optional[str] = None) -> Tuple[TestSummary, int]:
        # If host is None, skip ping/tcp/dns probes.
        ping_res = None
        tcp_res = None
        dns_res = None

        if host:
            ping_res = run_ping(host, count=count, timeout=timeout)
            tcp_res = run_tcp_connect(host, port=port, attempts=max(3, count // 2), timeout=timeout)
            dns_res = run_dns(host)

        http_res = run_http_head(http_url) if http_url else None

        verdict = make_verdict(ping_res, tcp_res, dns_res, http_res)
        summary = TestSummary(ping=ping_res, tcp=tcp_res, dns=dns_res, http=http_res, verdict=verdict)

        rc_map = {"NORMAL": 0, "ELEVATED": 1, "HIGH": 2, "ERROR": 3}
        return summary, rc_map.get(verdict.level, 3)