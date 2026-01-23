# PydPiper Shell

## Project Overview

The **PydPiper Shell** is a professional, interactive command-line interface (CLI) designed for technical SEOs, data engineers, and web scrapers. It orchestrates a complete pipeline: from high-performance asynchronous crawling and data parsing to automated auditing and interactive reporting.

Unlike simple scripts, PydPiper maintains a persistent state, manages multiple projects via a SQLite backend, and offers a bash-like syntax for building complex automation workflows.

**Core Capabilities:**

1. **Crawl:** High-concurrency async crawler with circuit breakers and anti-ban mechanisms.
2. **Parse:** Extract structured data (Images, Links, Metadata) and store efficiently in SQLite.
3. **Audit:** Run 50+ technical SEO checks (broken links, empty alt tags, canonicals, etc.).
4. **Rank:** Calculate Internal PageRank (IPR) to identify link equity distribution.
5. **Report:** Visualize data via a built-in local web server with interactive Site Tree views.

## Key Features

* **Architecture:** Clean modular design using Managers, Facades, and dependency injection.
* **Performance:** Built on `aiohttp` and `uvloop` (on supported systems) for maximum throughput.
* **Resilience:** Handles 429 rate limits automatically (Circuit Breaker pattern).
* **Scriptable:** Supports operators (`&&`, `||`, `|`) and context variables (`@{project.url}`) for automation.
* **JS Support:** Integrated Selenium scraper for JavaScript-heavy websites.
* **Queryable:** Built-in SQL-like interface to query project data directly.

---

## Installation

### Option 1: Docker (Recommended for Linux Compatibility)

The easiest way to run PydPiper in a consistent Linux environment is using Docker.

1. **Build the Image:**
```bash
docker build -t pydpiper-linux .

```


2. **Run the Container:**
To use the interactive shell and ensure the Report Server is accessible from your host machine, use:
```bash
docker run -it -p 5000:5000 pydpiper-linux

```


*Note: If you want to persist your crawl data on your host machine, mount the cache directory:*
`docker run -it -p 5000:5000 -v ${PWD}:/app pydpiper-linux`

### Option 2: Local Installation (Manual)

We provide automated scripts to set up the virtual environment and install dependencies.

1. **Clone the repository:**
```bash
git clone https://github.com/your-org/pydpiper-shell.git
cd pydpiper-shell

```


2. **Run the Setup Script:**
* **Windows:** `install.bat`
* **Linux / macOS:** `chmod +x install.sh && ./install.sh`


3. **Start the shell:**
* **Windows:** `.venv\Scripts\activate && python src\pydpiper_shell\app.py`
* **Linux / macOS:** `source .venv/bin/activate && python3 src/pydpiper_shell/app.py`



---

## Running the Report Server in Docker

When using the `audit report` command inside a Docker container, the server must be bound to `0.0.0.0` to be reachable from your host browser.

1. Run the audit command in the PydPiper shell:
```bash
PydPiper>> audit report

```


2. Open your browser on your host machine (Windows/macOS) and navigate to:
`http://localhost:5000`

---

## The PydPiper Workflow

A typical session involves creating a project, running a crawl pipeline, and inspecting the results.

### Example Pipeline

```bash
# Create and load a project
project create https://example.com

# Run the full chain: Crawl -> Plugins -> Parse -> Audit -> Rank -> Report
crawler run && plugin run crawl_report && parse run && audit run --workers 4 && audit rank && audit report

```