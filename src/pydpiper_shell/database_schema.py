# src/pydpiper_shell/database_schema.py

DEFAULT_SCHEMA_SCRIPT = """
CREATE TABLE IF NOT EXISTS project (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    start_url TEXT NOT NULL UNIQUE,
    run_mode TEXT DEFAULT 'discovery',
    sitemap_url TEXT,
    total_time REAL,
    pages INTEGER DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pages (
    id INTEGER PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    status_code INTEGER,
    content_type TEXT,   -- <--- NIEUW: Opslag voor MIME-type (bijv. 'application/pdf')
    crawled_at TEXT NOT NULL,
    content TEXT,
    ipr REAL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_pages_url ON pages(url);

CREATE TABLE IF NOT EXISTS links (
    id INTEGER PRIMARY KEY,
    project_id INTEGER NOT NULL,
    source_url TEXT NOT NULL,
    target_url TEXT NOT NULL,
    anchor TEXT,
    rel TEXT,
    is_external INTEGER DEFAULT 0,
    status_code INTEGER
);
CREATE INDEX IF NOT EXISTS idx_links_source ON links(source_url);
CREATE INDEX IF NOT EXISTS idx_links_target ON links(target_url);

CREATE TABLE IF NOT EXISTS requests (
    id INTEGER PRIMARY KEY,
    project_id INTEGER NOT NULL,
    url TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    headers TEXT,
    elapsed_time REAL,
    timers TEXT,
    redirect_chain TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_requests_status ON requests(status_code);

CREATE TABLE IF NOT EXISTS page_elements (
    id INTEGER PRIMARY KEY,
    project_id INTEGER NOT NULL,
    page_id INTEGER NOT NULL,
    element_type TEXT NOT NULL,
    content TEXT,
    UNIQUE (project_id, page_id, element_type),
    FOREIGN KEY (page_id) REFERENCES pages (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS images (
    id INTEGER PRIMARY KEY,
    project_id INTEGER NOT NULL,
    page_id INTEGER NOT NULL,
    image_url TEXT,
    alt_text TEXT,
    width INTEGER,
    height INTEGER,
    FOREIGN KEY (page_id) REFERENCES pages (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS plugin_page_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    page_id INTEGER NOT NULL,
    url TEXT,
    title_length INTEGER,
    h1_length INTEGER,
    meta_desc_length INTEGER,
    total_images INTEGER,
    missing_alt_tags INTEGER,
    missing_alt_ratio REAL,
    internal_link_count INTEGER,
    external_link_count INTEGER,
    incoming_link_count INTEGER,
    has_canonical INTEGER,
    word_count INTEGER,
    server_time REAL,
    broken_img_ratio REAL,
    UNIQUE (project_id, page_id),
    FOREIGN KEY (project_id) REFERENCES project (id) ON DELETE CASCADE,
    FOREIGN KEY (page_id) REFERENCES pages (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS audit_issues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    page_id INTEGER NOT NULL,
    url TEXT NOT NULL,
    category TEXT NOT NULL,
    element_type TEXT, 
    issue_code TEXT,    
    severity TEXT,
    message TEXT,
    details TEXT,       
    created_at TEXT NOT NULL,
    FOREIGN KEY (page_id) REFERENCES pages (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER,
    lib TEXT,         -- e.g., 'auditor', 'seo-module'
    category TEXT,    -- e.g., 'audit_summary', 'performance'
    name TEXT,        -- e.g., 'initial_scan', 're-scan_december'
    data JSON,        -- The flexible payload
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
"""