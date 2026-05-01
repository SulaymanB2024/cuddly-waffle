import argparse
import csv
import json
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from seo_audit.cli import run_audit


def _server_base_url(server: object) -> str:
    address = getattr(server, "server_address", None)
    host = str(address[0]) if isinstance(address, tuple) and len(address) >= 1 else "127.0.0.1"
    port = int(address[1]) if isinstance(address, tuple) and len(address) >= 2 else 80
    return f"http://{host}:{port}"


class MiniSiteHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        base = _server_base_url(self.server)
        if self.path == "/robots.txt":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(f"User-agent: *\nDisallow: /private\nSitemap: {base}/sitemap.xml\n".encode())
            return

        if self.path == "/sitemap.xml":
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.end_headers()
            self.wfile.write(
                f"""
                <sitemapindex xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'>
                  <sitemap><loc>{base}/sitemap-pages.xml</loc></sitemap>
                </sitemapindex>
                """.encode()
            )
            return

        if self.path == "/sitemap-pages.xml":
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.end_headers()
            self.wfile.write(
                f"""
                <urlset xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'>
                  <url><loc>{base}/</loc></url>
                  <url><loc>{base}/service</loc></url>
                  <url><loc>{base}/contact</loc></url>
                  <url><loc>{base}/thin</loc></url>
                  <url><loc>{base}/dup</loc></url>
                                    <url><loc>{base}/private</loc></url>
                </urlset>
                """.encode()
            )
            return

        if self.path == "/old":
            self.send_response(301)
            self.send_header("Location", "/service")
            self.end_headers()
            return

        pages = {
            "/": "<html><head><title>Home</title><meta name='description' content='Home desc'></head><body><h1>Welcome</h1><a href='/service'>Service</a><a href='/contact'>Contact</a><a href='/old'>Old</a><a href='/thin'>Thin</a><a href='/dup'>Dup</a></body></html>",
            "/service": f"<html><head><title>Dup Title</title><meta name='description' content='Dup Desc'><link rel='canonical' href='{base}/service-canonical'></head><body><h1>Service</h1><p>{'word ' * 200}</p></body></html>",
            "/dup": "<html><head><title>Dup Title</title><meta name='description' content='Dup Desc'></head><body><h1>Duplicate</h1></body></html>",
            "/contact": "<html><head><title>Contact Us</title><meta name='description' content='Call us'></head><body><h1>Contact</h1><script type='application/ld+json'>{\"@type\":\"Organization\"}</script><p>City State Phone</p></body></html>",
            "/thin": "<html><head><title>Thin</title><meta name='description' content='x'><meta name='robots' content='noindex'></head><body><h1>Thin</h1>tiny</body></html>",
        }
        if self.path in pages:
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(pages[self.path].encode())
            return

        self.send_response(404)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"not found")

    def log_message(self, format, *args):  # noqa: A003
        return


def test_pipeline_integration_local_site(tmp_path: Path, monkeypatch) -> None:
    server = HTTPServer(("127.0.0.1", 0), MiniSiteHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    out_dir = tmp_path / "out"
    domain = f"http://127.0.0.1:{server.server_port}"

    monkeypatch.setattr("seo_audit.cli.collect_performance", lambda *args, **kwargs: ([], []))

    args = argparse.Namespace(
        domain=domain,
        output=str(out_dir),
        max_pages=20,
        max_render_pages=5,
        render_mode="none",
        timeout=2.0,
        user_agent="TestAgent/1.0",
        ignore_robots=False,
        i_understand_robots_bypass=False,
        save_html=False,
        verbose=False,
        performance_targets=1,
        psi_enabled=False,
        crux_enabled=False,
        crux_origin_fallback=True,
        store_provider_payloads=False,
        payload_retention_days=30,
        provider_max_retries=0,
        provider_base_backoff_seconds=0.1,
        provider_max_backoff_seconds=0.5,
        provider_respect_retry_after=True,
        provider_max_total_wait_seconds=1.0,
    )

    try:
        run_audit(args)
    finally:
        server.shutdown()
        thread.join(timeout=2)

    db = sqlite3.connect(out_dir / "audit.sqlite")
    run_id = db.execute("SELECT run_id FROM runs ORDER BY rowid DESC LIMIT 1").fetchone()[0]

    pages = db.execute(
        "SELECT normalized_url, final_url, status_code, content_type, redirect_chain_json, duplicate_title_flag, duplicate_description_flag, content_hash, effective_field_provenance_json FROM pages WHERE run_id = ?",
        (run_id,),
    ).fetchall()
    assert len(pages) >= 5
    assert any(row[2] == 200 for row in pages)
    assert any(len(json.loads(row[4])) > 1 for row in pages if row[4])
    assert any(row[5] == 1 for row in pages)
    assert any(row[6] == 1 for row in pages)
    assert any(row[7] for row in pages if row[2] == 200 and "html" in (row[3] or "").lower())
    assert all(row[8] for row in pages)
    for row in pages:
        provenance = json.loads(row[8] or "{}")
        assert provenance.get("title")
        assert provenance.get("meta_description")
        assert provenance.get("canonical")
        assert provenance.get("hreflang")
        assert provenance.get("content_hash")

    link_rows = db.execute("SELECT source_context, dom_region FROM links WHERE run_id = ?", (run_id,)).fetchall()
    assert link_rows
    assert all(row[0] in {"raw_dom", "render_dom"} for row in link_rows)
    assert all((row[1] or "unknown") in {"main", "nav", "header", "footer", "aside", "unknown"} for row in link_rows)

    issue_codes = {row[0] for row in db.execute("SELECT issue_code FROM issues WHERE run_id = ?", (run_id,)).fetchall()}
    assert "NOINDEX" in issue_codes
    assert "CANONICAL_MISMATCH" in issue_codes
    assert "THIN_CONTENT" in issue_codes
    assert "SITEMAP_URL_BLOCKED_BY_ROBOTS" in issue_codes

    score_rows = db.execute("SELECT overall_score FROM scores WHERE run_id = ?", (run_id,)).fetchall()
    assert score_rows

    graph_rows = db.execute(
        "SELECT url, internal_pagerank, betweenness, closeness, community_id, bridge_flag FROM page_graph_metrics WHERE run_id = ?",
        (run_id,),
    ).fetchall()
    assert graph_rows
    assert all(row[1] is not None for row in graph_rows)

    for csv_name in ["pages.csv", "issues.csv", "scores.csv", "page_graph_metrics.csv", "report.md"]:
        assert (out_dir / csv_name).exists()

    with (out_dir / "pages.csv").open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
        assert any(r["final_url"].endswith("/service") for r in rows)
