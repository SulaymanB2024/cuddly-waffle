from pathlib import Path

from seo_audit.issues import build_issues
from seo_audit.policies import LOW_INTERNAL_LINKS_THRESHOLD
from seo_audit.reporting import build_markdown_report
from seo_audit.storage import Storage


def test_low_internal_links_threshold_matches_reporting_summary(tmp_path: Path) -> None:
    db = tmp_path / "audit.sqlite"
    storage = Storage(db)
    storage.init_db()
    run_id = "run1"
    storage.insert_run(run_id, "2026-01-01T00:00:00Z", "https://example.com", {}, "completed")

    storage.conn.execute(
        "INSERT INTO pages (run_id, discovered_url, normalized_url, status_code, content_type, title, h1, internal_links_out) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (run_id, "https://example.com/a", "https://example.com/a", 200, "text/html", "A", "A", LOW_INTERNAL_LINKS_THRESHOLD - 1),
    )
    storage.conn.execute(
        "INSERT INTO pages (run_id, discovered_url, normalized_url, status_code, content_type, title, h1, internal_links_out) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (run_id, "https://example.com/b", "https://example.com/b", 200, "text/html", "B", "B", LOW_INTERNAL_LINKS_THRESHOLD),
    )
    storage.conn.commit()

    page_rows = [dict(r) for r in storage.query("SELECT * FROM pages WHERE run_id = ?", (run_id,))]
    issues = build_issues(run_id, page_rows)
    low_internal_issue_urls = {issue.url for issue in issues if issue.issue_code == "LOW_INTERNAL_LINKS"}
    assert low_internal_issue_urls == {"https://example.com/a"}

    report_path = tmp_path / "report.md"
    build_markdown_report(storage, run_id, report_path)
    report = report_path.read_text(encoding="utf-8")
    assert "- Low internal outlink pages: 1" in report
    storage.close()
