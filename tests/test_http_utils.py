import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import seo_audit.http_utils as http_utils
from seo_audit.http_utils import build_conditional_headers, http_get, http_head, reset_http_session


class _HTTPUtilsHandler(BaseHTTPRequestHandler):
    def do_HEAD(self):  # noqa: N802
        if self.path.startswith("/head"):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("X-Test", "head")
            self.end_headers()
            return
        self.send_response(404)
        self.end_headers()

    def do_GET(self):  # noqa: N802
        if self.path == "/redir":
            self.send_response(302)
            self.send_header("Location", "/final")
            self.end_headers()
            return

        if self.path.startswith("/conditional"):
            if self.headers.get("If-None-Match") == '"abc123"':
                self.send_response(304)
                self.send_header("ETag", '"abc123"')
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("ETag", '"abc123"')
            self.send_header("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
            self.end_headers()
            self.wfile.write(b"<html><body>fresh</body></html>")
            return

        if self.path.startswith("/large-pdf"):
            self.send_response(200)
            self.send_header("Content-Type", "application/pdf")
            self.send_header("Content-Length", "999999")
            self.end_headers()
            self.wfile.write(b"%PDF-1.7")
            return

        if self.path.startswith("/final"):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(self.path.encode("utf-8"))
            return

        if self.path.startswith("/multi-robots"):
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("X-Robots-Tag", "noindex")
            self.send_header("X-Robots-Tag", "googlebot: nofollow")
            self.end_headers()
            self.wfile.write(b"<html><head><title>x</title></head><body>x</body></html>")
            return

        if self.path.startswith("/echo"):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(self.path.encode("utf-8"))
            return

        if self.path.startswith("/missing"):
            self.send_response(404)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"not found")
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, format, *args):  # noqa: A003
        return


def _start_server() -> tuple[HTTPServer, threading.Thread]:
    server = HTTPServer(("127.0.0.1", 0), _HTTPUtilsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def test_http_get_tracks_redirect_chain() -> None:
    server, thread = _start_server()
    try:
        response = http_get(f"http://127.0.0.1:{server.server_port}/redir", timeout=2.0)
        assert response.status_code == 200
        assert response.url.endswith("/final")
        assert len(response.redirect_chain) >= 2
        assert response.redirect_chain[0].endswith("/redir")
        assert response.redirect_chain[-1].endswith("/final")
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_http_get_http_error_returns_response() -> None:
    server, thread = _start_server()
    try:
        response = http_get(f"http://127.0.0.1:{server.server_port}/missing", timeout=2.0)
        assert response.status_code == 404
        assert "not found" in response.text
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_http_get_appends_query_params() -> None:
    server, thread = _start_server()
    try:
        response = http_get(
            f"http://127.0.0.1:{server.server_port}/echo",
            timeout=2.0,
            params={"a": "1", "b": "2"},
        )
        assert response.status_code == 200
        assert "a=1" in response.url
        assert "b=2" in response.url
        assert "a=1" in response.text
        assert "b=2" in response.text
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_http_head_returns_headers_without_body() -> None:
    server, thread = _start_server()
    try:
        response = http_head(f"http://127.0.0.1:{server.server_port}/head", timeout=2.0)
        assert response.status_code == 200
        assert response.headers.get("x-test") == "head"
        assert response.content == b""
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_http_get_skips_large_non_html_body() -> None:
    server, thread = _start_server()
    try:
        response = http_get(
            f"http://127.0.0.1:{server.server_port}/large-pdf",
            timeout=2.0,
            max_bytes=1024,
            max_non_html_bytes=10,
        )
        assert response.status_code == 200
        assert response.content == b""
        assert response.headers.get("x-seo-audit-body-skipped") == "1"
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_http_get_reuses_thread_local_session(monkeypatch) -> None:
    original_create = http_utils._create_session
    calls = {"count": 0}

    def counting_create(*args, **kwargs):
        calls["count"] += 1
        return original_create(*args, **kwargs)

    monkeypatch.setattr(http_utils, "_create_session", counting_create)
    reset_http_session()

    server, thread = _start_server()
    try:
        http_get(f"http://127.0.0.1:{server.server_port}/echo?a=1", timeout=2.0)
        http_get(f"http://127.0.0.1:{server.server_port}/echo?b=2", timeout=2.0)
        assert calls["count"] == 1
    finally:
        server.shutdown()
        thread.join(timeout=2)
        reset_http_session()


def test_http_get_preserves_repeated_headers_in_header_lists() -> None:
    server, thread = _start_server()
    try:
        response = http_get(f"http://127.0.0.1:{server.server_port}/multi-robots", timeout=2.0)
        assert "x-robots-tag" in response.header_lists
        assert len(response.header_lists["x-robots-tag"]) == 2
        assert response.header_lists["x-robots-tag"][0] == "noindex"
        assert response.header_lists["x-robots-tag"][1] == "googlebot: nofollow"
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_build_conditional_headers_sets_etag_and_last_modified() -> None:
    headers = build_conditional_headers(
        {"User-Agent": "TestAgent/1.0"},
        etag='"abc123"',
        last_modified="Wed, 21 Oct 2015 07:28:00 GMT",
    )
    assert headers["User-Agent"] == "TestAgent/1.0"
    assert headers["If-None-Match"] == '"abc123"'
    assert headers["If-Modified-Since"] == "Wed, 21 Oct 2015 07:28:00 GMT"


def test_http_get_reports_not_modified_flag() -> None:
    server, thread = _start_server()
    try:
        warm = http_get(f"http://127.0.0.1:{server.server_port}/conditional", timeout=2.0)
        assert warm.status_code == 200
        assert warm.not_modified is False
        etag = warm.headers.get("etag") or ""

        conditional = http_get(
            f"http://127.0.0.1:{server.server_port}/conditional",
            timeout=2.0,
            headers=build_conditional_headers({}, etag=etag),
        )
        assert conditional.status_code == 304
        assert conditional.not_modified is True
        assert conditional.content == b""
    finally:
        server.shutdown()
        thread.join(timeout=2)
