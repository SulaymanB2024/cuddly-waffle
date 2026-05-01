import sys
from types import ModuleType
from pathlib import Path

import requests

from seo_audit.search_console import collect_index_states, property_candidates, reconcile_index_states, resolve_property


def test_property_candidates_include_domain_and_url_forms() -> None:
    candidates = property_candidates("https://www.example.com")
    assert candidates[0] == "sc-domain:example.com"
    assert "https://example.com/" in candidates


def test_resolve_property_prefers_explicit_value() -> None:
    resolved = resolve_property("https://example.com", explicit_property="https://custom.example.com/")
    assert resolved == "https://custom.example.com/"


def test_collect_index_states_reports_missing_credentials() -> None:
    rows, meta = collect_index_states("sc-domain:example.com", ["https://example.com/"])
    assert rows == []
    assert meta["status"] == "skipped_missing_credentials"


def test_collect_index_states_reports_invalid_credentials_path(tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"
    rows, meta = collect_index_states("sc-domain:example.com", ["https://example.com/"], credentials_json=str(missing))
    assert rows == []
    assert meta["status"] == "failed_invalid_credentials_path"


def _install_fake_google_auth(monkeypatch, *, raise_auth: bool = False) -> None:
    fake_google = ModuleType("google")
    fake_google_auth = ModuleType("google.auth")
    fake_google_auth_transport = ModuleType("google.auth.transport")
    fake_google_auth_transport_requests = ModuleType("google.auth.transport.requests")

    class FakeAuthRequest:
        pass

    fake_google_auth_transport_requests.Request = FakeAuthRequest

    fake_google_oauth2 = ModuleType("google.oauth2")
    fake_google_oauth2_service_account = ModuleType("google.oauth2.service_account")

    class FakeCredentials:
        token = ""

        @classmethod
        def from_service_account_file(cls, *_args, **_kwargs):
            if raise_auth:
                raise RuntimeError("bad credentials")
            return cls()

        def refresh(self, _request) -> None:
            self.token = "fake-token"

    fake_google_oauth2_service_account.Credentials = FakeCredentials

    monkeypatch.setitem(sys.modules, "google", fake_google)
    monkeypatch.setitem(sys.modules, "google.auth", fake_google_auth)
    monkeypatch.setitem(sys.modules, "google.auth.transport", fake_google_auth_transport)
    monkeypatch.setitem(sys.modules, "google.auth.transport.requests", fake_google_auth_transport_requests)
    monkeypatch.setitem(sys.modules, "google.oauth2", fake_google_oauth2)
    monkeypatch.setitem(sys.modules, "google.oauth2.service_account", fake_google_oauth2_service_account)


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self) -> dict:
        return self._payload


def test_collect_index_states_success_with_rows(tmp_path: Path, monkeypatch) -> None:
    credentials = tmp_path / "gsc.json"
    credentials.write_text("{}", encoding="utf-8")
    _install_fake_google_auth(monkeypatch)

    def fake_post(_url: str, *, headers: dict, json: dict, timeout: float):  # type: ignore[override]
        assert headers["Authorization"] == "Bearer fake-token"
        assert timeout == 3.0
        inspected = str(json.get("inspectionUrl") or "")
        if inspected.endswith("/about"):
            payload = {
                "inspectionResult": {
                    "indexStatusResult": {
                        "coverageState": "Not indexed",
                        "indexingState": "INDEXING_NOT_ALLOWED",
                        "verdict": "FAIL",
                    }
                }
            }
            return _FakeResponse(200, payload)
        payload = {
            "inspectionResult": {
                "indexStatusResult": {
                    "coverageState": "Indexed, submitted in sitemap",
                    "indexingState": "INDEXING_ALLOWED",
                    "verdict": "PASS",
                }
            }
        }
        return _FakeResponse(200, payload)

    monkeypatch.setattr(requests, "post", fake_post)

    rows, meta = collect_index_states(
        "sc-domain:example.com",
        ["https://example.com/", "https://example.com/about"],
        credentials_json=str(credentials),
        timeout=3.0,
    )
    assert meta["status"] == "success"
    assert meta["rows_returned"] == 2
    by_url = {str(row.get("url") or ""): str(row.get("status") or "") for row in rows}
    assert by_url["https://example.com/"] == "indexed"
    assert by_url["https://example.com/about"] == "not_indexed"


def test_collect_index_states_partial_api_failures(tmp_path: Path, monkeypatch) -> None:
    credentials = tmp_path / "gsc.json"
    credentials.write_text("{}", encoding="utf-8")
    _install_fake_google_auth(monkeypatch)

    def fake_post(_url: str, *, headers: dict, json: dict, timeout: float):  # type: ignore[override]
        del headers, timeout
        inspected = str(json.get("inspectionUrl") or "")
        if inspected.endswith("/about"):
            return _FakeResponse(500, text="server error")
        payload = {
            "inspectionResult": {
                "indexStatusResult": {
                    "coverageState": "Indexed",
                    "indexingState": "INDEXING_ALLOWED",
                    "verdict": "PASS",
                }
            }
        }
        return _FakeResponse(200, payload)

    monkeypatch.setattr(requests, "post", fake_post)

    rows, meta = collect_index_states(
        "sc-domain:example.com",
        ["https://example.com/", "https://example.com/about"],
        credentials_json=str(credentials),
    )
    assert meta["status"] == "success_partial"
    assert meta["rows_returned"] == 1
    assert int(meta.get("error_count") or 0) == 1
    assert rows[0]["status"] == "indexed"


def test_collect_index_states_reports_auth_failure(tmp_path: Path, monkeypatch) -> None:
    credentials = tmp_path / "gsc.json"
    credentials.write_text("{}", encoding="utf-8")
    _install_fake_google_auth(monkeypatch, raise_auth=True)

    rows, meta = collect_index_states(
        "sc-domain:example.com",
        ["https://example.com/"],
        credentials_json=str(credentials),
    )
    assert rows == []
    assert meta["status"] == "failed_auth"


def test_reconcile_index_states_contract_counts() -> None:
    summary = reconcile_index_states(
        ["https://example.com/", "https://example.com/about", "https://example.com/contact"],
        [
            {"url": "https://example.com/", "status": "indexed"},
            {"url": "https://example.com/about", "status": "not_indexed"},
        ],
    )
    assert summary["crawled_total"] == 3
    assert summary["indexed"] == 1
    assert summary["not_indexed"] == 1
    assert summary["unknown"] == 1
