import json
from pathlib import Path
import subprocess

from seo_audit.lighthouse import LighthouseBudgetConfig, collect_lighthouse, ensure_sidecar_config


def test_ensure_sidecar_config_writes_default_file(tmp_path: Path) -> None:
    config_path = ensure_sidecar_config(tmp_path, form_factor="desktop")
    assert config_path.exists()

    payload = json.loads(config_path.read_text(encoding="utf-8"))
    assert payload["settings"]["formFactor"] == "desktop"
    assert "performance" in payload["settings"]["onlyCategories"]


def test_collect_lighthouse_parses_scores_and_budget_failures(tmp_path: Path, monkeypatch) -> None:
    lighthouse_payload = {
        "categories": {
            "performance": {"score": 0.61},
            "accessibility": {"score": 0.91},
            "best-practices": {"score": 0.88},
            "seo": {"score": 0.62},
        },
        "audits": {
            "largest-contentful-paint": {"numericValue": 2100},
            "cumulative-layout-shift": {"numericValue": 0.08},
            "interaction-to-next-paint": {"numericValue": 240},
            "server-response-time": {"numericValue": 120},
            "total-blocking-time": {"numericValue": 180},
            "speed-index": {"numericValue": 2900},
        },
    }

    monkeypatch.setattr("seo_audit.lighthouse.resolve_lighthouse_command", lambda: ["lighthouse"])

    def fake_runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        del timeout_seconds
        assert command[0] == "lighthouse"
        return subprocess.CompletedProcess(
            args=command,
            returncode=0,
            stdout=json.dumps(lighthouse_payload),
            stderr="",
        )

    rows, messages, telemetry = collect_lighthouse(
        "run-1",
        ["https://example.com"],
        output_dir=tmp_path,
        form_factor="mobile",
        budgets=LighthouseBudgetConfig(performance_min=70, seo_min=70),
        runner=fake_runner,
        store_payloads=False,
    )

    assert messages == []
    assert telemetry["attempts"] == 1
    assert telemetry["success"] == 1
    assert telemetry["budget_failed"] == 1
    assert len(rows) == 1
    assert rows[0].status == "success"
    assert rows[0].form_factor == "mobile"
    assert rows[0].performance_score == 61
    assert rows[0].seo_score == 62
    assert rows[0].budget_pass == 0


def test_collect_lighthouse_skips_when_command_unavailable(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("seo_audit.lighthouse.resolve_lighthouse_command", lambda: None)

    rows, messages, telemetry = collect_lighthouse(
        "run-1",
        ["https://example.com"],
        output_dir=tmp_path,
    )

    assert len(rows) == 1
    assert rows[0].status == "skipped_missing_dependency"
    assert telemetry["skipped_missing_dependency"] == 1
    assert messages
