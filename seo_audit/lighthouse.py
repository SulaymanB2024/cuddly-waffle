from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import shutil
import subprocess
from typing import Callable

from seo_audit.models import LighthouseRecord


@dataclass(slots=True)
class LighthouseBudgetConfig:
    performance_min: int = 70
    seo_min: int = 70


@dataclass(slots=True)
class LighthouseCollectionTelemetry:
    attempts: int = 0
    success: int = 0
    failed: int = 0
    skipped_missing_dependency: int = 0
    budget_failed: int = 0


def _score(payload: dict, category: str) -> int | None:
    value = payload.get("categories", {}).get(category, {}).get("score")
    if isinstance(value, (int, float)):
        return int(round(float(value) * 100.0))
    return None


def _metric(payload: dict, audit_id: str) -> float | None:
    value = payload.get("audits", {}).get(audit_id, {}).get("numericValue")
    if isinstance(value, (int, float)):
        return float(value)
    return None


def resolve_lighthouse_command() -> list[str] | None:
    lighthouse_bin = shutil.which("lighthouse")
    if lighthouse_bin:
        return [lighthouse_bin]

    npx_bin = shutil.which("npx")
    if npx_bin:
        return [npx_bin, "--yes", "lighthouse"]

    return None


def ensure_sidecar_config(
    output_dir: Path,
    *,
    form_factor: str,
    config_path: str = "",
) -> Path:
    explicit_path = str(config_path or "").strip()
    if explicit_path:
        return Path(explicit_path)

    target = output_dir / "lighthouse.sidecar.json"
    payload = {
        "extends": "lighthouse:default",
        "settings": {
            "onlyCategories": ["performance", "accessibility", "best-practices", "seo"],
            "formFactor": form_factor,
            "screenEmulation": {
                "mobile": form_factor == "mobile",
            },
            "throttlingMethod": "simulate",
        },
    }
    target.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return target


def _run_command(command: list[str], *, timeout_seconds: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        command,
        capture_output=True,
        text=True,
        check=False,
        timeout=max(1.0, float(timeout_seconds)),
    )


def collect_lighthouse(
    run_id: str,
    urls: list[str],
    *,
    output_dir: Path,
    form_factor: str = "desktop",
    timeout_seconds: float = 90.0,
    config_path: str = "",
    store_payloads: bool = False,
    budgets: LighthouseBudgetConfig | None = None,
    runner: Callable[[list[str], float], subprocess.CompletedProcess[str]] | None = None,
) -> tuple[list[LighthouseRecord], list[str], dict[str, int | float]]:
    normalized_form_factor = str(form_factor or "desktop").strip().lower()
    if normalized_form_factor not in {"desktop", "mobile"}:
        normalized_form_factor = "desktop"

    budget_config = budgets or LighthouseBudgetConfig()
    telemetry = LighthouseCollectionTelemetry()
    rows: list[LighthouseRecord] = []
    messages: list[str] = []

    command_prefix = resolve_lighthouse_command()
    if command_prefix is None:
        telemetry.skipped_missing_dependency = len(urls)
        for url in urls:
            rows.append(
                LighthouseRecord(
                    run_id=run_id,
                    url=url,
                    form_factor=normalized_form_factor,
                    status="skipped_missing_dependency",
                    budget_pass=1,
                    error_message="lighthouse command unavailable",
                )
            )
        messages.append("lighthouse skipped: command unavailable (install lighthouse or npx)")
        return rows, messages, {
            "attempts": telemetry.attempts,
            "success": telemetry.success,
            "failed": telemetry.failed,
            "skipped_missing_dependency": telemetry.skipped_missing_dependency,
            "budget_failed": telemetry.budget_failed,
        }

    sidecar_config_path = ensure_sidecar_config(
        output_dir,
        form_factor=normalized_form_factor,
        config_path=config_path,
    )

    run_cmd = runner or (lambda cmd, timeout: _run_command(cmd, timeout_seconds=timeout))
    chrome_flags = "--headless=new --no-sandbox --disable-dev-shm-usage"

    for url in urls:
        telemetry.attempts += 1
        command = [
            *command_prefix,
            url,
            "--quiet",
            "--output=json",
            "--output-path=stdout",
            f"--chrome-flags={chrome_flags}",
            f"--config-path={sidecar_config_path}",
        ]

        try:
            completed = run_cmd(command, timeout_seconds)
        except Exception as exc:  # pragma: no cover - subprocess timeout/system errors.
            telemetry.failed += 1
            message = f"lighthouse failed: {url} ({exc})"
            messages.append(message)
            rows.append(
                LighthouseRecord(
                    run_id=run_id,
                    url=url,
                    form_factor=normalized_form_factor,
                    status="failed",
                    budget_pass=1,
                    error_message=str(exc),
                )
            )
            continue

        if int(getattr(completed, "returncode", 1) or 0) != 0:
            telemetry.failed += 1
            stderr = str(getattr(completed, "stderr", "") or "").strip()
            message = f"lighthouse failed: {url} ({stderr or 'non-zero exit'})"
            messages.append(message)
            rows.append(
                LighthouseRecord(
                    run_id=run_id,
                    url=url,
                    form_factor=normalized_form_factor,
                    status="failed",
                    budget_pass=1,
                    error_message=stderr or "non-zero exit",
                )
            )
            continue

        stdout = str(getattr(completed, "stdout", "") or "").strip()
        try:
            payload_root = json.loads(stdout) if stdout else {}
        except json.JSONDecodeError:
            telemetry.failed += 1
            message = f"lighthouse failed: {url} (invalid JSON output)"
            messages.append(message)
            rows.append(
                LighthouseRecord(
                    run_id=run_id,
                    url=url,
                    form_factor=normalized_form_factor,
                    status="failed",
                    budget_pass=1,
                    error_message="invalid JSON output",
                )
            )
            continue

        lighthouse_payload = payload_root.get("categories") and payload_root or payload_root.get("lighthouseResult") or {}
        performance_score = _score(lighthouse_payload, "performance")
        accessibility_score = _score(lighthouse_payload, "accessibility")
        best_practices_score = _score(lighthouse_payload, "best-practices")
        seo_score = _score(lighthouse_payload, "seo")

        budget_failures: list[str] = []
        if performance_score is not None and performance_score < int(budget_config.performance_min):
            budget_failures.append(
                f"performance<{int(budget_config.performance_min)} ({performance_score})"
            )
        if seo_score is not None and seo_score < int(budget_config.seo_min):
            budget_failures.append(f"seo<{int(budget_config.seo_min)} ({seo_score})")

        if budget_failures:
            telemetry.budget_failed += 1
        telemetry.success += 1

        rows.append(
            LighthouseRecord(
                run_id=run_id,
                url=url,
                form_factor=normalized_form_factor,
                status="success",
                performance_score=performance_score,
                accessibility_score=accessibility_score,
                best_practices_score=best_practices_score,
                seo_score=seo_score,
                lcp=_metric(lighthouse_payload, "largest-contentful-paint"),
                cls=_metric(lighthouse_payload, "cumulative-layout-shift"),
                inp=_metric(lighthouse_payload, "interaction-to-next-paint"),
                ttfb=_metric(lighthouse_payload, "server-response-time"),
                total_blocking_time=_metric(lighthouse_payload, "total-blocking-time"),
                speed_index=_metric(lighthouse_payload, "speed-index"),
                budget_pass=int(not budget_failures),
                budget_failures_json=json.dumps(budget_failures, sort_keys=True),
                payload_json=(json.dumps(payload_root, sort_keys=True) if store_payloads else "{}"),
                error_message="",
            )
        )

    return rows, messages, {
        "attempts": telemetry.attempts,
        "success": telemetry.success,
        "failed": telemetry.failed,
        "skipped_missing_dependency": telemetry.skipped_missing_dependency,
        "budget_failed": telemetry.budget_failed,
    }
