from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from urllib.parse import quote

import requests


_GSC_READONLY_SCOPE = "https://www.googleapis.com/auth/webmasters.readonly"


def default_date_window(days: int) -> tuple[str, str]:
    safe_days = max(1, int(days))
    end = date.today()
    start = end - timedelta(days=safe_days - 1)
    return start.isoformat(), end.isoformat()


def _service_account_token(credentials_json: str) -> tuple[str | None, str | None]:
    credentials_path = Path(credentials_json)
    if not credentials_json:
        return None, "missing_credentials"
    if not credentials_path.exists():
        return None, f"credentials_not_found:{credentials_path}"

    try:
        from google.auth.transport.requests import Request as GoogleAuthRequest  # type: ignore[import-not-found]
        from google.oauth2 import service_account  # type: ignore[import-not-found]
    except Exception as exc:
        return None, f"missing_dependency:{exc}"

    try:
        credentials = service_account.Credentials.from_service_account_file(
            str(credentials_path),
            scopes=[_GSC_READONLY_SCOPE],
        )
        credentials.refresh(GoogleAuthRequest())
        token = str(credentials.token or "").strip()
        if not token:
            return None, "empty_access_token"
        return token, None
    except Exception as exc:
        return None, f"auth_failed:{exc}"


def collect_search_analytics(
    property_uri: str,
    *,
    credentials_json: str,
    start_date: str,
    end_date: str,
    dimensions: tuple[str, ...] = ("page", "query", "device", "country", "date"),
    row_limit: int = 5000,
    timeout: float = 20.0,
) -> tuple[list[dict], dict[str, object]]:
    token, token_error = _service_account_token(credentials_json)
    if token is None:
        return [], {
            "status": "failed_auth",
            "property_uri": property_uri,
            "rows_returned": 0,
            "message": token_error or "authentication failed",
        }

    encoded_site = quote(property_uri, safe="")
    endpoint = f"https://searchconsole.googleapis.com/webmasters/v3/sites/{encoded_site}/searchAnalytics/query"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    rows: list[dict] = []
    start_row = 0
    page_size = min(25000, max(1, int(row_limit)))

    while len(rows) < row_limit:
        request_body = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": list(dimensions),
            "rowLimit": min(page_size, row_limit - len(rows)),
            "startRow": start_row,
            "searchType": "web",
        }

        try:
            response = requests.post(endpoint, headers=headers, json=request_body, timeout=timeout)
        except requests.RequestException as exc:
            return rows, {
                "status": "failed_api",
                "property_uri": property_uri,
                "rows_returned": len(rows),
                "message": f"network_error:{exc}",
            }

        if response.status_code in {401, 403}:
            return rows, {
                "status": "failed_auth",
                "property_uri": property_uri,
                "rows_returned": len(rows),
                "message": f"auth_error:{response.text[:260]}",
            }

        if response.status_code >= 400:
            return rows, {
                "status": "failed_api",
                "property_uri": property_uri,
                "rows_returned": len(rows),
                "message": f"http_{response.status_code}:{response.text[:260]}",
            }

        payload = response.json() if response.text.strip() else {}
        batch = payload.get("rows") if isinstance(payload, dict) else []
        if not isinstance(batch, list) or not batch:
            break

        for row in batch:
            keys = row.get("keys") if isinstance(row, dict) else []
            if not isinstance(keys, list):
                keys = []
            record = {
                "clicks": float(row.get("clicks") or 0.0),
                "impressions": float(row.get("impressions") or 0.0),
                "ctr": float(row.get("ctr") or 0.0),
                "position": float(row.get("position") or 0.0),
            }
            for index, dim in enumerate(dimensions):
                record[dim] = str(keys[index]) if index < len(keys) else ""
            rows.append(record)
            if len(rows) >= row_limit:
                break

        if len(batch) < request_body["rowLimit"]:
            break
        start_row += len(batch)

    status = "success" if rows else "success_empty"
    message = "search analytics collected" if rows else "search analytics returned no rows"
    return rows, {
        "status": status,
        "property_uri": property_uri,
        "rows_returned": len(rows),
        "start_date": start_date,
        "end_date": end_date,
        "dimensions": list(dimensions),
        "message": message,
    }


def summarize_search_analytics(rows: list[dict]) -> dict[str, object]:
    if not rows:
        return {
            "rows": 0,
            "clicks": 0.0,
            "impressions": 0.0,
            "avg_ctr": 0.0,
            "avg_position": 0.0,
        }

    clicks = sum(float(row.get("clicks") or 0.0) for row in rows)
    impressions = sum(float(row.get("impressions") or 0.0) for row in rows)
    avg_ctr = (clicks / impressions) if impressions > 0 else 0.0
    avg_position = sum(float(row.get("position") or 0.0) for row in rows) / len(rows)

    return {
        "rows": len(rows),
        "clicks": round(clicks, 2),
        "impressions": round(impressions, 2),
        "avg_ctr": round(avg_ctr, 6),
        "avg_position": round(avg_position, 4),
    }
