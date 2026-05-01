from seo_audit.extract import extract_page_data


def test_extract_handles_malformed_html() -> None:
    html = "<html><head><title>Bad<title><meta name='description' content='x'></head><body><h1>Hi"
    data = extract_page_data(html, "https://example.com", 200, "text/html", {})
    assert "Bad" in data["title"]
    assert data["h1"] == "Hi"
    assert data["meta_description"] == "x"
    assert data["content_hash"]
