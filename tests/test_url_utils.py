from seo_audit.url_utils import (
    internal_hosts_for_site,
    is_internal_url,
    normalize_url,
    same_registrable_domain,
    should_skip_asset,
)


def test_normalize_url_tracking_and_fragment() -> None:
    url = normalize_url("http://Example.com/path/?utm_source=x&a=1#frag")
    assert url == "http://example.com/path?a=1"


def test_normalize_url_schemeless_defaults_to_https() -> None:
    url = normalize_url("//Example.com/path/?utm_source=x&a=1#frag")
    assert url == "https://example.com/path?a=1"


def test_same_domain() -> None:
    assert same_registrable_domain("https://www.example.com/x", "example.com")
    assert not same_registrable_domain("https://blog.example.com/x", "example.com")
    assert not same_registrable_domain("https://evil.com", "example.com")


def test_skip_asset() -> None:
    assert should_skip_asset("https://example.com/image.png")


def test_localhost_stays_http() -> None:
    assert normalize_url("http://localhost:8000/path") == "http://localhost:8000/path"


def test_internal_hosts_for_site_policy() -> None:
    assert internal_hosts_for_site("https://www.example.com") == {"example.com", "www.example.com"}


def test_is_internal_url_policy() -> None:
    assert is_internal_url("/path", "https://example.com", base_url="https://example.com")
    assert is_internal_url("https://www.example.com/path", "https://example.com")
    assert not is_internal_url("https://blog.example.com/path", "https://example.com")


def test_scope_mode_host_only_stays_on_exact_host() -> None:
    assert is_internal_url(
        "https://www.example.com/path",
        "https://www.example.com",
        scope_mode="host_only",
    )
    assert not is_internal_url(
        "https://example.com/path",
        "https://www.example.com",
        scope_mode="host_only",
    )


def test_scope_mode_all_subdomains_accepts_blog() -> None:
    assert is_internal_url(
        "https://blog.example.com/path",
        "https://www.example.com",
        scope_mode="all_subdomains",
    )


def test_scope_mode_custom_allowlist_accepts_explicit_host() -> None:
    assert is_internal_url(
        "https://shop.example.net/path",
        "https://www.example.com",
        scope_mode="custom_allowlist",
        custom_allowlist=("shop.example.net",),
    )


def test_normalize_url_percent_encodes_non_ascii_path() -> None:
    normalized = normalize_url("https://www.example.com/tool-removes-Camloc®-cloc")
    assert normalized == "https://www.example.com/tool-removes-Camloc%C2%AE-cloc"
