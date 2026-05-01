import json
from pathlib import Path

from seo_audit.extract import extract_page_data


def test_extract_fixture() -> None:
    html = Path("tests/fixtures/sample_page.html").read_text()
    data = extract_page_data(html, "https://example.com/service", 200, "text/html", {})
    assert data["title"] == "Sample Service Page"
    assert data["h1"] == "Service Page"
    assert data["meta_description"] == "Service description"
    assert data["internal_links_out"] >= 2
    assert data["external_links_out"] >= 1
    assert data["image_count"] == 2


def test_extract_anchor_and_visible_word_count_rules() -> None:
    html = """
    <html>
        <head>
            <title>Head Title</title>
            <link rel="Canonical" href="https://example.com/canon" />
            <style>.x { color: red; }</style>
            <script>var hidden = "ignore";</script>
            <script type="application/ld+json">{"@type":"Organization"}</script>
        </head>
        <body>
            <a href="/about">About Us</a>
            <p>Hello world</p>
        </body>
    </html>
    """
    data = extract_page_data(html, "https://example.com", 200, "text/html", {})
    assert data["canonical_url"] == "https://example.com/canon"
    assert data["anchors"][0]["anchor_text"] == "About Us"
    assert data["anchors"][0]["dom_region"] == "unknown"
    assert data["word_count"] == 4


def test_extract_counts_apex_and_www_as_internal_links() -> None:
    html = """
    <html>
        <head><title>T</title></head>
        <body>
            <a href="https://www.example.com/a">A</a>
            <a href="https://example.com/b">B</a>
            <a href="https://other.example.org/c">C</a>
            <a href="mailto:test@example.com">Mail</a>
        </body>
    </html>
    """
    data = extract_page_data(html, "https://example.com/", 200, "text/html", {})
    assert data["internal_links_out"] == 2
    assert data["external_links_out"] == 1
    assert len(data["anchors"]) == 3


def test_extract_internal_link_count_treats_apex_and_www_as_internal() -> None:
    html = """
    <html>
        <head><title>Links</title></head>
        <body>
            <a href="https://example.com/a">Apex</a>
            <a href="https://www.example.com/b">WWW</a>
            <a href="/relative">Relative</a>
            <a href="https://blog.example.com/c">Blog</a>
        </body>
    </html>
    """
    data = extract_page_data(html, "https://www.example.com/page", 200, "text/html", {})
    assert data["internal_links_out"] == 3
    assert data["external_links_out"] == 1


def test_extract_canonical_hreflang_and_pagination_signals() -> None:
        html = """
        <html lang="en">
            <head>
                <title>Catalog</title>
                <link rel="canonical" href="/catalog" />
                <link rel="canonical" href="/catalog?dup=1" />
                <link rel="alternate" hreflang="en-us" href="/catalog" />
                <link rel="alternate" hreflang="fr-fr" href="https://example.com/fr/catalog" />
                <link rel="next" href="/catalog?page=2" />
                <link rel="prev" href="/catalog?page=0" />
            </head>
            <body><h1>Catalog</h1></body>
        </html>
        """
        data = extract_page_data(html, "https://example.com/catalog?page=1", 200, "text/html", {})
        assert data["canonical_url"] == "https://example.com/catalog"
        assert data["canonical_count"] == 2
        canonical_urls = json.loads(data["canonical_urls_json"])
        assert len(canonical_urls) == 2
        assert json.loads(data["raw_canonical_urls_json"]) == canonical_urls
        assert data["hreflang_count"] == 2
        assert "fr-fr" in data["hreflang_links_json"]
        assert json.loads(data["raw_hreflang_links_json"]) == json.loads(data["hreflang_links_json"])
        assert data["rel_next_url"] == "https://example.com/catalog?page=2"
        assert data["rel_prev_url"] == "https://example.com/catalog?page=0"


def test_extract_json_ld_multiple_blocks_and_types() -> None:
        html = """
        <html>
            <head>
                <script type="application/ld+json">
                    {"@context":"https://schema.org","@type":"Organization"}
                </script>
                <script type="application/ld+json">
                    {"@graph":[{"@type":["LocalBusiness", "ProfessionalService"]}]}
                </script>
            </head>
            <body><h1>Company</h1></body>
        </html>
        """
        data = extract_page_data(html, "https://example.com/", 200, "text/html", {})
        schema_types = set(json.loads(data["schema_types_json"]))
        assert {"Organization", "LocalBusiness", "ProfessionalService"}.issubset(schema_types)
        assert data["schema_parse_error_count"] == 0


def test_extract_json_ld_malformed_block_does_not_crash() -> None:
        html = """
        <html>
            <head>
                <script type="application/ld+json">{"@type":"Organization"}</script>
                <script type="application/ld+json">{"@type":</script>
            </head>
            <body><h1>Company</h1></body>
        </html>
        """
        data = extract_page_data(html, "https://example.com/", 200, "text/html", {})
        schema_types = set(json.loads(data["schema_types_json"]))
        assert "Organization" in schema_types
        assert data["schema_parse_error_count"] == 1


def test_extract_snippet_and_nosnippet_controls() -> None:
        html = """
        <html>
            <head>
                <meta name="robots" content="nosnippet, max-snippet:50, max-image-preview:large" />
            </head>
            <body>
                <p data-nosnippet>Hide this sentence</p>
            </body>
        </html>
        """
        headers = {"x-robots-tag": "max-video-preview:30"}
        data = extract_page_data(html, "https://example.com/", 200, "text/html", headers)
        assert data["has_nosnippet_directive"] == 1
        assert data["max_snippet_directive"] == "50"
        assert data["max_image_preview_directive"] == "large"
        assert data["max_video_preview_directive"] == "30"
        assert data["data_nosnippet_count"] == 1


def test_extract_heading_outline_content_hash_and_dom_regions() -> None:
        html = """
        <html>
                <head><title>Layout</title></head>
                <body>
                        <header><a href="/home">Home</a></header>
                        <nav><a href="/services">Services</a></nav>
                        <main>
                                <h1>Main title</h1>
                                <h2>Section one</h2>
                                <p>Hello world from main content.</p>
                                <a href="/contact">Contact</a>
                        </main>
                        <aside><a href="/related">Related</a></aside>
                        <footer><a href="/legal">Legal</a></footer>
                </body>
        </html>
        """
        data = extract_page_data(html, "https://example.com/", 200, "text/html", {})

        heading_outline = json.loads(data["heading_outline_json"])
        assert heading_outline == [
                {"level": 1, "text": "Main title"},
                {"level": 2, "text": "Section one"},
        ]

        anchors = {item["href"]: item for item in data["anchors"]}
        assert anchors["/home"]["dom_region"] == "header"
        assert anchors["/services"]["dom_region"] == "nav"
        assert anchors["/contact"]["dom_region"] == "main"
        assert anchors["/related"]["dom_region"] == "aside"
        assert anchors["/legal"]["dom_region"] == "footer"

        assert len(data["content_hash"]) == 64
        assert data["content_hash"]
        assert data["raw_content_hash"] == data["content_hash"]
        assert data["raw_title"] == "Layout"
        assert json.loads(data["title_inventory_json"]) == ["Layout"]


def test_extract_schema_summary_allowlist() -> None:
        html = """
        <html>
            <head>
                <script type="application/ld+json">
                {
                    "@graph": [
                        {
                            "@type": ["Organization", "LocalBusiness"],
                            "name": "Acme Co",
                            "url": "https://example.com",
                            "sameAs": ["https://x.example", "https://y.example"],
                            "address": {
                                "addressLocality": "Austin",
                                "addressRegion": "TX",
                                "postalCode": "78701",
                                "addressCountry": "US"
                            }
                        },
                        {
                            "@type": "Product",
                            "name": "Widget",
                            "sku": "W-1",
                            "offers": {
                                "price": "99.00",
                                "priceCurrency": "USD",
                                "availability": "https://schema.org/InStock"
                            }
                        },
                        {
                            "@type": "BreadcrumbList",
                            "itemListElement": [
                                {"@type": "ListItem", "position": 1},
                                {"@type": "ListItem", "position": 2}
                            ]
                        }
                    ]
                }
                </script>
            </head>
            <body><h1>Schema sample</h1></body>
        </html>
        """

        data = extract_page_data(html, "https://example.com/", 200, "text/html", {})
        summary = json.loads(data["schema_summary_json"])
        summary_types = {entry.get("type") for entry in summary}
        assert {"Organization", "LocalBusiness", "Product", "BreadcrumbList"}.issubset(summary_types)

        product = next(entry for entry in summary if entry.get("type") == "Product")
        assert product.get("name") == "Widget"
        assert product.get("price") == "99.00"
        assert product.get("priceCurrency") == "USD"

        breadcrumb = next(entry for entry in summary if entry.get("type") == "BreadcrumbList")
        assert breadcrumb.get("itemCount") == 2


def test_extract_schema_summary_types_json_tracks_custom_summaries() -> None:
        html = """
        <html>
            <head>
                <script type="application/ld+json">
                {
                    "@type": "Product",
                    "name": "Widget",
                    "offers": {"price": "10", "priceCurrency": "USD"}
                }
                </script>
            </head>
            <body><h1>Product</h1></body>
        </html>
        """

        data = extract_page_data(html, "https://example.com/", 200, "text/html", {})
        summary_types = set(json.loads(data["schema_summary_types_json"]))
        assert "Product" in summary_types


def test_extract_includes_microdata_and_rdfa_schema_nodes() -> None:
        html = """
        <html>
            <body>
                <div itemscope itemtype="https://schema.org/Product">
                    <span itemprop="name">Micro Widget</span>
                </div>
                <section vocab="https://schema.org/" typeof="Organization">
                    <span property="name">RDFa Org</span>
                </section>
            </body>
        </html>
        """

        data = extract_page_data(html, "https://example.com/", 200, "text/html", {})
        schema_types = set(json.loads(data["schema_types_json"]))
        assert "Product" in schema_types
        assert "Organization" in schema_types
