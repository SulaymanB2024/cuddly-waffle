import builtins
import types

from seo_audit.render import PlaywrightRenderer, render_url


def test_render_import_failure_is_explicit(monkeypatch) -> None:
    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "playwright.sync_api":
            raise ImportError("no playwright")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    result, error = render_url("https://example.com")
    assert result is None
    assert error is not None
    assert "playwright import failed" in error


def test_render_uses_configured_user_agent(monkeypatch) -> None:
    original_import = builtins.__import__
    captured: dict[str, str | None] = {}

    class FakePage:
        def goto(self, *_args, **_kwargs):
            return None

        def content(self):
            return "<html><head><title>T</title></head><body><h1>H</h1></body></html>"

    class FakeContext:
        def new_page(self):
            return FakePage()

        def close(self):
            return None

    class FakeBrowser:
        def new_context(self, user_agent=None):
            captured["user_agent"] = user_agent
            return FakeContext()

        def close(self):
            return None

    class FakeChromium:
        def launch(self, headless=True):
            return FakeBrowser()

    class FakePlaywright:
        chromium = FakeChromium()

    class FakeSyncPlaywright:
        def __enter__(self):
            return FakePlaywright()

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_import(name, *args, **kwargs):
        if name == "playwright.sync_api":
            return types.SimpleNamespace(Error=Exception, sync_playwright=lambda: FakeSyncPlaywright())
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    result, error = render_url("https://example.com", user_agent="TestAgent/2.0")
    assert error is None
    assert result is not None
    assert captured["user_agent"] == "TestAgent/2.0"


def test_playwright_renderer_reuses_single_browser_instance(monkeypatch) -> None:
    original_import = builtins.__import__
    captured = {"launches": 0, "contexts": 0}

    class FakePage:
        def __init__(self):
            self.url = ""

        def route(self, *_args, **_kwargs):
            return None

        def goto(self, url, *_args, **_kwargs):
            self.url = url
            return None

        def wait_for_load_state(self, *_args, **_kwargs):
            return None

        def evaluate(self, *_args, **_kwargs):
            return 100

        def wait_for_timeout(self, *_args, **_kwargs):
            return None

        def content(self):
            return "<html><head><title>T</title></head><body><h1>H</h1></body></html>"

    class FakeContext:
        def new_page(self):
            return FakePage()

        def close(self):
            return None

    class FakeBrowser:
        def new_context(self, user_agent=None):
            del user_agent
            captured["contexts"] += 1
            return FakeContext()

        def close(self):
            return None

    class FakeChromium:
        def launch(self, headless=True):
            del headless
            captured["launches"] += 1
            return FakeBrowser()

    class FakePlaywright:
        chromium = FakeChromium()

    class FakeSyncPlaywright:
        def __enter__(self):
            return FakePlaywright()

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_import(name, *args, **kwargs):
        if name == "playwright.sync_api":
            return types.SimpleNamespace(Error=Exception, sync_playwright=lambda: FakeSyncPlaywright())
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    first = None
    second = None
    first_error = None
    second_error = None
    with PlaywrightRenderer(timeout=3.0, user_agent="TestAgent/2.0") as renderer:
        first, first_error = renderer.render("https://example.com/one")
        second, second_error = renderer.render("https://example.com/two")

    assert first_error is None
    assert second_error is None
    assert first is not None and second is not None
    assert captured["launches"] == 1
    assert captured["contexts"] == 1
