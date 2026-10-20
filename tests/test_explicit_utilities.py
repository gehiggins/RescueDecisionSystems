import importlib


def test_tle_fetcher_import_does_not_create_output(monkeypatch, tmp_path):
    output_dir = tmp_path / "tle"
    monkeypatch.setenv("RDS_TLE_DIR", str(output_dir))

    module = importlib.import_module("flask_app.app.tle_fetcher")
    importlib.reload(module)

    assert not output_dir.exists()


def test_tle_fetcher_uses_timeout_and_configured_directory(monkeypatch, tmp_path):
    from flask_app.app import tle_fetcher

    output_dir = tmp_path / "tle"
    calls = []

    class Response:
        text = "SATELLITE\nLINE 1\nLINE 2\n"

        @staticmethod
        def raise_for_status():
            return None

    def fake_get(url, timeout):
        calls.append((url, timeout))
        return Response()

    monkeypatch.setenv("RDS_TLE_DIR", str(output_dir))
    monkeypatch.setattr(tle_fetcher.requests, "get", fake_get)

    written = tle_fetcher.fetch_tle(
        {"test": "https://example.invalid/test.tle"}, timeout=7
    )

    assert calls == [("https://example.invalid/test.tle", 7)]
    assert written == [output_dir / "test.tle"]
    assert written[0].read_text(encoding="utf-8").startswith("SATELLITE")


def test_legacy_create_test_import_is_quiet(capsys):
    module = importlib.import_module("flask_app.app.create_test")
    importlib.reload(module)

    assert capsys.readouterr().out == ""
