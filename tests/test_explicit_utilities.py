import importlib

import pytest


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
        text = "SATELLITE\n1 TEST ORBIT\n2 TEST ORBIT\n"

        @staticmethod
        def raise_for_status():
            return None

    def fake_get(url, timeout, headers):
        calls.append((url, timeout, headers))
        return Response()

    monkeypatch.setenv("RDS_TLE_DIR", str(output_dir))
    monkeypatch.setattr(tle_fetcher.requests, "get", fake_get)

    written = tle_fetcher.fetch_tle(
        {"test": "https://example.invalid/test.tle"}, timeout=7
    )

    assert calls == [
        (
            "https://example.invalid/test.tle",
            7.0,
            tle_fetcher.TLE_HEADERS,
        )
    ]
    assert written == [output_dir / "test.tle"]
    assert written[0].read_text(encoding="utf-8").startswith("SATELLITE")


def test_tle_fetcher_rejects_unsafe_group_without_request(monkeypatch, tmp_path):
    from flask_app.app import tle_fetcher

    monkeypatch.setenv("RDS_TLE_DIR", str(tmp_path / "tle"))
    monkeypatch.setattr(
        tle_fetcher.requests,
        "get",
        lambda *_args, **_kwargs: pytest.fail("unsafe group should not be fetched"),
    )

    written = tle_fetcher.fetch_tle(
        {"../../outside": "https://example.invalid/test.tle"}
    )

    assert written == []
    assert not (tmp_path / "outside.tle").exists()


def test_tle_fetcher_explicit_empty_sources_do_nothing(monkeypatch, tmp_path):
    from flask_app.app import tle_fetcher

    output_dir = tmp_path / "tle"
    monkeypatch.setenv("RDS_TLE_DIR", str(output_dir))
    monkeypatch.setattr(
        tle_fetcher.requests,
        "get",
        lambda *_args, **_kwargs: pytest.fail("empty sources should not be fetched"),
    )

    assert tle_fetcher.fetch_tle({}) == []
    assert not output_dir.exists()


def test_tle_fetcher_does_not_replace_cache_with_invalid_response(
    monkeypatch, tmp_path
):
    from flask_app.app import tle_fetcher

    output_dir = tmp_path / "tle"
    output_dir.mkdir()
    cached = output_dir / "test.tle"
    cached.write_text("trusted cache", encoding="utf-8")

    class Response:
        text = "<html>provider error</html>"

        @staticmethod
        def raise_for_status():
            return None

    monkeypatch.setenv("RDS_TLE_DIR", str(output_dir))
    monkeypatch.setattr(
        tle_fetcher.requests, "get", lambda *_args, **_kwargs: Response()
    )

    written = tle_fetcher.fetch_tle(
        {"test": "https://example.invalid/test.tle"}
    )

    assert written == []
    assert cached.read_text(encoding="utf-8") == "trusted cache"


def test_tle_fetcher_does_not_follow_output_file_symlink(monkeypatch, tmp_path):
    from flask_app.app import tle_fetcher

    output_dir = tmp_path / "tle"
    output_dir.mkdir()
    protected = tmp_path / "protected.txt"
    protected.write_text("do not replace", encoding="utf-8")
    (output_dir / "test.tle").symlink_to(protected)

    class Response:
        text = "SATELLITE\n1 TEST ORBIT\n2 TEST ORBIT\n"

        @staticmethod
        def raise_for_status():
            return None

    monkeypatch.setenv("RDS_TLE_DIR", str(output_dir))
    monkeypatch.setattr(
        tle_fetcher.requests, "get", lambda *_args, **_kwargs: Response()
    )

    written = tle_fetcher.fetch_tle(
        {"test": "https://example.invalid/test.tle"}
    )

    assert written == []
    assert protected.read_text(encoding="utf-8") == "do not replace"


def test_legacy_create_test_import_is_quiet(capsys):
    module = importlib.import_module("flask_app.app.create_test")
    importlib.reload(module)

    assert capsys.readouterr().out == ""
