"""Regression coverage for the installed local-development command."""

import pytest

from flask_app import run


def test_development_cli_defaults_to_loopback_without_debug(monkeypatch):
    captured = {}
    monkeypatch.setattr(run.app, "run", lambda **kwargs: captured.update(kwargs))

    run.main([])

    assert captured == {
        "host": "127.0.0.1",
        "port": 5000,
        "debug": False,
        "use_reloader": False,
    }


def test_development_cli_accepts_explicit_host_port_and_debug(monkeypatch):
    captured = {}
    monkeypatch.setattr(run.app, "run", lambda **kwargs: captured.update(kwargs))

    run.main(["--host", "0.0.0.0", "--port", "5050", "--debug"])

    assert captured == {
        "host": "0.0.0.0",
        "port": 5050,
        "debug": True,
        "use_reloader": True,
    }


def test_development_cli_help_does_not_start_server(monkeypatch, capsys):
    monkeypatch.setattr(
        run.app,
        "run",
        lambda **_kwargs: pytest.fail("server started while showing help"),
    )

    with pytest.raises(SystemExit) as exc_info:
        run.main(["--help"])

    assert exc_info.value.code == 0
    assert "Run the local RDS web application" in capsys.readouterr().out


@pytest.mark.parametrize("port", ["0", "65536"])
def test_development_cli_rejects_invalid_ports(monkeypatch, port):
    monkeypatch.setattr(
        run.app,
        "run",
        lambda **_kwargs: pytest.fail("server started with invalid port"),
    )

    with pytest.raises(SystemExit) as exc_info:
        run.main(["--port", port])

    assert exc_info.value.code == 2
