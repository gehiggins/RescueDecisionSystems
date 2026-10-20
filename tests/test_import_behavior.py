import importlib


def test_shared_import_module_does_not_print_during_import(capsys):
    from flask_app.app import setup_imports

    capsys.readouterr()
    importlib.reload(setup_imports)

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
