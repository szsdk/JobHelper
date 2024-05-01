from tests.utils import run_jh


def test_init(tmp_path, monkeypatch, capsys):
    monkeypatch.syspath_prepend(tmp_path)
    monkeypatch.chdir(tmp_path)
    run_jh("jh init")
    capsys.readouterr()
    run_jh("jh add-one -n 2 - run")
    captured = capsys.readouterr()
    assert captured.out == "3\n"
    assert captured.err == ""
