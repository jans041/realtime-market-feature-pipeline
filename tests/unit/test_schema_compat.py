from pathlib import Path

import pytest

from pipeline.governance import schema_compat


@pytest.fixture
def no_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise AssertionError("the registry must not be called")

    monkeypatch.setattr(schema_compat, "check_compatibility", fail)


@pytest.mark.usefixtures("no_registry")
def test_missing_base_means_new_schema_and_passes(tmp_path: Path) -> None:
    candidate = tmp_path / "trade.avsc"
    candidate.write_text('{"type": "string"}', encoding="utf-8")

    code = schema_compat.main(
        ["--base", str(tmp_path / "absent.avsc"), "--candidate", str(candidate)]
    )

    assert code == 0


@pytest.mark.usefixtures("no_registry")
def test_empty_base_is_treated_as_missing(tmp_path: Path) -> None:
    # `git show main:schemas/x.avsc > base.avsc` leaves an empty file when x is new.
    base = tmp_path / "base.avsc"
    base.write_text("", encoding="utf-8")
    candidate = tmp_path / "trade.avsc"
    candidate.write_text('{"type": "string"}', encoding="utf-8")

    assert schema_compat.main(["--base", str(base), "--candidate", str(candidate)]) == 0


def test_incompatible_result_fails_the_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        schema_compat,
        "check_compatibility",
        lambda *args: schema_compat.CompatibilityResult(False, ["READER_FIELD_MISSING_DEFAULT"]),
    )
    base = tmp_path / "base.avsc"
    base.write_text('{"type": "string"}', encoding="utf-8")

    assert schema_compat.main(["--base", str(base), "--candidate", str(base)]) == 1
