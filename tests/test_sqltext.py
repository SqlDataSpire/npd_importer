import pytest

from npd_loader.sqltext import render, split_batches, sql_scripts


def test_render_substitutes_and_rejects_unknown():
    assert render("SELECT * FROM <<t:practitioner>> WHERE d = <<release>>",
                  {"t:practitioner": "[npd].[p]", "release": "'2026-09-29'"}) == \
        "SELECT * FROM [npd].[p] WHERE d = '2026-09-29'"
    with pytest.raises(KeyError, match="<<nope>>"):
        render("<<nope>>", {})


def test_split_batches_on_go_lines_only():
    text = "CREATE TABLE a (x int)\nGO\n  go  \nSELECT 'GO' AS x\r\nGO\r\n"
    assert split_batches(text) == ["CREATE TABLE a (x int)", "SELECT 'GO' AS x"]


def test_sql_scripts_lists_in_name_order():
    names = [n for n, _ in sql_scripts("mssql", "init")]
    assert names[0] == "001_schemas.sql" and names == sorted(names)
