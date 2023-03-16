from click.testing import CliRunner

from utils.rudderstack.__main__ import list_tables


def test_should_print_nothing(mocker):
    mock_sf_engine = mocker.patch("utils.rudderstack.__main__.snowflake_engine")
    mock_sf_engine.return_value = mocker.Mock()
    mock_list_event_table = mocker.patch("utils.rudderstack.__main__.list_event_tables")
    runner = CliRunner()
    # GIVEN: list event tables returns an empty list
    mock_list_event_table.return_value = []

    # WHEN: request to list tables
    result = runner.invoke(
        list_tables, ["db", "schema", "-a", "account", "-u", "user", "-p", "password", "-w", "warehouse", "-r", "role"]
    )

    # THEN: expect exit code to be 0
    assert result.exit_code == 0
    # THEN: expect nothing to be printed in stdout
    assert result.output == ''


def test_should_print_tables(mocker):
    mock_sf_engine = mocker.patch("utils.rudderstack.__main__.snowflake_engine")
    mock_sf_engine.return_value = mocker.Mock()
    mock_list_event_table = mocker.patch("utils.rudderstack.__main__.list_event_tables")
    runner = CliRunner(mix_stderr=False)
    # GIVEN: list event tables returns two new tables
    mock_list_event_table.return_value = ['new-table-1', 'new-table-2']

    # WHEN: request to list tables
    result = runner.invoke(
        list_tables, ["db", "schema", "-a", "account", "-u", "user", "-p", "password", "-w", "warehouse", "-r", "role"]
    )

    # THEN: expect exit code to be 1
    assert result.exit_code == 1
    # THEN: expect tables to be printed in stdout
    assert result.stdout == 'new-table-1\nnew-table-2\n'
    # THEN: expect error to be printed in stderr
    assert result.stderr == 'Error: New tables found...\n'


def test_should_print_tables_as_json(mocker):
    mock_sf_engine = mocker.patch("utils.rudderstack.__main__.snowflake_engine")
    mock_sf_engine.return_value = mocker.Mock()
    mock_list_event_table = mocker.patch("utils.rudderstack.__main__.list_event_tables")
    runner = CliRunner(mix_stderr=False)
    # GIVEN: list event tables returns two new tables
    mock_list_event_table.return_value = ['new-table-1', 'new-table-2']

    # WHEN: request to list tables
    result = runner.invoke(
        list_tables,
        [
            "db",
            "schema",
            "-a",
            "account",
            "-u",
            "user",
            "-p",
            "password",
            "-w",
            "warehouse",
            "-r",
            "role",
            "--format-json",
        ],
    )

    # THEN: expect exit code to be 1
    assert result.exit_code == 1
    # THEN: expect tables to be printed in stdout
    assert result.stdout == '{"new_tables": ["new-table-1", "new-table-2"]}\n'
    # THEN: expect error to be printed in stderr
    assert result.stderr == 'Error: New tables found...\n'
