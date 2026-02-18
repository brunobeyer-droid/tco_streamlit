from db.mssql_backend import _is_portfolio_dml_sql


def test_snapshot_maintenance_dml_does_not_bump_data_version() -> None:
    assert _is_portfolio_dml_sql("DELETE FROM dbo.TCO_TEAM_VELOCITY_SNAPSHOT WHERE YEAR = 2026") is False
    assert _is_portfolio_dml_sql(
        "INSERT INTO dbo.TCO_TEAM_VELOCITY_SNAPSHOT (TEAMID, YEAR, PI) VALUES ('t1', 2026, 1)"
    ) is False


def test_business_table_dml_still_bumps_data_version() -> None:
    assert _is_portfolio_dml_sql("UPDATE dbo.TEAMS SET TEAMNAME='A' WHERE TEAMID='1'") is True
