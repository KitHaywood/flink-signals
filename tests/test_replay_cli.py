from scripts.replay_prices import parse_args, parse_timestamp_ms


def test_parse_timestamp_ms_handles_z_suffix():
    ts_ms = parse_timestamp_ms("2024-06-01T12:00:00Z")
    assert ts_ms == 1717243200000


def test_parse_timestamp_ms_handles_offset():
    ts_ms = parse_timestamp_ms("2024-06-01T14:00:00+02:00")
    assert ts_ms == 1717243200000


def test_parse_args_supports_healthcheck_flags():
    args = parse_args(["--healthcheck", "--check-connection"])
    assert args.healthcheck is True
    assert args.check_connection is True
