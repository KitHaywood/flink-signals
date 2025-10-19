from scripts.strategy_runs import build_parser


def test_strategy_runs_parser_commands():
    parser = build_parser()
    list_args = parser.parse_args(["list"])
    assert hasattr(list_args, "active_only")

    create_args = parser.parse_args(["create", "sma_cross"])
    assert create_args.strategy_name == "sma_cross"

    update_args = parser.parse_args(["update", "run-id", "--end"])
    assert update_args.end is True
