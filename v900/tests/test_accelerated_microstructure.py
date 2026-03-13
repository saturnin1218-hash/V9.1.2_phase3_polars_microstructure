from __future__ import annotations

import pandas as pd

from binance_quant_builder.accelerated import benchmark_backend, polars_available
from binance_quant_builder.features.microstructure import build_microstructure_features_from_trades


def _sample_trades() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2024-01-01T00:00:00Z",
                    "2024-01-01T00:10:00Z",
                    "2024-01-01T00:20:00Z",
                    "2024-01-01T01:00:00Z",
                    "2024-01-01T01:10:00Z",
                    "2024-01-01T01:20:00Z",
                    "2024-01-01T02:00:00Z",
                    "2024-01-01T02:10:00Z",
                ],
                utc=True,
            ),
            "price": [100, 101, 102, 101, 103, 104, 103, 105],
            "quantity": [1, 2, 1.5, 1.2, 2.1, 0.9, 1.0, 1.1],
            "is_buyer_maker": [False, True, False, True, False, True, False, False],
        }
    )


def test_microstructure_auto_backend_matches_pandas_core_columns():
    trades = _sample_trades()
    pandas_out = build_microstructure_features_from_trades(trades, freq="1h", rolling_window=3, large_trade_quantile=0.9, acceleration_backend="pandas")
    auto_out = build_microstructure_features_from_trades(trades, freq="1h", rolling_window=3, large_trade_quantile=0.9, acceleration_backend="auto")
    cols = [
        "timestamp",
        "price_open",
        "price_high",
        "price_low",
        "price_close",
        "trade_count",
        "quantity_sum",
        "notional_sum",
        "buy_notional",
        "sell_notional",
        "large_trade_count",
        "large_trade_share",
        "signed_notional_imbalance",
    ]
    pd.testing.assert_frame_equal(
        pandas_out[cols].fillna(0).reset_index(drop=True),
        auto_out[cols].fillna(0).reset_index(drop=True),
        check_dtype=False,
    )


def test_benchmark_backend_returns_metrics():
    trades = _sample_trades()
    bench = benchmark_backend(
        build_microstructure_features_from_trades,
        trades,
        freq="1h",
        rolling_window=3,
        large_trade_quantile=0.9,
        acceleration_backend="pandas",
        repeat=2,
        warmup=0,
    )
    assert bench["avg_seconds"] >= 0.0
    assert bench["best_seconds"] >= 0.0
    assert bench["repeat"] == 2.0
    assert polars_available() in {True, False}
