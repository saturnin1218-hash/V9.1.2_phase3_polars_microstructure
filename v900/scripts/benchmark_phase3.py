from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from binance_quant_builder.accelerated import benchmark_backend, polars_available
from binance_quant_builder.features.microstructure import build_microstructure_features_from_trades


def _load_frame(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark phase 3 pandas vs polars sur les features microstructure.")
    parser.add_argument("input", help="Chemin vers un CSV/Parquet de trades natifs")
    parser.add_argument("--freq", default="1h")
    parser.add_argument("--rolling-window", type=int, default=1000)
    parser.add_argument("--large-trade-quantile", type=float, default=0.95)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--output", default="phase3_benchmark.json")
    args = parser.parse_args()

    df = _load_frame(Path(args.input))

    pandas_metrics = benchmark_backend(
        build_microstructure_features_from_trades,
        df,
        freq=args.freq,
        rolling_window=args.rolling_window,
        large_trade_quantile=args.large_trade_quantile,
        acceleration_backend="pandas",
        repeat=args.repeat,
        warmup=1,
    )
    payload = {
        "rows": int(len(df)),
        "freq": args.freq,
        "polars_available": polars_available(),
        "pandas": pandas_metrics,
    }

    if polars_available():
        polars_metrics = benchmark_backend(
            build_microstructure_features_from_trades,
            df,
            freq=args.freq,
            rolling_window=args.rolling_window,
            large_trade_quantile=args.large_trade_quantile,
            acceleration_backend="polars",
            repeat=args.repeat,
            warmup=1,
        )
        payload["polars"] = polars_metrics
        payload["speedup_best_vs_pandas"] = (
            pandas_metrics["best_seconds"] / polars_metrics["best_seconds"]
            if polars_metrics["best_seconds"] > 0
            else None
        )

    out_path = Path(args.output)
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))
    print(f"Benchmark écrit dans {out_path}")


if __name__ == "__main__":
    main()
