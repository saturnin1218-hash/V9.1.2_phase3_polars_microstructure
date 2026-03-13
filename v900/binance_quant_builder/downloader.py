from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .rate_limiter import RateLimiter
from .utils.time import ensure_utc_timestamp


PACKAGE_VERSION = "9.1.2"
BASE_URL_BY_MARKET = {
    "futures_um": "https://fapi.binance.com/fapi/v1/aggTrades",
    "spot": "https://api.binance.com/api/v3/aggTrades",
}
FUNDING_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
OPEN_INTEREST_HIST_URL = "https://fapi.binance.com/futures/data/openInterestHist"
LIQUIDATIONS_URL = "https://fapi.binance.com/fapi/v1/allForceOrders"


def build_http_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": f"binance-quant-builder/{PACKAGE_VERSION}"})
    retries = Retry(total=5, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET", "HEAD"])
    adapter = HTTPAdapter(max_retries=retries, pool_connections=16, pool_maxsize=16)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def api_get_with_retry(session: requests.Session, url: str, rate_limiter: Optional[RateLimiter] = None, timeout: int = 60, max_attempts: int = 5, params: dict | None = None):
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        if rate_limiter is not None:
            rate_limiter.acquire()
        try:
            resp = session.get(url, timeout=timeout, params=params)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            time.sleep(min(2 ** attempt, 10))
    raise RuntimeError(f"Echec GET {url}: {last_exc}")


def _to_ms(value: str) -> int:
    ts = datetime.fromisoformat(str(value))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)
    return int(ts.timestamp() * 1000)


def fetch_aggtrades_dataframe(
    symbol: str,
    market: str,
    start: str,
    end: str,
    session: requests.Session,
    rate_limiter: Optional[RateLimiter] = None,
    timeout: int = 60,
    limit: int = 1000,
) -> pd.DataFrame:
    url = BASE_URL_BY_MARKET.get(market)
    if not url:
        raise ValueError(f"Marché non supporté pour le downloader natif: {market}")

    start_ms = _to_ms(start)
    end_ms = _to_ms(end)
    cursor = start_ms
    rows: list[dict] = []

    while cursor < end_ms:
        params = {"symbol": symbol, "startTime": cursor, "endTime": end_ms, "limit": min(int(limit), 1000)}
        resp = api_get_with_retry(session=session, url=url, rate_limiter=rate_limiter, timeout=timeout, params=params)
        payload = resp.json()
        if not payload:
            break

        last_trade_time = cursor
        for item in payload:
            trade_time = int(item.get('T', item.get('time', cursor)))
            if trade_time >= end_ms:
                continue
            rows.append({
                'agg_trade_id': item.get('a'),
                'price': item.get('p'),
                'quantity': item.get('q'),
                'first_trade_id': item.get('f'),
                'last_trade_id': item.get('l'),
                'timestamp': pd.to_datetime(trade_time, unit='ms', utc=True),
                'is_buyer_maker': item.get('m'),
                'is_best_match': item.get('M'),
            })
            last_trade_time = max(last_trade_time, trade_time)

        if len(payload) < min(int(limit), 1000):
            break
        next_cursor = last_trade_time + 1
        if next_cursor <= cursor:
            break
        cursor = next_cursor

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=['agg_trade_id','price','quantity','first_trade_id','last_trade_id','timestamp','is_buyer_maker','is_best_match'])
    df = df.sort_values(['timestamp', 'agg_trade_id']).drop_duplicates(subset=['agg_trade_id'], keep='last').reset_index(drop=True)
    df['timestamp'] = ensure_utc_timestamp(df['timestamp'])
    return df


def _parse_payload_to_rows(payload) -> list[dict]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        data = payload.get('data')
        if isinstance(data, list):
            return data
        return [payload]
    return []


def _paginate_timeboxed_rows(
    *,
    url: str,
    session: requests.Session,
    start_ms: int,
    end_ms: int,
    limit: int,
    timeout: int,
    rate_limiter: Optional[RateLimiter],
    base_params: dict | None = None,
    time_field_candidates: tuple[str, ...] = ('timestamp', 'time', 'fundingTime', 'updateTime'),
) -> list[dict]:
    rows: list[dict] = []
    cursor = int(start_ms)
    hard_limit = max(1, int(limit))
    extra = dict(base_params or {})

    while cursor < end_ms:
        params = {**extra, 'startTime': cursor, 'endTime': end_ms, 'limit': hard_limit}
        resp = api_get_with_retry(session=session, url=url, rate_limiter=rate_limiter, timeout=timeout, params=params)
        batch = _parse_payload_to_rows(resp.json())
        if not batch:
            break

        max_seen = cursor
        progressed = False
        for item in batch:
            if not isinstance(item, dict):
                continue
            item_time = None
            for field in time_field_candidates:
                value = item.get(field)
                if value is not None:
                    try:
                        item_time = int(value)
                    except Exception:
                        item_time = None
                    break
            if item_time is not None:
                if item_time < start_ms:
                    continue
                if item_time >= end_ms:
                    continue
                max_seen = max(max_seen, item_time)
                progressed = True
            rows.append(item)

        if len(batch) < hard_limit:
            break
        next_cursor = max_seen + 1 if progressed else cursor + 1
        if next_cursor <= cursor:
            break
        cursor = next_cursor

    return rows


def fetch_funding_rate_dataframe(symbol: str, start: str, end: str, session: requests.Session, rate_limiter: Optional[RateLimiter] = None, timeout: int = 60, limit: int = 1000) -> pd.DataFrame:
    rows = _paginate_timeboxed_rows(
        url=FUNDING_URL,
        session=session,
        start_ms=_to_ms(start),
        end_ms=_to_ms(end),
        limit=min(int(limit), 1000),
        timeout=timeout,
        rate_limiter=rate_limiter,
        base_params={"symbol": symbol},
        time_field_candidates=('fundingTime', 'time', 'timestamp'),
    )
    if not rows:
        return pd.DataFrame(columns=['timestamp', 'funding_rate', 'mark_price'])
    df = pd.DataFrame(rows)
    if 'fundingTime' in df.columns:
        df['timestamp'] = pd.to_datetime(pd.to_numeric(df['fundingTime'], errors='coerce'), unit='ms', utc=True)
    elif 'time' in df.columns:
        df['timestamp'] = pd.to_datetime(pd.to_numeric(df['time'], errors='coerce'), unit='ms', utc=True)
    else:
        df['timestamp'] = ensure_utc_timestamp(df.get('timestamp'))
    df['funding_rate'] = pd.to_numeric(df.get('fundingRate'), errors='coerce')
    df['mark_price'] = pd.to_numeric(df.get('markPrice'), errors='coerce')
    return df[['timestamp', 'funding_rate', 'mark_price']].dropna(subset=['timestamp']).sort_values('timestamp').drop_duplicates(subset=['timestamp'], keep='last').reset_index(drop=True)


def fetch_open_interest_hist_dataframe(symbol: str, start: str, end: str, period: str, session: requests.Session, rate_limiter: Optional[RateLimiter] = None, timeout: int = 60, limit: int = 500) -> pd.DataFrame:
    rows = _paginate_timeboxed_rows(
        url=OPEN_INTEREST_HIST_URL,
        session=session,
        start_ms=_to_ms(start),
        end_ms=_to_ms(end),
        limit=min(int(limit), 500),
        timeout=timeout,
        rate_limiter=rate_limiter,
        base_params={"symbol": symbol, "period": period},
        time_field_candidates=('timestamp', 'time'),
    )
    if not rows:
        return pd.DataFrame(columns=['timestamp', 'sum_open_interest', 'sum_open_interest_value', 'cmc_circulating_supply', 'count_toptrader_long_short_ratio'])
    df = pd.DataFrame(rows)
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(pd.to_numeric(df['timestamp'], errors='coerce'), unit='ms', utc=True)
    else:
        df['timestamp'] = ensure_utc_timestamp(df.get('time'))
    df['sum_open_interest'] = pd.to_numeric(df.get('sumOpenInterest'), errors='coerce')
    df['sum_open_interest_value'] = pd.to_numeric(df.get('sumOpenInterestValue'), errors='coerce')
    df['cmc_circulating_supply'] = pd.to_numeric(df.get('CMCCirculatingSupply'), errors='coerce')
    ratio_col = None
    for candidate in ('countToptraderLongShortRatio', 'count_toptrader_long_short_ratio'):
        if candidate in df.columns:
            ratio_col = candidate
            break
    df['count_toptrader_long_short_ratio'] = pd.to_numeric(df.get(ratio_col) if ratio_col else None, errors='coerce')
    keep_cols = ['timestamp', 'sum_open_interest', 'sum_open_interest_value', 'cmc_circulating_supply', 'count_toptrader_long_short_ratio']
    return df[keep_cols].dropna(subset=['timestamp']).sort_values('timestamp').drop_duplicates(subset=['timestamp'], keep='last').reset_index(drop=True)


def fetch_liquidations_dataframe(symbol: str, start: str, end: str, session: requests.Session, rate_limiter: Optional[RateLimiter] = None, timeout: int = 60, limit: int = 100) -> pd.DataFrame:
    rows = _paginate_timeboxed_rows(
        url=LIQUIDATIONS_URL,
        session=session,
        start_ms=_to_ms(start),
        end_ms=_to_ms(end),
        limit=min(int(limit), 100),
        timeout=timeout,
        rate_limiter=rate_limiter,
        base_params={"symbol": symbol},
        time_field_candidates=('time', 'updateTime', 'timestamp'),
    )
    if not rows:
        return pd.DataFrame(columns=['timestamp', 'liquidation_side', 'liquidation_price', 'liquidation_qty', 'liquidation_notional'])
    df = pd.DataFrame(rows)
    if 'time' in df.columns:
        df['timestamp'] = pd.to_datetime(pd.to_numeric(df['time'], errors='coerce'), unit='ms', utc=True)
    elif 'updateTime' in df.columns:
        df['timestamp'] = pd.to_datetime(pd.to_numeric(df['updateTime'], errors='coerce'), unit='ms', utc=True)
    else:
        df['timestamp'] = ensure_utc_timestamp(df.get('timestamp'))
    order_col = 'o' if 'o' in df.columns else None
    if order_col:
        df['liquidation_side'] = df[order_col].apply(lambda x: x.get('S') if isinstance(x, dict) else None)
        df['liquidation_price'] = pd.to_numeric(df[order_col].apply(lambda x: x.get('ap') if isinstance(x, dict) else None), errors='coerce')
        df['liquidation_qty'] = pd.to_numeric(df[order_col].apply(lambda x: x.get('z') if isinstance(x, dict) else None), errors='coerce')
    else:
        df['liquidation_side'] = df.get('side')
        df['liquidation_price'] = pd.to_numeric(df.get('avgPrice'), errors='coerce')
        df['liquidation_qty'] = pd.to_numeric(df.get('executedQty'), errors='coerce')
    df['liquidation_notional'] = df['liquidation_price'] * df['liquidation_qty']
    return df[['timestamp', 'liquidation_side', 'liquidation_price', 'liquidation_qty', 'liquidation_notional']].dropna(subset=['timestamp']).sort_values('timestamp').drop_duplicates(subset=['timestamp', 'liquidation_side', 'liquidation_price', 'liquidation_qty'], keep='last').reset_index(drop=True)


def export_raw_trades(df: pd.DataFrame, out_path: str | Path) -> Path:
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return path
