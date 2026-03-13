from __future__ import annotations

from dataclasses import dataclass

from binance_quant_builder.downloader import fetch_funding_rate_dataframe


@dataclass
class FakeResponse:
    payload: list[dict]

    def json(self):
        return self.payload

    def raise_for_status(self):
        return None


class FakeSession:
    def __init__(self):
        self.calls = []

    def get(self, url, timeout=60, params=None):
        self.calls.append(dict(params or {}))
        start = params['startTime']
        if start == 0:
            return FakeResponse([
                {'fundingTime': 0, 'fundingRate': '0.001', 'markPrice': '100'},
                {'fundingTime': 1, 'fundingRate': '0.002', 'markPrice': '101'},
            ])
        if start == 2:
            return FakeResponse([
                {'fundingTime': 2, 'fundingRate': '0.003', 'markPrice': '102'},
            ])
        return FakeResponse([])


def test_fetch_funding_rate_dataframe_paginates():
    session = FakeSession()
    df = fetch_funding_rate_dataframe(
        symbol='BTCUSDT',
        start='1970-01-01T00:00:00+00:00',
        end='1970-01-01T00:00:00.003+00:00',
        session=session,
        limit=2,
    )
    assert len(session.calls) == 2
    assert len(df) == 3
    assert df['funding_rate'].tolist() == [0.001, 0.002, 0.003]
