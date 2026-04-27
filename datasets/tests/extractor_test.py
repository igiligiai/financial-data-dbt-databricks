import datasets.stock_prices as stock_prices
from datasets.stock_prices import _safe_round, _timestamp_iso, get_hourly_stock_data


def test_safe_round_rounds_numeric_string_to_default_decimals():
    assert _safe_round("123.456") == 123.46


def test_safe_round_uses_custom_decimals():
    assert _safe_round(10.98765, 3) == 10.988


def test_safe_round_returns_none_for_non_numeric_input():
    assert _safe_round("not-a-number") is None


def test_timestamp_iso_converts_epoch_to_utc_iso8601():
    assert _timestamp_iso(0) == "1970-01-01T00:00:00+00:00"


def test_timestamp_iso_returns_none_for_non_numeric_input():
    assert _timestamp_iso("1700000000") is None


def test_get_hourly_stock_data_returns_error_for_empty_symbol():
    result = get_hourly_stock_data("   ")
    assert result == {"error": "Invalid Symbol", "message": "Symbol cannot be empty"}


def test_get_hourly_stock_data_normalizes_symbol_and_rounds_fields(monkeypatch):
    class DummyTicker:
        info = {
            "currentPrice": "123.456",
            "dayHigh": "130.994",
            "dayLow": "120.001",
            "previousClose": 121.126,
            "volume": 1000,
            "marketCap": 5000000,
            "fiftyDayAverage": 118.555,
            "twoHundredDayAverage": 110.444,
            "fiftyTwoWeekHigh": 199.999,
            "fiftyTwoWeekLow": 90.001,
        }

    monkeypatch.setattr(stock_prices.yf, "Ticker", lambda _: DummyTicker())

    result = get_hourly_stock_data(" aapl ")
    assert result["symbol"] == "AAPL"
    assert result["current_price"] == 123.46
    assert result["day_high"] == 130.99
    assert result["day_low"] == 120.0
    assert result["previous_close"] == 121.13
    assert result["volume"] == 1000
    assert result["market_cap"] == 5000000


def test_get_hourly_stock_data_uses_regular_market_fallback_fields(monkeypatch):
    class DummyTicker:
        info = {
            "regularMarketDayHigh": 201.1,
            "regularMarketDayLow": 180.2,
            "regularMarketPreviousClose": 190.3,
            "regularMarketVolume": 999,
        }

    monkeypatch.setattr(stock_prices.yf, "Ticker", lambda _: DummyTicker())

    result = get_hourly_stock_data("MSFT")
    assert result["day_high"] == 201.1
    assert result["day_low"] == 180.2
    assert result["previous_close"] == 190.3
    assert result["volume"] == 999


def test_get_hourly_stock_data_returns_lookup_error_on_ticker_failure(monkeypatch):
    def _raise(_):
        raise RuntimeError("ticker failed")

    monkeypatch.setattr(stock_prices.yf, "Ticker", _raise)
    result = get_hourly_stock_data("TSLA")

    assert result["error"] == "Lookup Error"
    assert result["symbol"] == "TSLA"
    assert "ticker failed" in result["message"]
