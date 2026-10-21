from unittest.mock import Mock

import pytest
from curl_cffi.requests.exceptions import HTTPError

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


@pytest.mark.parametrize("extract", [
    stock_prices.get_hourly_stock_data, stock_prices.get_company_info,
])
def test_yahoo_authorization_error_is_not_hidden(monkeypatch, extract):
    ticker = stock_prices.yf.Ticker("WMT")
    response = Mock(text='{"finance":{"error":{"description":"Invalid Crumb"}}}')
    fetch = Mock(side_effect=HTTPError("HTTP Error 401: Invalid Crumb", response=response))
    monkeypatch.setattr(ticker._data, "get_raw_json", fetch)
    monkeypatch.setattr(ticker._quote, "_fetch_additional_info", lambda: {
        "quoteResponse": {"result": [{"symbol": "WMT", "regularMarketPrice": 100}]}
    })
    monkeypatch.setattr(ticker._quote, "_fetch_complementary", lambda: None)
    monkeypatch.setattr(stock_prices.yf, "Ticker", lambda _: ticker)
    monkeypatch.setattr(stock_prices.yf.config.debug, "hide_exceptions", True)

    result = extract("WMT")

    assert result["error"] == "Lookup Error"
    assert "401" in result["message"]


def test_all_failed_lookups_fail_the_extraction(monkeypatch):
    def fail(_):
        raise RuntimeError("Yahoo denied access: HTTP 401")

    monkeypatch.setattr(stock_prices.yf, "Ticker", fail)

    with pytest.raises(RuntimeError, match="All 2 stock lookups failed"):
        stock_prices.extract_all(["WMT", "XEL"])


@pytest.mark.parametrize("info", [None, {}])
def test_empty_yahoo_response_is_a_lookup_error(monkeypatch, info):
    monkeypatch.setattr(stock_prices.yf, "Ticker", lambda _: Mock(info=info))

    result = stock_prices.get_hourly_stock_data("WMT")

    assert result["error"] == "Lookup Error"
    assert result["message"] == "Yahoo returned no stock data for WMT"


def test_partial_lookup_failure_preserves_valid_data(monkeypatch):
    def ticker(symbol):
        if symbol == "WMT":
            raise RuntimeError("Yahoo denied access: HTTP 401")
        return Mock(info={"currentPrice": 100, "longName": "Xcel Energy"})

    fetch = Mock(side_effect=ticker)
    monkeypatch.setattr(stock_prices.yf, "Ticker", fetch)

    hourly, company = stock_prices.extract_all(["WMT", "XEL"])

    assert hourly[0]["error"] == company[0]["error"] == "Lookup Error"
    assert hourly[1]["current_price"] == 100
    assert company[1]["company_name"] == "Xcel Energy"
    assert fetch.call_count == 2
