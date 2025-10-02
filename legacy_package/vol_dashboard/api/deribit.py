import os
from datetime import date, datetime, time, timezone

import requests
from loguru import logger


class DeribitAPI:
    API_URL = "https://www.deribit.com/api/v2/public/"

    # --- API Helper Function ---
    def make_api_request(self, endpoint, params=None):
        """Helper function to make a GET request to the Deribit API."""
        try:
            if http_proxy_config := os.getenv("http_proxy"):
                proxies = {"http_proxy": http_proxy_config}
            else:
                proxies = None
            response = requests.get(self.API_URL + endpoint, params=params, proxies=proxies)
            response.raise_for_status()
            data = response.json()
            if "error" in data:
                if data["error"]["message"] != "instrument_not_found":
                    logger.debug(f"API Error in '{endpoint}': {data['error']['message']}")
                return None
            return data.get("result")
        except requests.exceptions.RequestException as e:
            logger.debug(f"HTTP Request Error for '{endpoint}': {e}")
            return None

    def get_deribit_option_expirations(
        self,
        currency: str,  # BTC/ETH
    ) -> list[date]:
        """Fetches all active BTC option expiration dates from Deribit."""
        params = {"currency": currency, "kind": "option", "expired": "false"}
        instruments = self.make_api_request("get_instruments", params=params)
        if not instruments:
            return []
        timestamps = set(inst["expiration_timestamp"] for inst in instruments)
        return sorted([datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date() for ts in timestamps])

    def get_index_price(
        self,
        currency: str,  # BTC/ETH
    ):
        """Fetches the current index (spot) price from Deribit."""
        params = {"index_name": f"{currency.lower()}_usd"}
        result = self.make_api_request(
            "get_index_price",
            params=params,
        )
        return result.get("index_price") if result else None

    def get_underlying_price_for_expiry(
        self,
        currency: str,
        expiry_date: date,
    ):
        """
        Fetches the underlying price for a specific expiry from its
        corresponding futures contract.
        """
        future_instrument_name = f"{currency.upper()}-{expiry_date.strftime('%-d%b%y').upper()}"
        ticker_data = self.make_api_request(
            "ticker",
            params={"instrument_name": future_instrument_name},
        )
        return ticker_data.get("mark_price") if ticker_data else None

    def get_option_implied_vol(self, instrument_name):
        """Fetches the implied volatility for a specific option instrument."""
        ticker_data = self.make_api_request(
            "ticker",
            params={"instrument_name": instrument_name},
        )
        if not ticker_data:
            return None
        mark_iv = ticker_data.get("mark_iv")
        return float(mark_iv) / 100.0 if mark_iv is not None else None

    def find_closest_call_strike(
        self,
        currency: str,
        target_expiry_date: date,
        underlying_price: float,
    ) -> float | None:
        """Finds the closest available strike for a CALL option given a specific underlying price."""
        params = {"currency": currency.upper(), "kind": "option"}
        all_instruments = self.make_api_request("get_instruments", params=params)

        if not all_instruments:
            return None

        call_options = [
            inst
            for inst in all_instruments
            if inst["instrument_name"].endswith("-C")
            and target_expiry_date
            == datetime.fromtimestamp(inst["expiration_timestamp"] / 1000, tz=timezone.utc).date()
        ]
        if not call_options:
            return None

        return min([inst["strike"] for inst in call_options], key=lambda strike: abs(strike - underlying_price))

    def find_deribit_iv(
        self,
        currency: str,
        expiry: date,
        underlying_price: float,
    ):
        # Find the closest listed CALL strike based on the correct underlying price
        atm_strike = self.find_closest_call_strike(currency, expiry, underlying_price)

        if atm_strike is None:
            logger.warning(f"Could not find any listed CALL strikes for expiry {expiry}")
            return {}
        else:
            instrument_name = f"{currency.upper()}-{expiry.strftime('%-d%b%y').upper()}-{int(atm_strike)}-C"
            logger.info(f"Found ATM CALL {instrument_name} for expiry {expiry}")
            implied_vol = self.get_option_implied_vol(instrument_name)
            expiry_time = datetime.combine(date=expiry, time=time(8, tzinfo=timezone.utc))
            tte = (expiry_time.timestamp() - datetime.now(tz=timezone.utc).timestamp()) / (365.25 * 24 * 60 * 60)
            return {
                "implied_vol": implied_vol,
                "instrument_name": instrument_name,
                "atm_strike": atm_strike,
                "underlying_price": underlying_price,
                "tte": tte,
            }


if __name__ == "__main__":
    api = DeribitAPI()
    print(api.get_index_price("BTC"))
    print(api.get_deribit_option_expirations("BTC"))
    print(api.get_underlying_price_for_expiry("BTC", date(2025, 12, 26)))
    print(api.get_option_implied_vol("BTC-26DEC25-40000-C"))
    print(api.find_closest_call_strike("BTC", date(2025, 12, 26), 145200))
    print(
        api.find_deribit_iv(
            "BTC", date(2025, 12, 26), underlying_price=145200, event_name="FOMC", release_date_str="UNK"
        )
    )
