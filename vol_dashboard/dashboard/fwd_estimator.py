import datetime
import json
import math
from typing import Any

import pandas as pd
import redis
from loguru import logger

from vol_dashboard.api.deribit import DeribitAPI
from vol_dashboard.config import CURRENCY_LIST, EVENT_LIST, INSTRUMENTS, YEARLY_TRADING_DAYS
from vol_dashboard.connector.db_connector import VolDbConnector
from vol_dashboard.connector.redis_connector import get_redis_instance
from vol_dashboard.utils.event_utils import get_upcoming_events
from vol_dashboard.utils.tz_utils import et_to_utc


class FwdVolEstimator:
    def __init__(self):
        self.db_conn = VolDbConnector()
        self.rds: redis.Redis = get_redis_instance()
        self.api = DeribitAPI()
        self.all_expirations: list[datetime.date] = []
        self.est_event_vol = self.load_est_event_vol()
        if not self.est_event_vol:
            logger.info("Fail to load est event vol")

    def load_est_event_vol(self) -> dict:
        raw_data = self.rds.get(name="EstEventVol")
        if not raw_data:
            return {}
        else:
            return dict(json.loads(raw_data))

    def update_est_event_vol(self, event_name: str, currency: str, new_vol: float) -> None:
        self.est_event_vol[f"{event_name}|{currency}"] = new_vol

    def get_event_removed_iv(
        self,
        raw_iv: float,
        tte: float,
        estimated_event_vol_l: list[float],
    ) -> float:
        raw_y = (raw_iv * 100) * math.sqrt(tte) / 2000
        y_er_sq = raw_y**2 - sum(
            [
                (estimated_event_vol / math.sqrt(YEARLY_TRADING_DAYS)) ** 2
                for estimated_event_vol in estimated_event_vol_l
            ]
        )
        if y_er_sq < 0:
            y_er_sq = 0
        y_er = math.sqrt(y_er_sq)
        iv_er = (y_er * 2000 / math.sqrt(tte)) / 100
        return y_er, iv_er

    def get_atm_iv(
        self,
        currency: str,
        all_expirations: list[datetime.date],
        spot_price: float,
        upcoming_events: list[dict],
    ) -> dict[str, dict]:
        results = {}
        for expiration in all_expirations:
            expiration_dt = datetime.datetime.combine(expiration, datetime.time(hour=8, tzinfo=datetime.timezone.utc))
            underlying_price = self.api.get_underlying_price_for_expiry(currency=currency, expiry_date=expiration)
            if underlying_price:
                logger.info(f"Using option's underlying price: ${underlying_price:,.2f}")
            else:
                logger.debug(
                    "No future found for {}. Falling back to spot price for strike selection.".format(
                        expiration.isoformat()
                    )
                )
                underlying_price = spot_price

            iv_strike = self.api.find_deribit_iv(currency, expiration, underlying_price)

            events_included = []
            event_vol_included: list[float] = []
            for event in upcoming_events:
                if event["utc_dt"] < expiration_dt:
                    est_event_vol = self.est_event_vol.get("{}|{}".format(event["event_name"], currency), 0)
                    events_included.append(
                        {
                            "utc_dt": event["utc_dt"].isoformat(),
                            "event_id": event["event_id"],
                            "event_name": event["event_name"],
                            "est_event_vol": est_event_vol,
                        }
                    )
                    event_vol_included.append(est_event_vol)

            iv_strike["events_included"] = events_included
            _, iv_strike["implied_vol_er"] = self.get_event_removed_iv(
                raw_iv=iv_strike["implied_vol"],
                tte=iv_strike["tte"],
                estimated_event_vol_l=event_vol_included,
            )
            iv_strike["update_dt"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            results[expiration.strftime("%-d%b%y").upper()] = iv_strike

        logger.info("Save ATM IV to redis")
        self.rds.set(name=f"ATM_IV:{currency}", value=json.dumps(results))
        return results

    def update_all_expirations(self):
        # fetch expiration dates
        logger.info("Fetching available option expiration dates...")
        all_expirations = self.api.get_deribit_option_expirations("BTC")
        if not all_expirations:
            logger.error("Could not fetch expiration dates. Exiting.")
            return []
        all_expirations = sorted(all_expirations)
        return all_expirations

    def match_expiry_pairs(self, all_expirations: list[datetime.date]) -> list[tuple]:
        expiry_pairs = []
        for i in range(len(all_expirations) - 1):
            _dt_1 = datetime.datetime.combine(
                all_expirations[i],
                datetime.time(hour=8, tzinfo=datetime.timezone.utc),
            )
            _dt_2 = datetime.datetime.combine(
                all_expirations[i + 1],
                datetime.time(hour=8, tzinfo=datetime.timezone.utc),
            )
            expiry_dt_pair = (_dt_1, _dt_2)
            expiry_pairs.append(expiry_dt_pair)
        return expiry_pairs

    def update_upcoming_event_vol(self) -> dict[str, dict[str, Any]]:
        # load upcoming events
        logger.info("Fetching upcoming events...")
        raw_upcoming_events = get_upcoming_events()
        upcoming_events = []
        for event_name, date_str, time_et_str in raw_upcoming_events:
            naive_et_dt = datetime.datetime.strptime(f"{date_str} {time_et_str}", "%Y-%m-%d %H:%M")
            _, utc_dt = et_to_utc(naive_et_dt)
            event_id = f"{event_name}/{date_str}/{time_et_str}"
            upcoming_events.append({"utc_dt": utc_dt, "event_id": event_id, "event_name": event_name})

        # fetch expiration dates
        self.all_expirations = all_expirations = self.update_all_expirations()

        logger.info(
            "All option expirations: {}".format(
                " ".join([expiration.isoformat() for expiration in all_expirations]),
            )
        )

        upcoming_event_vol = {}
        for currency in CURRENCY_LIST:
            logger.info(f"Fetching current {currency} index (spot) price for fallback...")
            spot_price = self.api.get_index_price(currency)
            if spot_price is None:
                logger.error(f"Could not fetch {currency} spot price. Exiting.")
                continue
            logger.info(f"Current {currency} Spot Price: ${spot_price:,.2f}")

            logger.info("Analyzing ATM IV and event removed ATM IV")
            atm_iv_results = self.get_atm_iv(
                currency,
                all_expirations,
                spot_price,
                upcoming_events,
            )

            logger.info("Analyzing fwd vol and event removed fwd vol")
            fwd_vol_results: dict[str, dict] = {}
            expiry_pairs = self.match_expiry_pairs(all_expirations)
            # from now to the first expiry
            first_expiry: datetime.datetime = datetime.datetime.combine(
                all_expirations[0],
                datetime.time(hour=8, tzinfo=datetime.timezone.utc),
            )
            first_expiry_key: str = first_expiry.strftime("%-d%b%y").upper()
            atm_iv_first = atm_iv_results[first_expiry_key]
            fwd_vol_results[first_expiry.date().strftime("%-d%b%y").upper()] = {
                "prev_option": "NOW",
                "prev_iv": 0,
                "prev_iv_er": 0,
                "next_option": atm_iv_first["instrument_name"],
                "next_iv": atm_iv_first["implied_vol"],
                "next_iv_er": atm_iv_first["implied_vol_er"],
                "col_id": first_expiry.date().strftime("%-d%b%y").upper(),
                "currency": currency,
                "fwd_vol": atm_iv_first["implied_vol"],
                "fwd_vol_er": atm_iv_first["implied_vol_er"],
                "update_dt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }

            # after the first expiry
            for expiry_pair in expiry_pairs:
                prev_expiry: datetime.datetime = expiry_pair[0]
                prev_expiry_key: str = prev_expiry.strftime("%-d%b%y").upper()
                next_expiry: datetime.datetime = expiry_pair[1]
                next_expiry_key: str = next_expiry.strftime("%-d%b%y").upper()

                logger.info(f"Processing fwd vol between {prev_expiry_key} and {next_expiry_key}")
                atm_iv_prev = atm_iv_results[prev_expiry_key]
                atm_iv_next = atm_iv_results[next_expiry_key]

                forward_vol = math.sqrt(
                    (
                        atm_iv_next["tte"] * atm_iv_next["implied_vol"] ** 2
                        - atm_iv_prev["tte"] * atm_iv_prev["implied_vol"] ** 2
                    )
                    / (atm_iv_next["tte"] - atm_iv_prev["tte"])
                )
                forward_vol_er = math.sqrt(
                    max(
                        (
                            atm_iv_next["tte"] * atm_iv_next["implied_vol_er"] ** 2
                            - atm_iv_prev["tte"] * atm_iv_prev["implied_vol_er"] ** 2
                        ),
                        0,
                    )
                    / (atm_iv_next["tte"] - atm_iv_prev["tte"])
                )
                fwd_vol_results[next_expiry.date().strftime("%-d%b%y").upper()] = {
                    "prev_option": atm_iv_prev["instrument_name"],
                    "prev_iv": atm_iv_prev["implied_vol"],
                    "prev_iv_er": atm_iv_prev["implied_vol_er"],
                    "next_option": atm_iv_next["instrument_name"],
                    "next_iv": atm_iv_next["implied_vol"],
                    "next_iv_er": atm_iv_next["implied_vol_er"],
                    "currency": currency,
                    "fwd_vol": forward_vol,
                    "fwd_vol_er": forward_vol_er,
                    "update_dt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                }

            logger.info("Save FWD VOL to redis")
            self.rds.set(name=f"FWD_VOL:{currency}", value=json.dumps(fwd_vol_results))

            upcoming_event_vol[currency] = {
                "atm_iv": atm_iv_results,
                "fwd_vol": fwd_vol_results,
            }

        return upcoming_event_vol

    def prepare_vol_data(self) -> pd.DataFrame:
        expiry_s = list(map(lambda dt: dt.strftime("%-d%b%y").upper(), self.all_expirations))
        columns = ["Currency"] + expiry_s
        upcoming_event_vol = self.update_upcoming_event_vol()

        atm_iv_rows_l = []
        fwd_vol_rows_l = []
        for currency in CURRENCY_LIST:
            # ATM IV & ATM IV (ER)
            atm_iv_row = {}
            atm_iv_er_row = {}
            atm_iv_row["Currency"] = currency
            atm_iv_er_row["Currency"] = f"{currency} (ER)"
            for col_id, atm_iv_record in upcoming_event_vol["atm_iv"].items():
                atm_iv_row[col_id] = atm_iv_record["implied_vol"]
                atm_iv_er_row[col_id] = atm_iv_record["implied_vol_er"]
            atm_iv_rows_l.append(atm_iv_row)
            atm_iv_rows_l.append(atm_iv_er_row)

            # Fwd Vol & Fwd Vol (ER)
            fwd_vol_row = {}
            fwd_vol_er_row = {}
            fwd_vol_row["Currency"] = currency
            fwd_vol_er_row["Currency"] = f"{currency} (ER)"
            for col_id, fwd_vol_record in upcoming_event_vol["fwd_vol"].items():
                fwd_vol_row[col_id] = fwd_vol_record["fwd_vol"]
                fwd_vol_er_row[col_id] = fwd_vol_record["fwd_vol_er"]
            fwd_vol_rows_l.append(fwd_vol_row)
            fwd_vol_rows_l.append(fwd_vol_er_row)

        atm_iv_df = pd.DataFrame(atm_iv_rows_l, columns=columns)
        fwd_vol_df = pd.DataFrame(fwd_vol_rows_l, columns=columns)
        for expiry in expiry_s:
            atm_iv_df[expiry] = atm_iv_df[expiry].apply(lambda x: f"{x:.4f}")
            fwd_vol_df[expiry] = fwd_vol_df[expiry].apply(lambda x: f"{x:.4f}")
        return atm_iv_df, fwd_vol_df


if __name__ == "__main__":
    estimator = FwdVolEstimator()
    try:
        estimator.update_upcoming_event_vol()
    except KeyboardInterrupt:
        pass
