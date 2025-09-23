import datetime
import json
import math
from pprint import pformat, pprint

import pandas as pd
from api_helper import DeribitAPI
from config import INSTRUMENTS
from db_connector import VolDbConnector
from event_utils import get_upcoming_events
from loguru import logger
from redis_connector import get_redis_instance
from tz_utils import et_to_utc

api_helper = DeribitAPI()
DB_CONN = VolDbConnector()


def get_underlying_price_for_expiries(currency: str, expiry_l: list[datetime.datetime], spot_price: float):
    ret = []
    for expiry in expiry_l:
        underlying_price = api_helper.get_underlying_price_for_expiry(currency, expiry.date())
        if underlying_price:
            logger.info(f"Using option's underlying price: ${underlying_price:,.2f}")
            ret.append(underlying_price)
        else:
            logger.debug(
                "No future found for {}. Falling back to spot price for strike selection.".format(
                    expiry.date().isoformat()
                )
            )
            ret.append(spot_price)
    return ret


def match_events_to_expiry(upcoming_events: list[dict], all_expirations: list[datetime.date]):
    all_expirations = sorted(all_expirations)
    matched_event_expiry = []
    for i in range(len(all_expirations) - 1):
        _dt_1 = datetime.datetime.combine(all_expirations[i], datetime.time(hour=8, tzinfo=datetime.timezone.utc))
        _dt_2 = datetime.datetime.combine(all_expirations[i + 1], datetime.time(hour=8, tzinfo=datetime.timezone.utc))
        expiry_dt_pair = (_dt_1, _dt_2)
        matched_events = []
        for event in upcoming_events:
            if _dt_1 <= event["utc_dt"] < _dt_2:
                matched_events.append(event)
        matched_event_expiry.append(
            {
                "expiry_dt_pair": expiry_dt_pair,
                "matched_events": matched_events,
            }
        )
    return matched_event_expiry


def get_event_removed_iv(raw_iv: float, tte: float, estimated_event_vol: list[float]):
    pass


def update_upcoming_event_vol_by_currency(
    currency: str,
    matched_event_expiry: list[dict],
    avg_historical_vol: dict[tuple, float],
):
    currency = currency.upper()
    logger.info(f"Fetching current {currency} index (spot) price for fallback...")
    spot_price = api_helper.get_index_price(currency)
    if spot_price is None:
        logger.error(f"Could not fetch {currency} spot price. Exiting.")
        return
    logger.info(f"Current {currency} Spot Price: ${spot_price:,.2f}")

    results = []
    logger.info("Processing macroeconomic events...")
    for event_expiry in matched_event_expiry:
        prev_expiry: datetime.datetime = event_expiry["expiry_dt_pair"][0]
        next_expiry: datetime.datetime = event_expiry["expiry_dt_pair"][1]

        logger.info("{sep} 📅 {sep} ".format(sep="=" * 12))
        logger.info(f"Processing expiry {prev_expiry.date().isoformat()} and {next_expiry.date().isoformat()}")
        logger.info(
            "Processing events {}".format(",".join([event["event_id"] for event in event_expiry["matched_events"]]))
        )
        logger.info("{sep} ".format(sep="=" * (12 * 2 + 4)))

        underlying_price_prev, underlying_price_next = get_underlying_price_for_expiries(
            currency, [prev_expiry, next_expiry], spot_price
        )

        iv_strike_prev = api_helper.find_deribit_iv(currency, prev_expiry.date(), underlying_price_prev)
        iv_strike_next = api_helper.find_deribit_iv(currency, next_expiry.date(), underlying_price_next)

        if (
            iv_strike_prev
            and iv_strike_prev["implied_vol"] is not None
            and iv_strike_next
            and iv_strike_next["implied_vol"] is not None
        ):
            logger.info(f"Found IV (prev) and IV (next)")
            fetch_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

            # get fwd vol with events from iv_prev and iv_next
            forward_vol = math.sqrt(
                (
                    iv_strike_next["tte"] * iv_strike_next["implied_vol"] ** 2
                    - iv_strike_prev["tte"] * iv_strike_prev["implied_vol"] ** 2
                )
                / (iv_strike_next["tte"] - iv_strike_prev["tte"])
            )

            # get event-removed implied vol for expiry_next
            estimated_event_vol_l = []
            for events in event_expiry["matched_events"]:
                estimated_event_vol_l.append(avg_historical_vol[(events["event_name"], currency)])

            # Event-removed Y
            y_pct_er = iv_strike_next["implied_vol"] ** 2 * iv_strike_next["tte"] / 2000 - sum(
                [estimated_event_vol**2 for estimated_event_vol in estimated_event_vol_l]
            )
            iv_er = y_pct_er * 2000 / math.sqrt(iv_strike_next["tte"])

            # Event removed fwd vol
            forward_vol_er = math.sqrt(
                (iv_strike_next["tte"] * iv_er**2 - iv_strike_prev["tte"] * iv_strike_prev["implied_vol"] ** 2)
                / (iv_strike_next["tte"] - iv_strike_prev["tte"])
            )

            results.append(
                {
                    "Events_Included": [
                        {
                            "utc_dt": events["utc_dt"].isoformat(),
                            "event_id": events["event_id"],
                            "event_name": events["event_name"],
                        }
                        for events in event_expiry["matched_events"]
                    ],
                    "Prev_Option": iv_strike_prev["instrument_name"],
                    "Prev_IV": iv_strike_prev["implied_vol"],
                    "Prev_TTE": iv_strike_prev["tte"],
                    "Next_Option": iv_strike_next["instrument_name"],
                    "Next_IV": iv_strike_next["implied_vol"],
                    "Next_TTE": iv_strike_next["tte"],
                    "Col_ID": next_expiry.date().strftime("%-d%b%y").upper(),
                    "Currency": currency,
                    "Fwd_Vol": forward_vol,
                    "Fwd_Vol_ER": forward_vol_er,
                    "Data_Fetch_Timestamp": fetch_timestamp,
                }
            )
        else:
            logger.error(f"Fail to fetch IV")
            return

    if not results:
        logger.error("No data was successfully fetched.")
        return

    redis_instance = get_redis_instance()
    redis_instance.set(name=f"FwdVol:{currency}", value=json.dumps(results))
    logger.info("Save to redis success")


def estimate_event_vol() -> dict[tuple, float]:
    """
    Returns:
        return value example:
        {('CPI', 'BTC'): 0.48650858129208474,
        ('CPI', 'ETH'): 0.8956060885913556,
        ('FOMC', 'BTC'): 0.8341024650036615,
        ('FOMC', 'ETH'): 1.2593448799387723,
        ('NFP', 'BTC'): 0.6719360519847367,
        ('NFP', 'ETH'): 0.8433502237656211,
        ('PPI', 'BTC'): 0.20108337643044344,
        ('PPI', 'ETH'): 0.3779159578296879}
    """
    previous_vol_data = DB_CONN.get_event_vols()
    previous_vol_df = pd.DataFrame(
        previous_vol_data,
        columns=[
            "ID",
            "Event Name",
            "Symbol",
            "UTC Time",
            "Vol Before",
            "Vol After",
            "Event Vol",
        ],
    )
    previous_vol_df = previous_vol_df[previous_vol_df["Event Vol"] > 0]
    previous_vol_df["Currency"] = previous_vol_df["Symbol"].apply(lambda x: x.replace("-PERPETUAL", ""))
    previous_vol_df = previous_vol_df[["Event Name", "Currency", "Event Vol"]]
    avg_historical_vol = previous_vol_df.groupby(["Event Name", "Currency"]).mean()
    return avg_historical_vol.to_dict()


def update_upcoming_event_vol():
    logger.info("Fetching upcoming events...")
    raw_upcoming_events = get_upcoming_events()
    upcoming_events = []
    for event_name, date_str, time_et_str in raw_upcoming_events:
        naive_et_dt = datetime.datetime.strptime(f"{date_str} {time_et_str}", "%Y-%m-%d %H:%M")
        _, utc_dt = et_to_utc(naive_et_dt)
        event_id = f"{event_name}/{date_str}/{time_et_str}"
        upcoming_events.append({"utc_dt": utc_dt, "event_id": event_id, "event_name": event_name})

    logger.info("Fetching available option expiration dates...")
    all_expirations = api_helper.get_deribit_option_expirations("BTC")
    if not all_expirations:
        logger.error("Could not fetch expiration dates. Exiting.")
        return
    logger.info(
        "All option expirations: {}".format(
            " ".join([expiration.isoformat() for expiration in all_expirations]),
        )
    )
    redis_instance = get_redis_instance()
    redis_instance.set(
        name=f"Expirations",
        value=json.dumps([expiration.strftime("%-d%b%y").upper() for expiration in all_expirations]),
    )
    matched_event_expiry = match_events_to_expiry(upcoming_events, all_expirations)
    # logger.info(f"matched_event_expiry: {matched_event_expiry}")
    avg_historical_vol = estimate_event_vol()

    for inst in INSTRUMENTS:
        currency = inst.replace("-PERPETUAL", "")
        update_upcoming_event_vol_by_currency(currency, matched_event_expiry, avg_historical_vol)


if __name__ == "__main__":
    try:
        update_upcoming_event_vol()
    except KeyboardInterrupt:
        pass
