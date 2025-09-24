import datetime
import json
import math

import pandas as pd
from config import INSTRUMENTS, YEARLY_TRADING_DAYS
from loguru import logger

from vol_dashboard.api.deribit import DeribitAPI
from vol_dashboard.connector.db_connector import VolDbConnector
from vol_dashboard.connector.redis_connector import get_redis_instance
from vol_dashboard.utils.event_utils import get_upcoming_events
from vol_dashboard.utils.tz_utils import et_to_utc

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
        exp1_events = []
        exp2_events = []
        matched_events = []
        for event in upcoming_events:
            if event["utc_dt"] < _dt_2:
                exp2_events.append(event)
            if event["utc_dt"] < _dt_1:
                exp1_events.append(event)
            if _dt_1 <= event["utc_dt"] < _dt_2:
                matched_events.append(event)
        matched_event_expiry.append(
            {
                "expiry_dt_pair": expiry_dt_pair,
                "exp1_events": exp1_events,
                "exp2_events": exp2_events,
                "matched_events": matched_events,
            }
        )
    return matched_event_expiry


def get_event_removed_iv(raw_iv: float, tte: float, estimated_event_vol_l: list[float]) -> float:
    raw_y = (raw_iv * 100) * math.sqrt(tte) / 2000
    y_er_sq = raw_y**2 - sum(
        [(estimated_event_vol / math.sqrt(YEARLY_TRADING_DAYS)) ** 2 for estimated_event_vol in estimated_event_vol_l]
    )
    if y_er_sq < 0:
        y_er_sq = 0
    y_er = math.sqrt(y_er_sq)
    iv_er = (y_er * 2000 / math.sqrt(tte)) / 100
    return y_er, iv_er


def update_upcoming_event_vol_by_currency(
    currency: str,
    matched_event_expiry: list[dict],
    est_historical_vol: dict[tuple, float],
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

            # get event-removed implied vol for expiry_prev and expiry_next
            exp_p_events_vol_l = [
                est_historical_vol[(events["event_name"], currency)] for events in event_expiry["exp1_events"]
            ]
            exp_n_events_vol_l = [
                est_historical_vol[(events["event_name"], currency)] for events in event_expiry["exp2_events"]
            ]
            _, iv_strike_prev["implied_vol_er"] = get_event_removed_iv(
                raw_iv=iv_strike_prev["implied_vol"],
                tte=iv_strike_prev["tte"],
                estimated_event_vol_l=exp_p_events_vol_l,
            )
            _, iv_strike_next["implied_vol_er"] = get_event_removed_iv(
                raw_iv=iv_strike_next["implied_vol"],
                tte=iv_strike_next["tte"],
                estimated_event_vol_l=exp_n_events_vol_l,
            )
            # Event removed fwd vol
            forward_vol_er = math.sqrt(
                (
                    iv_strike_next["tte"] * iv_strike_next["implied_vol_er"] ** 2
                    - iv_strike_prev["tte"] * iv_strike_prev["implied_vol_er"] ** 2
                )
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
                    "Prev_IV_ER": iv_strike_prev["implied_vol_er"],
                    "Prev_TTE": iv_strike_prev["tte"],
                    "Prev_Events": [
                        {
                            "utc_dt": events["utc_dt"].isoformat(),
                            "event_id": events["event_id"],
                            "event_name": events["event_name"],
                        }
                        for events in event_expiry["exp1_events"]
                    ],
                    "Next_Option": iv_strike_next["instrument_name"],
                    "Next_IV": iv_strike_next["implied_vol"],
                    "Next_IV_ER": iv_strike_next["implied_vol_er"],
                    "Next_TTE": iv_strike_next["tte"],
                    "Next_Events": [
                        {
                            "utc_dt": events["utc_dt"].isoformat(),
                            "event_id": events["event_id"],
                            "event_name": events["event_name"],
                        }
                        for events in event_expiry["exp2_events"]
                    ],
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


def update_atm_iv_by_currency(
    currency: str,
    all_expirations: list[datetime.date],
    spot_price: float,
    est_historical_vol: dict[tuple, float],
    upcoming_events: list[dict],
):
    results = {}
    all_expirations = sorted(all_expirations)
    for expiration in all_expirations:
        expiration_dt = datetime.datetime.combine(expiration, datetime.time(hour=8, tzinfo=datetime.timezone.utc))
        underlying_price = api_helper.get_underlying_price_for_expiry(currency, expiration)
        if underlying_price:
            logger.info(f"Using option's underlying price: ${underlying_price:,.2f}")
        else:
            logger.debug(
                "No future found for {}. Falling back to spot price for strike selection.".format(
                    expiration.isoformat()
                )
            )
            underlying_price = spot_price

        iv_strike = api_helper.find_deribit_iv(currency, expiration, underlying_price)

        events_included = []
        event_vol_included: list[float] = []
        for event in upcoming_events:
            if event["utc_dt"] < expiration_dt:
                events_included.append(
                    {
                        "utc_dt": event["utc_dt"].isoformat(),
                        "event_id": event["event_id"],
                        "event_name": event["event_name"],
                        "est_event_vol": est_historical_vol[(event["event_name"], currency)],
                    }
                )
                event_vol_included.append(est_historical_vol[(event["event_name"], currency)])

        iv_strike["events_included"] = events_included
        _, iv_strike["implied_vol_er"] = get_event_removed_iv(
            raw_iv=iv_strike["implied_vol"],
            tte=iv_strike["tte"],
            estimated_event_vol_l=event_vol_included,
        )

        results[expiration.strftime("%-d%b%y").upper()] = iv_strike

    redis_instance = get_redis_instance()
    redis_instance.set(name=f"ATM_IV:{currency}", value=json.dumps(results))
    logger.info("Save ATM IV to redis success")
    return results


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
    est_historical_vol = previous_vol_df.groupby(["Event Name", "Currency"]).last()
    return est_historical_vol.to_dict()["Event Vol"]


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
    all_expirations = sorted(all_expirations)
    logger.info(
        "All option expirations: {}".format(
            " ".join([expiration.isoformat() for expiration in all_expirations]),
        )
    )

    matched_event_expiry = match_events_to_expiry(upcoming_events, all_expirations)
    # logger.info(f"matched_event_expiry: {matched_event_expiry}")
    est_historical_vol = estimate_event_vol()
    redis_instance = get_redis_instance()
    redis_instance.set(name=f"EstHistVol", value=json.dumps(est_historical_vol))

    for inst in INSTRUMENTS:
        currency = inst.replace("-PERPETUAL", "").upper()
        logger.info(f"Fetching current {currency} index (spot) price for fallback...")
        spot_price = api_helper.get_index_price(currency)
        if spot_price is None:
            logger.error(f"Could not fetch {currency} spot price. Exiting.")
            continue
        logger.info(f"Current {currency} Spot Price: ${spot_price:,.2f}")

        atm_iv_results = update_atm_iv_by_currency(
            currency,
            all_expirations,
            spot_price,
            est_historical_vol,
            upcoming_events,
        )
        # update_upcoming_event_vol_by_currency(currency, matched_event_expiry, est_historical_vol)


if __name__ == "__main__":
    try:
        update_upcoming_event_vol()
    except KeyboardInterrupt:
        pass
