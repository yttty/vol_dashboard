import numpy as np

from vol_dashboard.config import YEARLY_TRADING_DAYS


def calculate_realized_volatility(prices_by_min: np.array) -> float:
    """Calculates annualized realized volatility from 1-minute price data."""
    if len(prices_by_min) < 2:
        raise ValueError
    log_returns = np.diff(np.log(prices_by_min))
    realized_variance = np.sum(log_returns**2)
    minutes_in_year = YEARLY_TRADING_DAYS * 24 * 60
    annualized_volatility = np.sqrt(realized_variance) * np.sqrt(minutes_in_year / len(prices_by_min))
    return float(annualized_volatility)
