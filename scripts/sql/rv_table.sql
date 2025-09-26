select
  btc_rv.dt as date,
  btc_rv.rv_raw as rv_btc,
  btc_rv.rv_er as rv_er_btc,
  eth_rv.rv_raw as rv_eth,
  eth_rv.rv_er as rv_er_eth,
  btc_rv.event_id as event
from
  (
    SELECT
      dt,
      rv_raw,
      rv_er,
      event_id
    FROM
      public.daily_rv
    where
      exchange = 'DERIBIT'
      and symbol = 'BTC-PERPETUAL'
      and $__timeFilter(dt)
    order by
      dt asc
  ) btc_rv full
  outer join (
    SELECT
      dt,
      rv_raw,
      rv_er
    FROM
      public.daily_rv
    where
      exchange = 'DERIBIT'
      and symbol = 'ETH-PERPETUAL'
      and $__timeFilter(dt)
    order by
      dt asc
  ) eth_rv on eth_rv.dt = btc_rv.dt
order by
  date desc;