select dt as date,
  symbol as symbol,
  rv_raw as rv_raw,
  rv_er as rv_er,
  -- er_duration,
  event_id
from public.daily_rv
where exchange = 'DERIBIT'
  and $__timeFilter(dt)
order by date asc
