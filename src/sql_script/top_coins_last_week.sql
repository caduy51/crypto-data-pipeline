-- Top 5 coins last week
with coins as (
	-- convert date column
	select date, symbol_name, close
	from binance_bars
),

coins_last_week as (
	select *, 
		LAG(close, 7) OVER (PARTITION BY symbol_name ORDER BY date) AS prev_close_7_days
	from coins
)

select *, round(cast((close - prev_close_7_days) / close * 100 as numeric), 2) as percent_change
from coins_last_week
where date = CURRENT_DATE - 1
order by percent_change desc
limit 5



