-- Top 5 coins yesterday
with coins as (
	-- convert date column
	select date, symbol_name, close
	from binance_bars
),

coins_previous_close as (
	select *, 
		LAG(close) OVER (PARTITION BY symbol_name ORDER BY date) AS prev_close
	from coins
)

select *, round(cast((close - prev_close) / close * 100 as numeric), 2) as percent_change
from coins_previous_close
where date = CURRENT_DATE - 1
order by percent_change desc
limit 5