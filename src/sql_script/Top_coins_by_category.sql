-- category size
-- with category_size as (
-- 	select *
-- 	from (
-- 	select category as category_details, num_coins,
-- 		substring(url FROM '.*/(.*)$') AS category
-- 	from category_overview
-- 	where category is not null
-- 	order by num_coins desc
-- 	) as t
-- ),

select *
from (
	select *,
		row_number() over (partition by category_name order by percent_change desc) as rank
	from (
		select
			prev_close_today.*, 
			case
				when binance_symbols."categoryName" is not null then binance_symbols."categoryName"
				else 'Unknown'
			end as category_name
		from (
			select *
			from (
				select
						date,
						symbol_name, 
						LAG(close) OVER (PARTITION BY symbol_name ORDER BY date) AS prev_close,
						close,
						round(cast((close - LAG(close) OVER (PARTITION BY symbol_name ORDER BY date)) / close * 100 as numeric), 2) as percent_change
				from binance_bars
			) as prev_close
			where date = CURRENT_DATE -1 
		) as prev_close_today
		left join binance_symbols on prev_close_today.symbol_name = binance_symbols.symbol
	) as category
) final
where rank <= 5

