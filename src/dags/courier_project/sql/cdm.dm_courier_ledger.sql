create table if not exists cdm.dm_courier_ledger (
id serial primary key,
courier_id varchar not null,
courier_name varchar not null,
settlement_year varchar not null,
settlement_month int not null,
orders_count int not null DEFAULT 0,
orders_total_sum numeric(14,2) not null DEFAULT 0,
rate_avg numeric(14,2) not null DEFAULT 0,
order_processing_fee numeric(14,2) not null DEFAULT 0,
courier_order_sum numeric(14,2) not null DEFAULT 0,
courier_tips_sum numeric(14,2) not null DEFAULT 0,
courier_reward_sum numeric(14,2) not null DEFAULT 0,
CONSTRAINT dm_courier_ledger_month_check check(((settlement_month >= 1) AND (settlement_month <= 12))),
CONSTRAINT dm_courier_ledger_order_processing_fee_check check(order_processing_fee=orders_total_sum * 0.25),
CONSTRAINT dm_courier_ledger_courier_reward_sum_check check(courier_order_sum + courier_tips_sum * 0.95));


INSERT INTO cdm.dm_courier_ledger(courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee,
courier_order_sum, courier_tips_sum, courier_reward_sum)
SELECT 
	courier_id
	,name
	,extract(year from delivery_ts) as settlement_year
	,extract(month from delivery_ts) as settlement_month
	,COUNT(order_id) as orders_count
	,sum(sum) as orders_total_sum
	,AVG(rate*1.00) as rate_avg
	,sum(sum)*0.25 as order_processing_fee
	,sum(case when rate < 4 and 0.05*sum >= 100 then 0.05*sum
		  when rate < 4 and 0.05*sum < 100 then 100
		  when rate >= 4 and rate < 4.5 and 0.07*sum >= 150 then 0.07*sum
		  when rate >= 4 and rate < 4.5 and 0.07*sum < 150 then 150
		  when rate >= 4.5 and rate < 4.9 and 0.08*sum >= 175 then 0.08*sum
		  when rate >= 4.5 and rate < 4.9 and 0.08*sum < 175 then 175
		  when rate >= 4.9 and 0.1*sum >= 200 then 0.1*sum else 200 end) as courier_order_sum
	,sum(tip_sum) as courier_tips_sum
	,sum(case when rate < 4 and 0.05*sum >= 100 then 0.05*sum
		  when rate < 4 and 0.05*sum < 100 then 100
		  when rate >= 4 and rate < 4.5 and 0.07*sum >= 150 then 0.07*sum
		  when rate >= 4 and rate < 4.5 and 0.07*sum < 150 then 150
		  when rate >= 4.5 and rate < 4.9 and 0.08*sum >= 175 then 0.08*sum
		  when rate >= 4.5 and rate < 4.9 and 0.08*sum < 175 then 175
		  when rate >= 4.9 and 0.1*sum >= 200 then 0.1*sum else 200 end)
	+ sum(tip_sum)* 0.95 as courier_reward_sum
FROM dds.fct_deliveries fd
INNER JOIN dds.dm_couriers dc 
ON fd.courier_id = dc.id
GROUP BY
	extract(year from delivery_ts)
	,extract(month from delivery_ts)
	,courier_id
	,name
;