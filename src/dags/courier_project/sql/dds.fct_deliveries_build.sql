create table if not exists dds.fct_deliveries (
order_id varchar primary key,
order_ts timestamp not null,
delivery_id varchar,
courier_id varchar,
address text,
delivery_ts timestamp,
rate int,
sum numeric(14,2),
tip_sum numeric(14,2),
restaurant_id varchar not null)