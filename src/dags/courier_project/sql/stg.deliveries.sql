create table if not exists stg.deliveries (
id serial primary key,
order_id varchar not null,
order_ts timestamp not null,
delivery_id varchar,
courier_id varchar,
address text,
delivery_ts timestamp,
rate int,
sum numeric(14,2),
tip_sum numeric(14,2),
restaurant_id varchar not null,
upload_at timestamp,
run_id varchar)