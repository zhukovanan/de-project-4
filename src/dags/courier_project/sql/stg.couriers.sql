create table if not exists stg.couriers (
id serial primary key,
id_courier varchar not null,
name varchar not null,
upload_at timestamp,
run_id varchar)
