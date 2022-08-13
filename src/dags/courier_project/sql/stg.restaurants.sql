create table if not exists stg.restaurants (
id serial primary key,
id_restaurant varchar not null,
name varchar not null,
upload_at timestamp not null,
run_id varchar not null)