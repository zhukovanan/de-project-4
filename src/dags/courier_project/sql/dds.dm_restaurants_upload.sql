INSERT INTO dds.dm_restaurants (id, name)


SELECT 
    id_restaurant
    ,name
from stg.restaurants
where upload_at = (select max(upload_at) from stg.restaurants)
    
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;