INSERT INTO dds.dm_couriers (id, name)


SELECT 
    id_courier
    ,name
from stg.couriers
where upload_at = (select max(upload_at) from stg.couriers)
    
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;