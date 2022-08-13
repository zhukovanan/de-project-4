INSERT INTO dds.fct_deliveries (order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum, restaurant_id)


SELECT 
    order_id
    ,order_ts
    ,delivery_id
    ,courier_id
    ,address
    ,delivery_ts
    ,rate
    ,sum
    ,tip_sum
    ,restaurant_id
FROM stg.deliveries
WHERE upload_at = (SELECT max(upload_at) FROM stg.deliveries)
    
ON CONFLICT (order_id) DO UPDATE SET delivery_id = EXCLUDED.delivery_id
                                     ,courier_id = EXCLUDED.courier_id
                                     ,address = EXCLUDED.address
                                     ,delivery_ts = EXCLUDED.delivery_ts
                                     ,rate = EXCLUDED.rate
                                     ,sum = EXCLUDED.sum
                                     ,tip_sum = EXCLUDED.tip_sum
                                     ,restaurant_id = EXCLUDED.restaurant_id;