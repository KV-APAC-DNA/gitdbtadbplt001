with source as
(
    select * from {{ ref('idnitg_integration__itg_distributor_ivy_inventory') }} 
),   
final as
(
    select
        distributor_code::varchar(25) as distributor_code,
        warehouse_code::varchar(25) as warehouse_code,
        product_code::varchar(25) as product_code,
        batch_code::varchar(50) as batch_code,
        batch_expiry_date::varchar(50) as batch_expiry_date,
        uom::varchar(15) as uom,
        qty::number(10,0) as qty,
	    create_dt::varchar(50) as create_dt
    from source
)
select * from final