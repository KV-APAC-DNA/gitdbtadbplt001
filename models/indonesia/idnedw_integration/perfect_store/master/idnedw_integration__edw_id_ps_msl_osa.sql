with source as 
(
    select * from {{ref('idnitg_integration__itg_id_ps_product_availability')}}
),
final as 
(
    select
        outlet_id::varchar(10) as outlet_id,
        outlet_name::varchar(100) as outlet_name,
        province::varchar(50) as province,
        city::varchar(50) as city,
        channel::varchar(50) as channel,
        merchandiser_id::varchar(20) as merchandiser_id,
        merchandiser_name::varchar(50) as merchandiser_name,
        cust_group::varchar(50) as cust_group,
        to_date(input_date) as input_date,
        day_name::varchar(20) as day_name,
        franchise::varchar(50) as franchise,
        put_up_sku::varchar(100) as put_up_sku,
        stock_qty_pcs::number(18,0) as stock_qty_pcs,
        osa_flag::varchar(2) as osa_flag,
        availability_per_franchise::number(38,5) as availability_per_franchise,
        availability_osa::varchar(30) as availability_osa,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final
