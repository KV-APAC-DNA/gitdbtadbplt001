with source as (
    select * from {{ ref('idnitg_integration__itg_id_ps_planogram') }} 
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
        photo_link::varchar(100) as photo_link,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final
