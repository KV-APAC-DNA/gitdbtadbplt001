with itg_id_ps_product_availability as 
(
    select * from {{ref('idnitg_integration__itg_id_ps_product_availability')}}
),
itg_id_ps_msl_reference as
(
    select * from {{ref('idnitg_integration__itg_id_ps_msl_reference')}}
),
pa as 
(
    (select distinct 
        pa.outlet_id,
        coalesce(pa.outlet_custom_code,'NA') outlet_custom_code,
        pa.outlet_name,
        pa.province,
        pa.city,
        pa.channel,
        msl.channel_group,
        pa.cust_group,
        pa.address,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
        row_number() over (partition by pa.outlet_id order by input_date desc) rn
    from itg_id_ps_product_availability pa
    left join itg_id_ps_msl_reference msl
            on upper (pa.channel) = upper (msl.channel)
            and upper (pa.cust_group) = upper (msl.cust_group))
),
transformed as  
(
    select 
        outlet_id,
        outlet_custom_code,
        outlet_name,
        province,
        city,
        channel,
        channel_group,
        cust_group,
        address,
        crt_dttm
    from pa
    where rn = 1
),
final as 
(
    select
        outlet_id::varchar(10) as outlet_id,
        outlet_custom_code::varchar(10) as outlet_custom_code,
        outlet_name::varchar(100) as outlet_name,
        province::varchar(50) as province,
        city::varchar(50) as city,
        channel::varchar(50) as channel,
        channel_group::varchar(50) as channel_group,
        cust_group::varchar(50) as cust_group,
        address::varchar(255) as address,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm
    from transformed
)
select * from final
