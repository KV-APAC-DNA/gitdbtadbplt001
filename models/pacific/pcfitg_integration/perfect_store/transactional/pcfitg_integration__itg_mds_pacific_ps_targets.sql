with source as
(
    select * from {{source('pcfsdl_raw','sdl_mds_pacific_ps_targets')}}
),
final as
(
    select 
        name::varchar(100) as market,
        channel::varchar(100) as channel,
        re_customer_flag::varchar(100) as retail_env,
        kpi::varchar(100) as kpi,
        attribute_1::varchar(100) as attribute_1,
        attribute_2::varchar(100) as attribute_2,
        value::number(20,4) as target,
        current_timestamp::timestamp_ntz(9) as crtd_dttm,
        valid_from::timestamp_ntz(9) as valid_from,
        valid_to::timestamp_ntz(9) as valid_to
    from source    
)
select * from final