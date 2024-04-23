with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_mds_pacific_ps_targets') }}
),
final as
(    
    select distinct 
        kpi::varchar(200) as kpi,
        re_customer_flag::varchar(510) as re_customer,
        attribute_2::varchar(510) as sub_category,
        attribute_1::varchar(510) as re_customer_value,
        value::number(31,3) as target,
        name::varchar(200) as market,
        channel::varchar(200) as channel,
        valid_from::timestamp_ntz(9) as valid_from,
        valid_to::timestamp_ntz(9) as valid_to,
        null::timestamp_ntz(9) as create_dt,
        null::timestamp_ntz(9) as update_dt
    from source
    where upper(channel) <> 'E-COMMERCE'
)
select * from final