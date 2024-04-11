with source as
(
    select * from {{ ref('pcfwks_integration__wks_pacific_perfect_store_targets') }}
),
final as
(    
    select
        kpi::varchar(200) as kpi,
        re_customer::varchar(510) as re_customer,
        sub_category::varchar(510) as sub_category,
        re_customer_value::varchar(510) as re_customer_value,
        target::number(31,3) as target,
        current_timestamp::timestamp_ntz(9) as create_dt,
        current_timestamp::timestamp_ntz(9) as update_dt,
        market::varchar(200) as market,
        channel::varchar(200) as channel,
        valid_from::timestamp_ntz(9) as valid_from,
        valid_to::timestamp_ntz(9) as valid_to
    from source
)
select * from final