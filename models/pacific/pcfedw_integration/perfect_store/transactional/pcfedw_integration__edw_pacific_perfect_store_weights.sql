with source as
(
    select * from {{ ref('pcfwks_integration__wks_pacific_perfect_store_weights') }}
),
final as
(    
    select
        ctry::varchar(510) as ctry,
        data_type::varchar(510) as data_type,
        kpi_name::varchar(510) as kpi_name,
        retail_environment::varchar(510) as retail_environment,
        value::number(31,2) as value,
        current_timestamp::timestamp_ntz(9) as create_dt,
        current_timestamp::timestamp_ntz(9) as update_dt
    from source
)
select * from final