with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_mds_pacific_ps_weights') }}
),
final as
(    
    select distinct 
        ctry::varchar(510) as ctry,
        data_type::varchar(510) as data_type,
        kpi_name::varchar(510) as kpi_name,
        retail_environment::varchar(510) as retail_environment,
        value::number(31,2) as value,
        null::timestamp_ntz(9) as create_dt,
        null::timestamp_ntz(9) as update_dt
    from source
    where upper(channel) <> 'E-COMMERCE'
)
select * from final