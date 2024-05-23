with source as(
    select * from {{ source('ntasdl_raw', 'sdl_mds_tw_sales_representative') }}
),
final as(
    select 
        code::varchar(500) as psr_code,
        name::varchar(500) as psr_name,
        reportto_code::varchar(500) as report_to,
        reportto_name::varchar(500) as reportto_name,
        reverse_code::varchar(500) as reverse,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    FROM source
)
select * from final