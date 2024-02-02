with source as (
    select * from {{ source('myssdl_raw', 'sdl_my_ids_rate') }}
),
final as (
    select
        cust_id::varchar(50) as cust_id,
        cust_nm::varchar(100) as cust_nm,
        cast(exchng_rate as decimal(20, 4)) as exchng_rate,
        replace(yearmo,'/','')::varchar(30) as yearmo,
        cdl_dttm::varchar(255) as cdl_dttm,
        try_cast(substring(curr_dt, 1, 19) as timestampntz)::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final