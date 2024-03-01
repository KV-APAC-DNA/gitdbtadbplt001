with source as (
    select * from {{ source('myssdl_raw', 'sdl_my_in_transit') }}
),
final as (
    select
        bill_doc::varchar(50) as bill_doc,
        to_date(bill_dt,'DD-MM-YYYY') as bill_dt,
        to_date(gr_dt,'DD-MM-YYYY') as gr_dt,
        to_date(closing_dt,'DD-MM-YYYY') as closing_dt,
        remarks::varchar(255) as remarks,
        cdl_dttm::varchar(50) as cdl_dttm,
        try_cast(substring(curr_dt, 1, 19) as timestampntz)::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final