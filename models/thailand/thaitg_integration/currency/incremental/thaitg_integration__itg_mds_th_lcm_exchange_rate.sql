with source as(
    select * from {{ source('thasdl_raw', 'sdl_mds_th_lcm_exchange_rate') }}
),
final as(
    select
        cntry_key::varchar(20) as cntry_key,
        cntry_nm::varchar(100) as cntry_nm,
        from_ccy::varchar(20) as from_ccy,
        to_ccy::varchar(20) as to_ccy,
        to_date(valid_from)::timestampntz(9) as valid_from,
        to_date(valid_to)::timestampntz(9) as valid_to,
        exch_rate::NUMBER(28,7) as exch_rate,
        current_timestamp()::timestampntz(9) as crt_dttm,
        current_timestamp()::timestampntz(9) as updt_dttm
    from source
)
select * from final