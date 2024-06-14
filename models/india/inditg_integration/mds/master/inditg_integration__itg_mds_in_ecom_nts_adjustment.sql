with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_mds_in_ecom_nts_adjustment') }}
),
final as 
(
    select
        valid_from::varchar(20) as valid_from,
        valid_to::varchar(20) as valid_to,
        value::numeric(31,4) as value,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final
