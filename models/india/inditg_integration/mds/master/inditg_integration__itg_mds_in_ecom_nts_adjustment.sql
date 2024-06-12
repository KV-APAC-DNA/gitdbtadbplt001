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
        convert_timezone('UTC', current_timestamp()) as crt_dttm,
        convert_timezone('UTC', current_timestamp()) as updt_dttm
    from source
)
select * from final
