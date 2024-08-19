with source as
(
    select * from {{ source('aspsdl_raw', 'sdl_mds_ap_digital_shelf_targets') }}
),
final as
(
    select market,
    re as retail_environment,
    kpi,
    attribute_1,
    attribute_2,
    value,
    valid_from,
    valid_to,
    convert_timezone('Asia/Singapore',current_timestamp()) AS crt_dttm,
    convert_timezone('Asia/Singapore',current_timestamp()) as updt_dttm
    from source
)
select market::varchar(50) as market,
    retail_environment::varchar(50) as retail_environment,
    kpi::varchar(50) as kpi,
    attribute_1::varchar(100) as attribute_1,
    attribute_2::varchar(100) as attribute_2,
    value::numeric(10,2) as value,
    valid_from::timestamp_ntz(9) as valid_from,
    valid_to::timestamp_ntz(9) as valid_to,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
 from final