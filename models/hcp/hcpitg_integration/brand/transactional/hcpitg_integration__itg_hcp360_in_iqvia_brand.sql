with sdl_hcp360_in_iqvia_aveeno_brand as(
    select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_iqvia_aveeno_brand') }}
),
final as(
SELECT
    'IN' as country,    
    product_description::varchar(50) as product_description,
    pack_description::varchar(100) as pack_description,
    zone::varchar(20) as zone,
    year_month::date as year_month,
    no_of_prescriptions::number(18,5) as no_of_prescriptions,
    no_of_prescribers::number(18,5) as no_of_prescribers,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm,
    '/source_type'::varchar(20) as data_source,
    pack_volume::varchar(20) as pack_volume,
FROM
sdl_hcp360_in_iqvia_aveeno_brand
)
select * from final








