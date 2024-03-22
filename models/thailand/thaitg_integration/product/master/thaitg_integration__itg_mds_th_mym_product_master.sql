with source as(
    select * from {{ source('thasdl_raw','sdl_mds_th_mym_product_master') }}
),
final as (
select
  "item NO."::varchar(200) as item_no,
  "product description"::varchar(200) as product_description,
  "sap code"::number(28,0) as sap_code,
  "sap name"::varchar(200) as sap_name,
  brand::varchar(200) as brand,
  franchise::varchar(200) as franchise,
  size::number(28,0) as size,
  "dz per case"::number(28,1) as dz_per_case,
  "unit per case"::number(28,0) as unit_per_case,
  price_per_case_usd::number(28,7) as price_per_case_usd,
  current_timestamp()::timestamp_ntz(9) as crt_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm
from source
)

select * from final