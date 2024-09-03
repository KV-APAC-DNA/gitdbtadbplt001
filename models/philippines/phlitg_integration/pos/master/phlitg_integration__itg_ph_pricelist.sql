{{
    config
    (
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key= ["jj_mnth_id","item_cd"]
    )
}}

with sdl_ph_pricelist as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pricelist') }}
),
final as (
select 
jj_mnth_id::varchar(10) as jj_mnth_id,
item_cd::varchar(20) as item_cd,
item_desc::varchar(80) as item_desc,
consumer_bar_cd::varchar(20) as consumer_bar_cd,
shippers_bar_cd::varchar(20) as shippers_bar_cd,
dz_per_case::number(15,4) as dz_per_case,
lst_price_case::number(15,4) as lst_price_case,
lst_price_dz::number(15,4) as lst_price_dz,
lst_price_unit::number(15,4) as lst_price_unit,
srp::number(15,4) as srp,
status::varchar(15) as status,
sap_item_desc::varchar(50) as sap_item_desc,
brnd_vrnt::varchar(100) as brnd_vrnt,
frnchse_cd::varchar(50) as frnchse_cd,
cdl_dttm::varchar(20) as cdl_dttm,
current_timestamp()::timestamp_ntz(9) as crtd_dttm,
null::timestamp_ntz(9) as updt_dttm,
file_name::varchar(255) as file_name
from sdl_ph_pricelist
)
select * from final