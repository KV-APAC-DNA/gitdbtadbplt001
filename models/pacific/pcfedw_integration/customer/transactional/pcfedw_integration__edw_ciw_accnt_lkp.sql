
{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['sap_account']
    )
}}

with source as (
    select * from {{ source('pcfsdl_raw','sdl_mds_pacific_sap_gl_ciw_mapping') }}
),
final as
(
select 
    name::varchar(40) as key_measure,
	ciw_category::varchar(40) as ciw_category,
	ciw_account_group::varchar(40) as ciw_account_group,
	code::varchar(20) as sap_account,
    current_timestamp()::timestamp_ntz(9) as crt_dttm
from source
)
select * from final