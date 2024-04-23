{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['sap_code', 'item_idnt'],
        pre_hook= ["delete from {{ ref('pcfwks_integration__wks_dstr_coles_sap_mapping') }} where sap_code in ( select sap_code from ( select sap_code, count(*) from ( select sap_code, item_idnt from {{ ref('pcfwks_integration__wks_dstr_coles_sap_mapping') }} ) group by 1 having count(*) > 1 ) )","delete FROM {{this}} WHERE (LTRIM(sap_code, 0), LTRIM(item_idnt, 0)) IN ( SELECT LTRIM(sap_code, 0), LTRIM(item_idnt, 0) FROM {{ ref('pcfwks_integration__wks_dstr_coles_sap_mapping') }} WHERE NOT sap_code IS NULL OR sap_code <> '' );"]
    )
}}

with source as(
    select * from {{ ref('pcfwks_integration__wks_dstr_coles_sap_mapping') }}
),
final as(
    Select 
        sap_code::varchar(50) as sap_code,
        item_idnt::varchar(50) as item_idnt,
        item_desc::varchar(200) as item_desc,
        current_timestamp()::timestamp_ntz(9) as crtd_dtmm
    from source where not sap_code is null or sap_code<>''
)
select * from final