{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['sap_code', 'item_idnt'],
        pre_hook= ["delete from {{ ref('pcfwks_integration__wks_dstr_woolworth_sap_mapping') }} where article_code in ( select article_code from ( select article_code, count(*) from ( select article_code, sap_code from {{ ref('pcfwks_integration__wks_dstr_woolworth_sap_mapping') }} ) group by 1 having count(*) > 1 ) )", "delete from {{this}} where article_code in ( select distinct article_code from {{ ref('pcfwks_integration__wks_dstr_woolworth_sap_mapping') }} where not sap_code is null or sap_code <> '' );"]
    )
}}

with source as(
    select * from {{ ref('pcfwks_integration__wks_dstr_woolworth_sap_mapping') }}
),
final as(
    Select 
        article_code as article_code,
        sap_code as sap_code,
        article_name as article_name,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source where not sap_code is null or sap_code<>''
)
select * from final