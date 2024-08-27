{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}


with sdl_ph_pos_sm_goods as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_sm_goods') }}
    where file_name not in (
        select distinct file_name from {{SOURCE('phlwks_integration','tratbl_sdl_ph_pos_sm_goods__null_test')}}
        union all
        select distinct file_name from {{SOURCE('phlwks_integration','tratbl_sdl_ph_pos_sm_goods__lookup_sm_CUST_CD')}}
        union all
        select distinct file_name from {{SOURCE('phlwks_integration','tratbl_sdl_ph_pos_sm_goods__lookup_sm_SAP_ITEM_CD')}}
        union all
        select distinct file_name from {{SOURCE('phlwks_integration','tratbl_sdl_ph_pos_sm_goods__lookup_sm_JNJ_PC_PER_CUST_UNIT')}}
        union all
        select distinct file_name from {{SOURCE('phlwks_integration','tratbl_sdl_ph_pos_sm_goods__lookup_sm_LST_PRICE_UNIT')}}
        union all
        select distinct file_name from {{SOURCE('phlwks_integration','tratbl_sdl_ph_pos_sm_goods__lookup_sm_PL_JJ_MNTH_ID')}}
    )
),
final as (
select *
from sdl_ph_pos_sm_goods
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %})
select * from final