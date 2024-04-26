{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with sdl_ph_non_ise_shm as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_non_ise_shm') }}
),
final as (
SELECT ret_nm_prefix,

       sku_code,

       brand,

       barcode,

       item_description,

       msl_large,

       msl_small,

       msl_premium,

       month,

       week,

       reason,

       acct_deliv_date,

       osa_check_date,

       encoded_report,

       team_leader,

       branch_code,

       branch_code_original,

       branch_classification,

       branch_name,

       osa_flag,

       retailer_name,

       filename,

       run_id,

       crtd_dttm

FROM sdl_ph_non_ise_shm
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
 {% endif %})
select * from final 