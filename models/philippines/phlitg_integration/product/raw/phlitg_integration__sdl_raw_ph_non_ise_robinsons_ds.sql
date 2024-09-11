{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('phlsdl_raw', 'sdl_ph_non_ise_robinsons_ds') }}
    where filename not in (
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_non_ise_robinsons_ds__null_test')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_non_ise_robinsons_ds__duplicate_test')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_non_ise_robinsons_ds__test_date_format_odd_eve_leap')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_non_ise_robinsons__lookup_test')}}
    )
),
final as
(
    select 
        ret_nm_prefix as ret_nm_prefix,
        sku_code as sku_code,
        brand as brand,
        barcode as barcode,
        item_description as item_description,
        msl_dept as msl_dept,
        month as month,
        week as week,
        reason as reason,
        acct_deliv_date as acct_deliv_date,
        osa_check_date as osa_check_date,
        encoded_report as encoded_report,
        team_leader as team_leader,
        branch_code as branch_code,
        branch_code_original as branch_code_original,
        branch_classification as branch_classification,
        branch_name as branch_name,
        osa_flag as osa_flag,
        retailer_name as retailer_name,
        filename as filename,
        run_id as run_id,
        crtd_dttm as crtd_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)
select * from final
