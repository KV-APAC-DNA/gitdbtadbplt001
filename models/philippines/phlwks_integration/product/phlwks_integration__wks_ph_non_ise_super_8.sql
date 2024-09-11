with source as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_non_ise_super_8') }}
    where filename not in (
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_non_ise_super_8__null_test')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_non_ise_super_8__duplicate_test')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_non_ise_super_8__test_date_format_odd_eve_leap')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_non_ise_super_8__lookup_test')}}
    )
),
final as (
    select 
        ret_nm_prefix,
        sku_code,
        brand,
        barcode,
        item_description,
        msl_sup_hybrid,
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
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source 
)
select * from final