with source as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_non_ise_puregold') }}
    where filename not in (
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_non_ise_puregold__null_test')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_non_ise_puregold__duplicate_test')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_non_ise_puregold__test_date_format_odd_eve_leap')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_non_ise_puregold__lookup_test')}}
    )
),
final as (
    select 
        ret_nm_prefix,
        sku_code,
        brand,
        barcode,
        item_description,
        msl_large,
        msl_small,
        msl_grocery,
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
        sub_channel as sub_channel,
        diser_name as diser_name,
        msl_reseller as msl_reseller,
        msl_priceclub as msl_priceclub,
        filename,
        run_id,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
        
    from source 
)
select * from final