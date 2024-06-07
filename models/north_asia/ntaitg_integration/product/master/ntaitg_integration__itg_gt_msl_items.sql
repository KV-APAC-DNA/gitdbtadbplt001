



with source as
(
    -- select * from {{ ref('ntawks_integration__wks_gt_msl_items')}}
    select * from dev_dna_core.snapntawks_integration.wks_gt_msl_items
),



final as
(
    select 
        ctry_cd as ctry_cd,
        dstr_cd as dstr_cd,
        brand as brand,
        dstr_prod_cd as dstr_prod_cd,
        sap_matl_cd as sap_matl_cd,
        prod_desc_eng as prod_desc_eng,
        prod_desc_chnse as prod_desc_chnse,
        store_class as store_class,
        msl_flg as msl_flg,
        strt_yr_mnth as strt_yr_mnth,
        nvl(end_yr_mnth, '999912') as end_yr_mnth,
        convert_timezone('UTC', current_timestamp()) as crt_dttm,
        CAST(NULL AS DATE) as updt_dttm    
    from source WHERE file_rec_dt = 
    (
        SELECT MAX(file_rec_dt)
        FROM source
    )
)

select * from final