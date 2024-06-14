with source as
(
    
    select * from dev_dna_core.snapntawks_integration.wks_gt_msl_items
),



transformed as
(
    select 
        ctry_cd,
        dstr_cd,
        brand,
        dstr_prod_cd,
        sap_matl_cd,
        prod_desc_eng,
        prod_desc_chnse,
        store_class,
        msl_flg,
        strt_yr_mnth,
        nvl(end_yr_mnth, '999912') AS end_yr_mnth,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) AS crt_dttm,
        CAST(NULL AS DATE) updt_dttm
    FROM source
    WHERE file_rec_dt = 
    (
        SELECT MAX(file_rec_dt)
        FROM source
    )
),

final as
(
    select 
        ctry_cd::varchar(5) as ctry_cd,
		dstr_cd::varchar(10) as dstr_cd,
		brand::varchar(50) as brand,
		dstr_prod_cd::varchar(40) as dstr_prod_cd,
		sap_matl_cd::varchar(40) as sap_matl_cd,
		prod_desc_eng::varchar(100) as prod_desc_eng,
		prod_desc_chnse::varchar(100) as prod_desc_chnse,
		store_class::varchar(3)as store_class,
		msl_flg::varchar(2)as msl_flg,
		strt_yr_mnth::varchar(20) as strt_yr_mnth,
		end_yr_mnth::varchar(20) as end_yr_mnth,
		convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
		null::timestamp_ntz as updt_dttm
    from transformed
)

select * from final