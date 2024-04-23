with source as (
    select * from {{ ref ('pcfedw_integration__edw_pacific_perenso_ims_pharmacy_snpshot_analysis') }}
),
final as (
	select 
        delvry_dt as "delvry_dt",
        snapshot_dt as "snapshot_dt",
        jj_year as "jj_year",
        jj_qrtr as "jj_qrtr",
        jj_mnth_id as "jj_mnth_id",
        jj_mnth_shrt as "jj_mnth_shrt",
        acct_key as "acct_key",
        acct_banner_division as "acct_banner_division",
        acct_banner as "acct_banner",
        acct_display_name as "acct_display_name",
        acct_tsm as "acct_tsm",
        acct_terriroty as "acct_terriroty",
        prod_sapbw_code as "prod_sapbw_code",
        prod_desc as "prod_desc",
        prod_jj_brand as "prod_jj_brand",
        prod_ean as "prod_ean",
        prod_jj_franchise as "prod_jj_franchise",
        prod_jj_category as "prod_jj_category",
        unit_qty as "unit_qty",
        nis as "nis",
        aud_nis as "aud_nis",
        usd_nis as "usd_nis"
    from source
)
select * from final
