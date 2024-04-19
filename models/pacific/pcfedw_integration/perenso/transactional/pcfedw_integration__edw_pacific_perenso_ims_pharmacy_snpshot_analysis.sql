with source as(
    select * from {{ ref('pcfedw_integration__vw_pacific_perenso_ims_pharmacy_snpshot_analysis') }}
),
final as(
    select 
        to_date(delvry_dt) as delvry_dt,
        to_date(snapshot_dt) as snapshot_dt,
        jj_year::number(18,0) as jj_year,
        jj_qrtr::number(18,0) as jj_qrtr,
        jj_mnth_id::number(18,0) as jj_mnth_id,
        jj_mnth_shrt::varchar(3) as jj_mnth_shrt,
        acct_key::number(10,0) as acct_key,
        acct_banner_division::varchar(256) as acct_banner_division,
        acct_banner::varchar(256) as acct_banner,
        acct_display_name::varchar(256) as acct_display_name,
        acct_tsm::varchar(256) as acct_tsm,
        acct_terriroty::varchar(256) as acct_terriroty,
        prod_sapbw_code::varchar(50) as prod_sapbw_code,
        prod_desc::varchar(100) as prod_desc,
        prod_jj_brand::varchar(100) as prod_jj_brand,
        prod_ean::varchar(50) as prod_ean,
        prod_jj_franchise::varchar(100) as prod_jj_franchise,
        prod_jj_category::varchar(100) as prod_jj_category,
        unit_qty::number(10,0) as unit_qty,
        nis::number(21,2) as nis,
        aud_nis::number(37,7) as aud_nis,
        usd_nis::number(37,7) as usd_nis
    from source
)
select * from final