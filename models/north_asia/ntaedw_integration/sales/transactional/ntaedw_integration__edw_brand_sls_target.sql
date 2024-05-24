with source as (
    select * from snapntawks_integration.wks_edw_brand_sls_target
),
final as 
(
    select
        brnd::varchar(30) as brnd,
        ctry_cd::varchar(10) as ctry_cd,
        acct_mgr::varchar(30) as acct_mgr,
        cust_num::varchar(30) as cust_nm,
        yr::number(18,0) as yr,
        mo::number(18,0) as mo,
        brnd_trgt::number(16,5) as brnd_trgt,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as upd_dttm
    from source
)
select * from final
