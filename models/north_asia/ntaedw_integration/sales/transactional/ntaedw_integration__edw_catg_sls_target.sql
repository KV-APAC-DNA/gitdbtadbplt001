with source as (
    select * from {{ ref('ntawks_integration__wks_edw_catg_sls_target') }}
),
final as 
(
    select
        sls_grp::varchar(30) as sls_grp,
        brnd::varchar(30) as brnd,
        acct_mgr::varchar(2000) as acct_mgr,
        ctry_cd::varchar(10) as ctry_cd,
        cust_num::varchar(30) as cust_num,
        yr::number(18,0) as yr,
        mo::number(18,0) as mo,
        sls_grp_cat_trgt::number(16,5) as sls_grp_cat_trgt,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as upd_dttm
    from source
)
select * from final
