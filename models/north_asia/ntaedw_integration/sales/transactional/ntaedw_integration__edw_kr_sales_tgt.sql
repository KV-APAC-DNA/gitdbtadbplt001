with source as (
    select * from {{ ref('ntaitg_integration__itg_kr_sales_tgt') }}
),
final as (
    select 
        ctry_cd::varchar(2) as ctry_cd,
        crncy_cd::varchar(3) as crncy_cd,
        sls_ofc_cd::varchar(4) as sls_ofc_cd,
        sls_ofc_desc::varchar(50) as sls_ofc_desc,
        channel::varchar(50) as channel,
        store_type::varchar(50) as store_type,
        sls_grp_cd::varchar(3) as sls_grp_cd,
        sls_grp::varchar(100) as sls_grp,
        target_type::varchar(4) as target_type,
        prod_hier_l2::varchar(50) as prod_hier_l2,
        prod_hier_l4::varchar(50) as prod_hier_l4,
        fisc_yr::number(18,0) as fisc_yr,
        fisc_yr_per::number(18,0) as fisc_yr_per,
        target_amt::number(20,5) as target_amt,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final