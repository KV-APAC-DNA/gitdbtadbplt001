with edw_vw_my_sellin_sales_fact as (
    select * from {{ ref('mysedw_integration__edw_vw_my_sellin_sales_fact') }}
),

final as (
    select 
        co_cd::varchar(4) as co_cd,
        cntry_nm::varchar(3) as cntry_nm,
        pstng_dt::varchar(20) as pstng_dt,
        jj_mnth_id::varchar(22) as jj_mnth_id,
        item_cd::varchar(18) as item_cd,
        cust_id::varchar(10) as cust_id,
        sls_org::varchar(4) as sls_org,
        plnt::varchar(4) as plnt,
        dstr_chnl::varchar(2) as dstr_chnl,
        acct_no::varchar(10) as acct_no,
        bill_typ::varchar(4) as bill_typ,
        sls_ofc::varchar(4) as sls_ofc,
        sls_grp::varchar(3) as sls_grp,
        sls_dist::varchar(6) as sls_dist,
        cust_grp::varchar(2) as cust_grp,
        cust_sls::varchar(10) as cust_sls,
        fisc_yr::number(18,0) as fisc_yr,
        pstng_per::number(18,0) as pstng_per,
        base_val::number(38,5) as base_val,
        sls_qty::number(38,5) as sls_qty,
        ret_qty::number(38,5) as ret_qty,
        sls_less_rtn_qty::number(38,5) as sls_less_rtn_qty,
        gts_val::number(38,5) as gts_val,
        ret_val::number(38,5) as ret_val,
        gts_less_rtn_val::number(38,5) as gts_less_rtn_val,
        trdng_term_val::number(38,5) as trdng_term_val,
        tp_val::number(38,5) as tp_val,
        trde_prmtn_val::number(38,5) as trde_prmtn_val,
        nts_val::number(38,5) as nts_val,
        nts_qty::number(38,5) as nts_qty,
        current_timestamp()::timestamp_ntz(9) as snapshot_dt
    from edw_vw_my_sellin_sales_fact
    where cntry_nm = 'MY'
)

select * from final
