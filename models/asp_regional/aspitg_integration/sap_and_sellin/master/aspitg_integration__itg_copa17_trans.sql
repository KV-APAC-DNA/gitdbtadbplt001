{{
    config(
        materialized="incremental",
        incremental_strategy= "merge",
        unique_key=  ["fisc_yr_per","vers"],
        merge_exclude_columns=["crt_dttm"]
    )
}}

--import CTE

with source as (
    select * from {{ ref('aspwks_integration__wks_itg_copa17_trans')}}
),

--Logical CTE

final as(
    select 
    request_number as request_number,
    data_packet as data_packet,
    data_record as data_record,
    fiscper as fisc_yr_per,
    fiscvarnt as fisc_yr_vrnt,
    fiscyear as fisc_yr,
    calday as cal_day,
    fiscper3 as pstng_per,
    calmonth as cal_yr_mo,
    calyear as cal_yr,
    version as vers,
    vtype as val_type,
    comp_code as co_cd,
    co_area as cntl_area,
    profit_ctr as prft_ctr,
    salesemply as sls_emp_hist,
    salesorg as sls_org,
    sales_grp as sls_grp,
    sales_off as sls_off,
    cust_group as cust_grp,
    distr_chan as dstn_chnl,
    sales_dist as sls_dstrc,
    customer as cust,
    material as matl,
    cust_sales as cust_sls_view,
    division as div,
    plant as plnt,
    zmercref as mercia_ref,
    zz_mvgr1 as b3_base_prod,
    zz_mvgr2 as b4_vrnt,
    zz_mvgr3 as b5_put_up,
    zz_mvgr4 as b1_mega_brnd,
    zz_mvgr5 as b2_brnd,
    region as rgn,
    country as ctry,
    prodh6 as prod_minor,
    prodh5 as prod_maj,
    prodh4 as prod_fran,
    prodh3 as fran,
    prodh2 as fran_grp,
    prodh1 as oper_grp,
    zfis_quar as fisc_qtr,
    mat_sales as matl2,
    bill_type as bill_type,
    jjfiscwe as fisc_wk,
    amocac as amt_grp_crcy,
    amoccc as amt_obj_crcy,
    currency as crncy,
    obj_curr as obj_crncy,
    account as acct_num,
    chrt_accts as chrt_of_acct,
    zz_wwme as mgmt_entity,
    zsalesper as sls_prsn_respons,
    bus_area as busn_area,
    grossamttc as ga,
    curkey_tc as tc,
    mat_plant as matl_plnt_view,
    quantity as qty,
    unit as uom,
    zqtyieu as sls_vol_ieu,
    zunitieu as un_sls_vol_ieu,
    zbpt_dc as bpt_dstn_chnl,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
--final select
select * from final