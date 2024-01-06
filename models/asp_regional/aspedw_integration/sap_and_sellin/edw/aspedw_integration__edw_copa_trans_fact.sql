{{
    config(
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["acct_num"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}

with 

source as (
    select * from {{ ref('aspitg_integration__itg_copa_trans') }}
),
 
edw_acct_hier as (
    select * from {{ source('asing012_workspace', 'edw_acct_hier') }}
),

 
final as (
    select
    co_cd,
    cntl_area,
    prft_ctr,
    sls_org,
    matl,
    cust_num,
    div,
    plnt,
    A.chrt_acct,
    A.acct_num,
    dstr_chnl,
    fisc_yr_var,
    vers,
    bw_delta_upd_mode,
    bill_typ,
    sls_off,
    cntry_key,
    sls_deal,
    sls_grp,
    sls_emp_hist,
    sls_dist,
    cust_grp,
    cust_sls,
    buss_area,
    val_type_rpt,
    mercia_ref,
    caln_day,
    caln_yr_mo,
    fisc_yr,
    pstng_per,
    fisc_yr_per,
    b3_base_prod,
    b4_var,
    b5_put_up,
    b1_mega_brnd,
    b2_brnd,
    reg,
    prod_minor,
    prod_maj,
    prod_fran,
    fran,
    gran_grp,
    oper_grp,
    sls_prsn_resp,
    matl_sls,
    prod_hier,
    mgmt_entity,
    fx_amt_cntl_area_crncy * multiplication_factor as fx_amt_cntl_area_crncy,
    amt_cntl_area_crncy * multiplication_factor as amt_cntl_area_crncy,
    crncy_key,
    amt_obj_crncy * multiplication_factor as amt_obj_crncy,
    obj_crncy_co_obj,
    grs_amt_trans_crncy * multiplication_factor as grs_amt_trans_crncy,
    crncy_key_trans_crncy,
    qty * multiplication_factor as qty,
    uom,
    sls_vol * multiplication_factor as sls_vol,
    un_sls_vol,
    measure_name,
    measure_code,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source as a
  inner join edw_acct_hier as b
    on ltrim(rtrim(a.acct_num)) = ltrim(rtrim(b.acct_num))
)

select * from final