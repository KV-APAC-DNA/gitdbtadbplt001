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
    co_cd::varchar(4) as CO_CD,
cntl_area::varchar(4) as CNTL_AREA,
prft_ctr::varchar(10) as PRFT_CTR,
sls_org::varchar(4) as SLS_ORG,
matl::varchar(18) as MATL_NUM,
cust_num::varchar(10) as CUST_NUM,
div::varchar(2) as DIV,
plnt::varchar(4) as PLNT,
A.chrt_acct::varchar(4) as CHRT_ACCT,
A.acct_num::varchar(10) as ACCT_NUM,
dstr_chnl::varchar(2) as DSTR_CHNL,
fisc_yr_var::varchar(2) as FISC_YR_VAR,
vers::varchar(3) as VERS,
bw_delta_upd_mode::varchar(1) as BW_DELTA_UPD_MODE,
bill_typ::varchar(4) as BILL_TYP,
sls_off::varchar(4) as SLS_OFC,
cntry_key::varchar(3) as CTRY_KEY,
sls_deal::varchar(10) as SLS_DEAL,
sls_grp::varchar(3) as SLS_GRP,
sls_emp_hist::number(18,0) as SLS_EMP_HIST,
sls_dist::varchar(6) as SLS_DIST,
cust_grp::varchar(2) as CUST_GRP,
cust_sls::varchar(10) as CUST_SLS,
buss_area::varchar(4) as BUSS_AREA,
val_type_rpt::number(18,0) as VAL_TYPE_RPT,
mercia_ref::varchar(5) as MERCIA_REF,
caln_day::varchar(20) as CALN_DAY,
caln_yr_mo::number(18,0) as CALN_YR_MO,
fisc_yr::number(18,0) as FISC_YR,
pstng_per::number(18,0) as PSTNG_PER,
fisc_yr_per::number(18,0) as FISC_YR_PER,
b3_base_prod::varchar(3) as B3_BASE_PROD,
b4_var::varchar(3) as B4_VAR,
b5_put_up::varchar(3) as B5_PUT_UP,
b1_mega_brnd::varchar(3) as B1_MEGA_BRND,
b2_brnd::varchar(3) as B2_BRND,
reg::varchar(3) as REG,
prod_minor::varchar(18) as PROD_MINOR,
prod_maj::varchar(18) as PROD_MAJ,
prod_fran::varchar(18) as PROD_FRAN,
fran::varchar(18) as FRAN,
gran_grp::varchar(18) as GRAN_GRP,
oper_grp::varchar(18) as OPER_GRP,
sls_prsn_resp::varchar(30) as SLS_PRSN_RESP,
matl_sls::varchar(18) as MATL_SLS,
prod_hier::varchar(18) as PROD_HIER,
mgmt_entity::varchar(6) as MGMT_ENTITY,
fx_amt_cntl_area_crncy * multiplication_factor::NUMBER(20,5) as fx_amt_cntl_area_crncy,
amt_cntl_area_crncy * multiplication_factor::NUMBER(20,5) as amt_cntl_area_crncy,
crncy_key::varchar(5) as crncy_key,
amt_obj_crncy * multiplication_factor::NUMBER(20,5) as amt_obj_crncy,
obj_crncy_co_obj::varchar(5) as obj_crncy_co_obj,
grs_amt_trans_crncy * multiplication_factor::NUMBER(20,5) as grs_amt_trans_crncy,
crncy_key_trans_crncy::varchar(5) as crncy_key_trans_crncy,
qty * multiplication_factor::NUMBER(20,5) as qty,
uom::varchar(20) as uom,
sls_vol * multiplication_factor as sls_vol,
un_sls_vol::varchar(20) as un_sls_vol,
measure_name::varchar(100) as ACCT_HIER_DESC,
measure_code::varchar(100) as ACCT_HIER_SHRT_DESC,
current_timestamp()::timestamp_ntz(9) as crt_dttm,
current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source as a
  inner join edw_acct_hier as b
    on ltrim(rtrim(a.acct_num)) = ltrim(rtrim(b.acct_num))
)

select * from final