{{
    config(
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
    select * from {{ ref('aspedw_integration__edw_acct_hier') }}
),

 
final as (
select
co_cd::varchar(4) as co_cd,
cntl_area::varchar(4) as cntl_area,
prft_ctr::varchar(10) as prft_ctr,
sls_org::varchar(4) as sls_org,
matl::varchar(18) as matl_num,
cust_num::varchar(10) as cust_num,
div::varchar(2) as div,
plnt::varchar(4) as plnt,
a.chrt_acct::varchar(4) as chrt_acct,
a.acct_num::varchar(10) as acct_num,
dstr_chnl::varchar(2) as dstr_chnl,
fisc_yr_var::varchar(2) as fisc_yr_var,
vers::varchar(3) as vers,
bw_delta_upd_mode::varchar(1) as bw_delta_upd_mode,
bill_typ::varchar(4) as bill_typ,
sls_off::varchar(4) as sls_ofc,
cntry_key::varchar(3) as ctry_key,
sls_deal::varchar(10) as sls_deal,
sls_grp::varchar(3) as sls_grp,
sls_emp_hist::number(18,0) as sls_emp_hist,
sls_dist::varchar(6) as sls_dist,
cust_grp::varchar(2) as cust_grp,
cust_sls::varchar(10) as cust_sls,
buss_area::varchar(4) as buss_area,
val_type_rpt::number(18,0) as val_type_rpt,
mercia_ref::varchar(5) as mercia_ref,
caln_day::varchar(20) as caln_day,
caln_yr_mo::number(18,0) as caln_yr_mo,
fisc_yr::number(18,0) as fisc_yr,
pstng_per::number(18,0) as pstng_per,
fisc_yr_per::number(18,0) as fisc_yr_per,
b3_base_prod::varchar(3) as b3_base_prod,
b4_var::varchar(3) as b4_var,
b5_put_up::varchar(3) as b5_put_up,
b1_mega_brnd::varchar(3) as b1_mega_brnd,
b2_brnd::varchar(3) as b2_brnd,
reg::varchar(3) as reg,
prod_minor::varchar(18) as prod_minor,
prod_maj::varchar(18) as prod_maj,
prod_fran::varchar(18) as prod_fran,
fran::varchar(18) as fran,
gran_grp::varchar(18) as gran_grp,
oper_grp::varchar(18) as oper_grp,
sls_prsn_resp::varchar(30) as sls_prsn_resp,
matl_sls::varchar(18) as matl_sls,
prod_hier::varchar(18) as prod_hier,
mgmt_entity::varchar(6) as mgmt_entity,
fx_amt_cntl_area_crncy * multiplication_factor::number(20,5) as fx_amt_cntl_area_crncy,
amt_cntl_area_crncy * multiplication_factor::number(20,5) as amt_cntl_area_crncy,
crncy_key::varchar(5) as crncy_key,
amt_obj_crncy * multiplication_factor::number(20,5) as amt_obj_crncy,
obj_crncy_co_obj::varchar(5) as obj_crncy_co_obj,
grs_amt_trans_crncy * multiplication_factor::number(20,5) as grs_amt_trans_crncy,
crncy_key_trans_crncy::varchar(5) as crncy_key_trans_crncy,
qty * multiplication_factor::number(20,5) as qty,
uom::varchar(20) as uom,
sls_vol * multiplication_factor as sls_vol,
un_sls_vol::varchar(20) as un_sls_vol,
measure_name::varchar(100) as acct_hier_desc,
measure_code::varchar(100) as acct_hier_shrt_desc,
current_timestamp()::timestamp_ntz(9) as crt_dttm,
current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source as a
  inner join edw_acct_hier as b
    on ltrim(rtrim(a.acct_num)) = ltrim(rtrim(b.acct_num))
)

select * from final