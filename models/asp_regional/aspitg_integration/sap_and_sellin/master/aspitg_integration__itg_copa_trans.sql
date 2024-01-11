{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["request_number","data_packet","data_record"],
        merge_exclude_columns= ["crt_dttm"]
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspwks_integration__wks_itg_copa_trans') }}
),

--Logical CTE

--Final CTE
final as (
select
request_number::varchar(100) as request_number,
data_packet::varchar(50) as data_packet,
data_record::varchar(100) as data_record,
comp_code::varchar(4) as co_cd,
co_area::varchar(4) as cntl_area,
profit_ctr::varchar(10) as prft_ctr,
salesorg::varchar(4) as sls_org,
material::varchar(18) as matl,
customer::varchar(10) as cust_num,
division::varchar(2) as div,
plant::varchar(4) as plnt,
chrt_accts::varchar(4) as chrt_acct,
account::varchar(10) as acct_num,
distr_chan::varchar(2) as dstr_chnl,
fiscvarnt::varchar(2) as fisc_yr_var,
version::varchar(3) as vers,
recordmode::varchar(1) as bw_delta_upd_mode,
bill_type::varchar(4) as bill_typ,
sales_off::varchar(4) as sls_off,
country::varchar(3) as cntry_key,
salesdeal::varchar(10) as sls_deal,
sales_grp::varchar(3) as sls_grp,
salesemply as sls_emp_hist,
sales_dist::varchar(6) as sls_dist,
cust_group::varchar(2) as cust_grp,
cust_sales::varchar(10) as cust_sls,
bus_area::varchar(4) as buss_area,
vtype as val_type_rpt,
zmercref::varchar(5) as mercia_ref,
calday::varchar(20) as caln_day,
calmonth as caln_yr_mo,
fiscyear as fisc_yr,
fiscper3 as pstng_per,
fiscper as fisc_yr_per,
zz_mvgr1::varchar(3) as b3_base_prod,
zz_mvgr2::varchar(3) as b4_var,
zz_mvgr3::varchar(3) as b5_put_up,
zz_mvgr4::varchar(3) as b1_mega_brnd,
zz_mvgr5::varchar(3) as b2_brnd,
region::varchar(3) as reg,
prodh6::varchar(18) as prod_minor,
prodh5::varchar(18) as prod_maj,
prodh4::varchar(18) as prod_fran,
prodh3::varchar(18) as fran,
prodh2::varchar(18) as gran_grp,
prodh1::varchar(18) as oper_grp,
zsalesper::varchar(30) as sls_prsn_resp,
mat_sales::varchar(18) as matl_sls,
prod_hier::varchar(18) as prod_hier,
zz_wwme::varchar(6) as mgmt_entity,
zfamocac::number(20,5) as fx_amt_cntl_area_crncy,
amocac::number(20,5) as amt_cntl_area_crncy,
currency::varchar(5) as crncy_key,
amoccc::number(20,5) as amt_obj_crncy,
obj_curr::varchar(5) as obj_crncy_co_obj,
grossamttc::number(20,5) as grs_amt_trans_crncy,
curkey_tc::varchar(5) as crncy_key_trans_crncy,
quantity::number(20,5) as qty,
unit::varchar(20) as uom,
zqtyieu::number(20,5) as sls_vol,
zunitieu::varchar(20) as un_sls_vol,
current_timestamp()::timestamp_ntz(9) as crt_dttm,
current_timestamp()::timestamp_ntz(9) as updt_dttm
from source
)

--Final select
select * from final 