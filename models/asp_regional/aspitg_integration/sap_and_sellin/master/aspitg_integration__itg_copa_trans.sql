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
request_number as request_number,
data_packet as data_packet,
data_record as data_record,
comp_code as co_cd,
co_area as cntl_area,
profit_ctr as prft_ctr,
salesorg as sls_org,
material as matl,
customer as cust_num,
division as div,
plant as plnt,
chrt_accts as chrt_acct,
account as acct_num,
distr_chan as dstr_chnl,
fiscvarnt as fisc_yr_var,
version as vers,
recordmode as bw_delta_upd_mode,
bill_type as bill_typ,
sales_off as sls_off,
country as cntry_key,
salesdeal as sls_deal,
sales_grp as sls_grp,
salesemply as sls_emp_hist,
sales_dist as sls_dist,
cust_group as cust_grp,
cust_sales as cust_sls,
bus_area as buss_area,
vtype as val_type_rpt,
zmercref as mercia_ref,
calday as caln_day,
calmonth as caln_yr_mo,
fiscyear as fisc_yr,
fiscper3 as pstng_per,
fiscper as fisc_yr_per,
zz_mvgr1 as b3_base_prod,
zz_mvgr2 as b4_var,
zz_mvgr3 as b5_put_up,
zz_mvgr4 as b1_mega_brnd,
zz_mvgr5 as b2_brnd,
region as reg,
prodh6 as prod_minor,
prodh5 as prod_maj,
prodh4 as prod_fran,
prodh3 as fran,
prodh2 as gran_grp,
prodh1 as oper_grp,
zsalesper as sls_prsn_resp,
mat_sales as matl_sls,
prod_hier as prod_hier,
zz_wwme as mgmt_entity,
zfamocac as fx_amt_cntl_area_crncy,
amocac as amt_cntl_area_crncy,
currency as crncy_key,
amoccc as amt_obj_crncy,
obj_curr as obj_crncy_co_obj,
grossamttc as grs_amt_trans_crncy,
curkey_tc as crncy_key_trans_crncy,
quantity as qty,
unit as uom,
zqtyieu as sls_vol,
zunitieu as un_sls_vol,
current_timestamp()::timestamp_ntz(9) as crt_dttm,
current_timestamp()::timestamp_ntz(9) as updt_dttm
from source
)

--Final select
select * from final 