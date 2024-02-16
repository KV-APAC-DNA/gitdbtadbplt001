{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ["file_name"],
        post_hook="{{sap_transaction_processed_files('BWA_COPA17','vw_stg_sdl_sap_bw_copa17','itg_copa17_trans')}}"
        )
}}

--import CTE

with source as (
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_copa17')}}
),
sap_transactional_processed_files as (
    select * from {{ source('aspwks_integration', 'sap_transactional_processed_files') }}
),
--Logical CTE

final as(
    select 
    CAST(request_number as VARCHAR(100)) as request_number,
    CAST(data_packet as VARCHAR(50)) as data_packet,
    CAST(data_record as VARCHAR(100)) as data_record,
    CAST(fiscper as NUMBER) as fisc_yr_per,
    CAST(fiscvarnt as VARCHAR(2)) as fisc_yr_vrnt,
    CAST(fiscyear as NUMBER) as fisc_yr,
    CAST(calday as DATE) as cal_day,
    CAST(fiscper3 as NUMBER) as pstng_per,
    CAST(calmonth as NUMBER) as cal_yr_mo,
    CAST(calyear as NUMBER) as cal_yr,
    CAST(version as VARCHAR(3)) as vers,
    CAST(vtype as NUMBER) as val_type,
    CAST(comp_code as VARCHAR(4)) as co_cd,
    CAST(co_area as VARCHAR(4)) as cntl_area,
    CAST(profit_ctr as VARCHAR(10)) as prft_ctr,
    CAST(salesemply as NUMBER) as sls_emp_hist,
    CAST(salesorg as VARCHAR(4)) as sls_org,
    CAST(sales_grp as VARCHAR(3)) as sls_grp,
    CAST(sales_off as VARCHAR(4)) as sls_off,
    CAST(cust_group as VARCHAR(2)) as cust_grp,
    CAST(distr_chan as VARCHAR(2)) as dstn_chnl,
    CAST(sales_dist as VARCHAR(6)) as sls_dstrc,
    CAST(customer as VARCHAR(10)) as cust,
    CAST(material as VARCHAR(18)) as matl,
    CAST(cust_sales as VARCHAR(10)) as cust_sls_view,
    CAST(division as VARCHAR(2)) as div,
    CAST(plant as VARCHAR(4)) as plnt,
    CAST(zmercref as VARCHAR(5)) as mercia_ref,
    CAST(zz_mvgr1 as VARCHAR(3)) as b3_base_prod,
    CAST(zz_mvgr2 as VARCHAR(3)) as b4_vrnt,
    CAST(zz_mvgr3 as VARCHAR(3)) as b5_put_up,
    CAST(zz_mvgr4 as VARCHAR(3)) as b1_mega_brnd,
    CAST(zz_mvgr5 as VARCHAR(3)) as b2_brnd,
    CAST(region as VARCHAR(3)) as rgn,
    CAST(country as VARCHAR(3)) as ctry,
    CAST(prodh6 as VARCHAR(18))  as prod_minor,
    CAST(prodh5 as VARCHAR(18)) as prod_maj,
    CAST(prodh4 as VARCHAR(18)) as prod_fran,
    CAST(prodh3 as VARCHAR(18)) as fran,
    CAST(prodh2 as VARCHAR(18)) as fran_grp,
    CAST(prodh1 as VARCHAR(18)) as oper_grp,
    CAST(zfis_quar as NUMBER) as fisc_qtr,
    CAST(mat_sales as VARCHAR(18)) as matl2,
    CAST(bill_type as VARCHAR(4)) as bill_type,
    CAST(jjfiscwe as DATE) as fisc_wk,
    CAST(amocac as NUMBER) as amt_grp_crcy,
    CAST(amoccc as NUMBER) as amt_obj_crcy,
    CAST(currency as VARCHAR(5)) as crncy,
    CAST(obj_curr as VARCHAR(5)) as obj_crncy,
    CAST(account as VARCHAR(10)) as acct_num,
    CAST(chrt_accts as VARCHAR(4)) as chrt_of_acct,
    CAST(zz_wwme as VARCHAR(6)) as mgmt_entity,
    CAST(zsalesper as VARCHAR(30)) as sls_prsn_respons,
    CAST(bus_area as VARCHAR(4)) as busn_area,
    CAST(grossamttc as NUMBER) as ga,
    CAST(curkey_tc as VARCHAR(5)) as tc,
    CAST(mat_plant as VARCHAR(18)) as matl_plnt_view,
    CAST(quantity as NUMBER) as qty,
    CAST(unit as VARCHAR(10)) as uom,
    CAST(zqtyieu as NUMBER) as sls_vol_ieu,
    CAST(zunitieu as VARCHAR(10)) as un_sls_vol__ieu,
    CAST(zbpt_dc as VARCHAR(2)) as bpt_dstn_chnl,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    where not exists (
    select 
        act_file_name 
    from sap_transactional_processed_files 
    where target_table_name='itg_copa17_trans' and sap_transactional_processed_files.act_file_name=source.file_name
  )
)
--final select
select * from final