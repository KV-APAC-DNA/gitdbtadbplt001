{{
    config(
        materialized="incremental",
        incremental_strategy='append',
        pre_hook="{% if var('job_to_execute') == 'tw_ims_distributor_standard_sell_out' %}
            delete from {{this}} where dstr_cd in ( select distinct dstr_cd from {{ ref('ntawks_integration__wks_edw_ims_sls_std') }};
            {% elif var('job_to_execute') == 'kr_edw_ims_fact' %}
            delete from {{this}} where upper(dstr_nm) in ('DAISO','HYUNDAI','LOTTE','AK','(JU) HJ LIFE','BO YOUNG JONG HAP LOGISTICS','DA IN SANG SA','DONGBU LSD','DU BAE RO YU TONG','IL DONG HU DI S DEOK SEONG SANG SA','JUNGSEOK','KOREA DAE DONG LTD','NU RI ZON','LOTTE LOGISTICS YANG JU','NACF') and upper(dstr_cd) in ('NH','OTC')
            {% endif %}
            "
    )
}}
with wks_edw_ims_sls_std as (
    select * from {{ ref('ntawks_integration__wks_edw_ims_sls_std') }}
),
itg_kr_gt_sellout as (
    select * from {{ ref('ntaitg_integration__itg_kr_gt_sellout')}}

)
{% if var('job_to_execute') == 'tw_ims_distributor_standard_sell_out' %}
,
taiwan as
(
    SELECT 
        ims_txn_dt::date as ims_txn_dt,
        dstr_cd::varchar(10) as dstr_cd,
        dstr_nm::varchar(100) as dstr_nm,
        cust_cd::varchar(50) as cust_cd,
        cust_nm::varchar(100) as cust_nm,
        prod_cd::varchar(255) as prod_cd,
        prod_nm::varchar(255) as prod_nm,
        rpt_per_strt_dt::date as rpt_per_strt_dt,
        rpt_per_end_dt::date as rpt_per_end_dt,
        ean_num::varchar(20) as ean_num,
        uom::varchar(10) as uom,
        unit_prc::number(21,5) as unit_prc,
        sls_amt::number(21,5) as sls_amt,
        sls_qty::number(18,0) as sls_qty,
        rtrn_qty::number(18,0) as rtrn_qty,
        rtrn_amt::number(21,5) as rtrn_amt,
        ship_cust_nm::varchar(100) as ship_cust_nm,
        cust_cls_grp::varchar(20) as cust_cls_grp,
        cust_sub_cls::varchar(20) as cust_sub_cls,
        prod_spec::varchar(50) as prod_spec,
        itm_agn_nm::varchar(100) as itm_agn_nm,
        ordr_co::varchar(20) as ordr_co,
        rtrn_rsn::varchar(100) as rtrn_rsn,
        sls_ofc_cd::varchar(10) as sls_ofc_cd,
        sls_grp_cd::varchar(10) as sls_grp_cd,
        sls_ofc_nm::varchar(20) as sls_ofc_nm,
        sls_grp_nm::varchar(20) as sls_grp_nm,
        acc_type::varchar(10) as acc_type,
        co_cd::varchar(20) as co_cd,
        sls_rep_cd::varchar(20) as sls_rep_cd,
        sls_rep_nm::varchar(50) as sls_rep_nm,
        doc_dt::date as doc_dt,
        doc_num::varchar(20) as doc_num,
        invc_num::varchar(15) as invc_num,
        remark_desc::varchar(100) as remark_desc,
        gift_qty::number(18,0) as gift_qty,
        sls_bfr_tax_amt::number(21,5) as sls_bfr_tax_amt,
        sku_per_box::number(21,5) as sku_per_box,
        ctry_cd::varchar(2) as ctry_cd,
        crncy_cd::varchar(3) as crncy_cd,
        current_timestamp AS CRT_DTTM,
        current_timestamp AS UPDT_DTTM,
        prom_sls_amt::number(16,5) as prom_sls_amt,
        prom_rtrn_amt::number(16,5) as prom_rtrn_amt,
        prom_prc_amt::number(16,5) as prom_prc_amt,
        null::varchar(255) as sap_code,
        null::varchar(20) as sku_type,
        null::varchar(50) as sub_customer_code,
        null::varchar(100) as sub_customer_name,
        null::varchar(10) as sales_priority,
        null::number(21,5) as sales_stores,
        null::number(21,5) as sales_rate
    FROM wks_edw_ims_sls_std
)
select * from taiwan
{% elif var('job_to_execute') == 'kr_edw_ims_fact' %}
,
Korea as (
select ims_txn_dt,
       dstr_cd,
       dstr_nm,
       cust_cd,
       cust_nm,
       prod_cd,
       prod_nm,
       ean_num,
       unit_prc,
       sls_amt,
       sls_qty,
       ctry_cd,
       crncy_cd,
       crt_dttm,
       updt_dttm,
       sub_customer_code,
       sub_customer_name
from itg_kr_gt_sellout
where upper(dstr_nm) in 
('DAISO','HYUNDAI','LOTTE','AK','(JU) HJ LIFE','BO YOUNG JONG HAP LOGISTICS','DA IN SANG SA','DONGBU LSD','DU BAE RO YU TONG','IL DONG HU DI S DEOK SEONG SANG SA','JUNGSEOK','KOREA DAE DONG LTD','NU RI ZON','LOTTE LOGISTICS YANG JU','NACF')
)  and upper(dstr_cd) in ('NH','OTC')
select * from korea

{% endif %}