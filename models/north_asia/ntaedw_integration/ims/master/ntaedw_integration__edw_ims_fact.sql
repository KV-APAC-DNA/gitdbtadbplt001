{{
    config(
        materialized="incremental",
        incremental_strategy='append',
        pre_hook="{% if var('ims_job_to_execute') == 'tw_ims_distributor_standard_sell_out' %}
            {% if is_incremental() %}
            delete from {{this}} where dstr_cd in ( select distinct dstr_cd from {{ ref('ntawks_integration__wks_edw_ims_sls_std') }});
            {% endif %}
            {% elif var('ims_job_to_execute') == 'kr_gt_sellout' %}
            {% if is_incremental() %}
            delete from {{this}} where upper(dstr_nm) in ('DAISO','HYUNDAI','LOTTE','AK','(JU) HJ LIFE','BO YOUNG JONG HAP LOGISTICS','DA IN SANG SA','DONGBU LSD','DU BAE RO YU TONG','IL DONG HU DI S DEOK SEONG SANG SA','JUNGSEOK','KOREA DAE DONG LTD','NU RI ZON','LOTTE LOGISTICS YANG JU','NACF') and upper(dstr_cd) in ('NH','OTC');
            {% endif %}
            {% elif var('ims_job_to_execute') == 'kr_ecommerce_sellout_tran' %}
            {% if is_incremental() %}
            delete from {{this}} where  ctry_cd = 'KR' and (dstr_nm = 'UNITOA' or dstr_nm = 'TCAKOREA');
            {% endif %}
            {% elif var('ims_job_to_execute') == 'na_trxn_ims' %}
            {% if is_incremental() %}
            delete from {{this}} edw_ims_fact using (select distinct dstr_cd from {{ ref('ntawks_integration__wks_edw_ims_sls') }}) wks_edw_ims_sls where edw_ims_fact.dstr_cd = wks_edw_ims_sls.dstr_cd
            {% endif %}
            {% endif %}
            "
    )
}}
with wks_edw_ims_sls_std as (
    select * from {{ ref('ntawks_integration__wks_edw_ims_sls_std') }}
),
itg_kr_gt_sellout as (
    select * from {{ ref('ntaitg_integration__itg_kr_gt_sellout')}}
),
wks_edw_ims_sls as (
    select * from {{ ref('ntawks_integration__wks_edw_ims_sls') }}
),
itg_kr_ecommerce_sellout as (
    select * from {{ ref('ntaitg_integration__itg_kr_ecommerce_sellout') }}
)
{% if var('ims_job_to_execute') == 'tw_ims_distributor_standard_sell_out' %}
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
{% elif var('ims_job_to_execute') == 'kr_gt_sellout' %}
,
Korea as (
select ims_txn_dt::date as ims_txn_dt,
        dstr_cd::varchar(10) as dstr_cd,
        dstr_nm::varchar(100) as dstr_nm,
        cust_cd::varchar(50) as cust_cd,
        cust_nm::varchar(100) as cust_nm,
        prod_cd::varchar(255) as prod_cd,
        prod_nm::varchar(255) as prod_nm,
        null::date as rpt_per_strt_dt,
        null::date as rpt_per_end_dt,
        ean_num::varchar(20) as ean_num,
        null::varchar(10) as uom,
        unit_prc::number(21,5) as unit_prc,
        sls_amt::number(21,5) as sls_amt,
        sls_qty::number(18,0) as sls_qty,
        null::number(18,0) as rtrn_qty,
        null::number(21,5) as rtrn_amt,
        null::varchar(100) as ship_cust_nm,
        null::varchar(20) as cust_cls_grp,
        null::varchar(20) as cust_sub_cls,
        null::varchar(50) as prod_spec,
        null::varchar(100) as itm_agn_nm,
        null::varchar(20) as ordr_co,
        null::varchar(100) as rtrn_rsn,
        null::varchar(10) as sls_ofc_cd,
        null::varchar(10) as sls_grp_cd,
        null::varchar(20) as sls_ofc_nm,
        null::varchar(20) as sls_grp_nm,
        null::varchar(10) as acc_type,
        null::varchar(20) as co_cd,
        null::varchar(20) as sls_rep_cd,
        null::varchar(50) as sls_rep_nm,
        null::date as doc_dt,
        null::varchar(20) as doc_num,
        null::varchar(15) as invc_num,
        null::varchar(100) as remark_desc,
        null::number(18,0) as gift_qty,
        null::number(21,5) as sls_bfr_tax_amt,
        null::number(21,5) as sku_per_box,
        ctry_cd::varchar(2) as ctry_cd,
        crncy_cd::varchar(3) as crncy_cd,
        current_timestamp AS CRT_DTTM,
        current_timestamp AS UPDT_DTTM,
        null::number(16,5) as prom_sls_amt,
        null::number(16,5) as prom_rtrn_amt,
        null::number(16,5) as prom_prc_amt,
        null::varchar(255) as sap_code,
        null::varchar(20) as sku_type,
        sub_customer_code::varchar(50) as sub_customer_code,
        sub_customer_name::varchar(100) as sub_customer_name,
        null::varchar(10) as sales_priority,
        null::number(21,5) as sales_stores,
        null::number(21,5) as sales_rate
from itg_kr_gt_sellout
where upper(dstr_nm) in 
('DAISO','HYUNDAI','LOTTE','AK','(JU) HJ LIFE','BO YOUNG JONG HAP LOGISTICS','DA IN SANG SA','DONGBU LSD','DU BAE RO YU TONG','IL DONG HU DI S DEOK SEONG SANG SA','JUNGSEOK','KOREA DAE DONG LTD','NU RI ZON','LOTTE LOGISTICS YANG JU','NACF')
and upper(dstr_cd) in ('NH','OTC')
)
select * from korea
{% elif var('ims_job_to_execute') == 'kr_ecommerce_sellout_tran' %}
,
Korea_unitoa as (
select
transaction_date::date as ims_txn_dt as ims_txn_dt,
customer_code::varchar(10) as dstr_cd,
customer_name::varchar(100) as dstr_nm,
NULL::varchar(50) as cust_cd,
sub_customer_name::varchar(100) as cust_nm,
NULL::varchar(255) as prod_cd,
product_name::varchar(255) as prod_nm,
NULL::date as rpt_per_strt_dt,
NULL::date as rpt_per_end_dt,
ean_number::varchar(20) as ean_num,
NULL::varchar(10) as uom,
NULL::number(21, 5) as unit_prc,
sellout_amount::number(21, 5) as sls_amt,
sellout_qty::number(18,0), as sls_qty,
NULL::number(18,0), as rtrn_qty,
NULL::number(21, 5) as rtrn_amt,
NULL::varchar(100) as ship_cust_nm,
NULL::varchar(20) as cust_cls_grp,
NULL::varchar(20) as cust_sub_cls,
NULL::varchar(50) as prod_spec,
NULL::varchar(100) as itm_agn_nm,
NULL::varchar(20) as ordr_co,
NULL::varchar(100) as rtrn_rsn,
NULL::varchar(10) as sls_ofc_cd,
NULL::varchar(10) as sls_grp_cd,
NULL::varchar(20) as sls_ofc_nm,
NULL::varchar(20) as sls_grp_nm,
NULL::varchar(10) as acc_type,
NULL::varchar(20) as co_cd,
NULL::varchar(20) as sls_rep_cd,
NULL::varchar(50) as sls_rep_nm,
NULL::date as doc_dt,
NULL::varchar(20) as doc_num,
NULL::varchar(15) as invc_num,
NULL::varchar(100) as remark_desc,
NULL::number(18, 0) as gift_qty,
NULL::number(21, 5) as sls_bfr_tax_amt,
NULL::number(21, 5) as sku_per_box,
ctry_cd::varchar(2) as ctry_cd,
crncy_cd::varchar(3) as crncy_cd,
sysdate()::timestamp_ntz(9) crt_dttm,
NULL as updt_dttm,
NULL::number(16, 5) as prom_sls_amt,
NULL::number(16, 5) as prom_rtrn_amt,
NULL::number(16, 5) as prom_prc_amt,
sap_code::varchar(255) as sap_code,
sku_type::varchar(20) as sku_type,
null::varchar(50) as sub_customer_code,
null::varchar(100) as sub_customer_name,
null::varchar(10) as sales_priority,
null::number(21, 5) as sales_stores,
null::number(21, 5) as sales_rate
from
itg_kr_ecommerce_sellout
where
customer_name = 'UNITOA'
),
korea_tca as (
select
transaction_date::date as ims_txn_dt,
customer_code::varchar(10) as dstr_cd,
customer_name::varchar(100) as dstr_nm,
NULL::varchar(50) as cust_cd,
sub_customer_name::varchar(100) as cust_nm,
NULL::varchar(255) as prod_cd,
product_name::varchar(255) as prod_nm,
NULL::date as rpt_per_strt_dt,
NULL::date as rpt_per_end_dt as rpt_per_end_dt,
ean_number::varchar(20) as ean_num,
NULL::varchar(10) as uom,
NULL::number(21, 5) as unit_prc,
sellout_amount::number(21, 5) as sls_amt,
sellout_qty::number(18, 0) as sls_qty,
NULL::number(18, 0) as rtrn_qty,
NULL::number(21, 5) as rtrn_amt,
NULL::varchar(100) as ship_cust_nm,
NULL::varchar(20) as cust_cls_grp,
NULL::varchar(20) as cust_sub_cls,
NULL::varchar(50) as prod_spec,
NULL::varchar(100) as itm_agn_nm,
NULL::varchar(20) as ordr_co,
NULL::varchar(100) as rtrn_rsn,
NULL::varchar(10) as sls_ofc_cd,
NULL::varchar(10) as sls_grp_cd,
NULL::varchar(20) as sls_ofc_nm,
NULL::varchar(20) as sls_grp_nm,
NULL::varchar(10) as acc_type,
NULL::varchar(20) as co_cd,
NULL::varchar(20) as sls_rep_cd,
NULL::varchar(50) as sls_rep_nm,
NULL::date as doc_dt,
NULL::varchar(20) as doc_num,
NULL::varchar(15) as invc_num,
NULL::varchar(100) as remark_desc,
NULL::number(18, 0) as gift_qty,
NULL::number(21, 5) as sls_bfr_tax_amt,
NULL::number(21, 5) as sku_per_box,
ctry_cd::varchar(2) as ctry_cd,
crncy_cd::varchar(3) as crncy_cd,
sysdate()::timestamp_ntz(9) crt_dttm,
NULL as updt_dttm,
NULL::number(16, 5) as prom_sls_amt,
NULL::number(16, 5) as prom_rtrn_amt,
NULL::number(16, 5) as prom_prc_amt,
sap_code::varchar(255) as sap_code,
sku_type::varchar(20) as sku_type,
null::varchar(50) as sub_customer_code,
null::varchar(100) as sub_customer_name,
null::varchar(10) as sales_priority,
null::number(21, 5) as sales_stores,
null::number(21, 5) as sales_rate
from
itg_kr_ecommerce_sellout
where
customer_name = 'TCAKOREA'
),
final as (
    select * from Korea_unitoa
    union all 
    select * from korea_tca
)
select * from final
{% elif var('ims_job_to_execute') == 'na_trxn_ims' %}
,
hk as (
select
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
crt_dttm::timestamp_ntz(9) as crt_dttm,
current_timestamp()::timestamp_ntz(9) as updt_dttm,
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
from wks_edw_ims_sls
)
select * from hk

{% endif %}