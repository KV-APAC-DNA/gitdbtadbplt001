--{{
    config(
        materialized="incremental",
        incremental_strategy = "append"        
    )
}}

with edw_copa_trans_fact as
(
    select * from aspedw_integration.edw_copa_trans_fact
    --{{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
edw_copa_plan_fact as
(
    select * from snapaspedw_integration.edw_copa_plan_fact
    --{{ ref('aspedw_integration__edw_copa_plan_fact') }}
),
itg_pvc_input_content as
(
    select * from {{ ref('aspitg_integration__itg_pvc_input_content') }}
),
final as
(
    select  
    COPA_TRANS_FACT.co_cd,
    COPA_TRANS_FACT.prft_ctr,
    COPA_TRANS_FACT.matl_num,
    COPA_TRANS_FACT.vers,
    COPA_TRANS_FACT.val_type_rpt,
    COPA_TRANS_FACT.fisc_yr_per,
    COPA_TRANS_FACT.fx_amt_cntl_area_crncy,
    COPA_TRANS_FACT.amt_cntl_area_crncy,
    COPA_TRANS_FACT.crncy_key,
    COPA_TRANS_FACT.amt_obj_crncy,
    COPA_TRANS_FACT.obj_crncy_co_obj,
    COPA_TRANS_FACT.grs_amt_trans_crncy,
    COPA_TRANS_FACT.crncy_key_trans_crncy,
    COPA_TRANS_FACT.qty,
    COPA_TRANS_FACT.uom,
    COPA_TRANS_FACT.sls_vol,
    COPA_TRANS_FACT.un_sls_vol,
    ACTUAL_MATERIAL.content,
    ACTUAL_MATERIAL.content_short,
    ACTUAL_MATERIAL.content_number,
    CASE WHEN COPA_TRANS_FACT.qty IS NULL OR ACTUAL_MATERIAL.content_number IS NULL THEN COPA_TRANS_FACT.qty
        ELSE COPA_TRANS_FACT.qty*ACTUAL_MATERIAL.content_number
        END AS nts_qty_in_net_contents,
    CAST(SUBSTRING(COPA_TRANS_FACT.fisc_yr_per,1,4)||'-'||SUBSTRING(COPA_TRANS_FACT.fisc_yr_per,6,2)||'-'||'01' AS DATE)  AS fisc_per_date
    FROM
    (SELECT 
        co_cd,
        prft_ctr,
        matl_num,
        vers,
        val_type_rpt,
        fisc_yr_per,
        sum(fx_amt_cntl_area_crncy) AS fx_amt_cntl_area_crncy,
        sum(amt_cntl_area_crncy) AS amt_cntl_area_crncy,
        crncy_key,
        sum(amt_obj_crncy) AS amt_obj_crncy,
        obj_crncy_co_obj,
        sum(grs_amt_trans_crncy) AS grs_amt_trans_crncy,
        crncy_key_trans_crncy,
        sum(qty) AS qty,
        uom,
        sum(sls_vol) AS sls_vol,
        un_sls_vol
    FROM
    edw_copa_trans_fact 
    where  edw_copa_trans_fact.acct_hier_desc in ('Net Trade Sales') AND
    edw_copa_trans_fact.fisc_yr_per in (9999999)
    group by co_cd,
        prft_ctr,
        matl_num,
        vers,
        val_type_rpt,
        fisc_yr_per,
        crncy_key,
        obj_crncy_co_obj,
        crncy_key_trans_crncy,
        uom,
        un_sls_vol) COPA_TRANS_FACT
        LEFT JOIN
        (select * from  itg_pvc_input_content) ACTUAL_MATERIAL
        ON COPA_TRANS_FACT.matl_num=ACTUAL_MATERIAL.matl_num

    UNION ALL

    select  
    COPA_PLAN_FACT.co_cd,
    COPA_PLAN_FACT.prft_ctr,
    COPA_PLAN_FACT.matl_num,
    COPA_PLAN_FACT.vers,
    COPA_PLAN_FACT.val_type as val_type_rpt,
    COPA_PLAN_FACT.fisc_yr_per,
    null as fx_amt_cntl_area_crncy,
    null as amt_cntl_area_crncy,
    null as crncy_key,
    COPA_PLAN_FACT.amt_obj_crncy,
    COPA_PLAN_FACT.obj_crncy_co_obj,
    null as grs_amt_trans_crncy,    
    null as crncy_key_trans_crncy, 
    COPA_PLAN_FACT.qty,
    COPA_PLAN_FACT.uom,
    null as sls_vol,
    null as un_sls_vol,
    PLAN_MATERIAL.content,
    PLAN_MATERIAL.content_short,
    PLAN_MATERIAL.content_number,
    CASE WHEN COPA_PLAN_FACT.qty IS NULL OR PLAN_MATERIAL.content_number IS NULL THEN COPA_PLAN_FACT.qty
        ELSE COPA_PLAN_FACT.qty*PLAN_MATERIAL.content_number
        END AS nts_qty_in_net_contents,
    CAST(SUBSTRING(COPA_PLAN_FACT.fisc_yr_per,1,4)||'-'||SUBSTRING(COPA_PLAN_FACT.fisc_yr_per,6,2)||'-'||'01' AS DATE)  AS fisc_per_date
    FROM
    (SELECT co_cd,
        prft_ctr,
        matl_num,
        vers,
        val_type,
        fisc_yr_per,
        sum(amt_obj_crcy) as amt_obj_crncy,
        obj_crncy AS obj_crncy_co_obj,
        sum(qty) as qty,
        uom
    FROM edw_copa_plan_fact 
    where edw_copa_plan_fact.acct_hier_shrt_desc in ('NTS')
    and edw_copa_plan_fact.vers in ('999')
    and edw_copa_plan_fact.fisc_yr_per in (9999999)  
    group by co_cd,
        prft_ctr,
        matl_num,
        vers,
        val_type,
        fisc_yr_per,
        obj_crncy,
        uom) COPA_PLAN_FACT
        LEFT JOIN 
        (select * from itg_pvc_input_content)  PLAN_MATERIAL 
        ON COPA_PLAN_FACT.matl_num=PLAN_MATERIAL.matl_num
)
select co_cd::varchar(4) as co_cd,
    null::varchar(4) as cntl_area,
    prft_ctr::varchar(10) as prft_ctr,
    null::varchar(4) as sls_org,
    matl_num::varchar(18) as matl_num,
    null::varchar(10) as cust_num,
    null::varchar(2) as div,
    null::varchar(4) as plnt,
    null::varchar(4) as chrt_acct,
    null::varchar(10) as acct_num,
    null::varchar(2) as dstr_chnl,
    null::varchar(2) as fisc_yr_var,
    vers::varchar(3) as vers,
    null::varchar(1) as bw_delta_upd_mode,
    null::varchar(4) as bill_typ,
    null::varchar(4) as sls_ofc,
    null::varchar(3) as ctry_key,
    null::varchar(10) as sls_deal,
    null::varchar(3) as sls_grp,
    null::number(38,0) as sls_emp_hist,
    null::varchar(6) as sls_dist,
    null::varchar(2) as cust_grp,
    null::varchar(10) as cust_sls,
    null::varchar(4) as buss_area,
    val_type_rpt::number(38,0) as val_type_rpt,
    null::varchar(5) as mercia_ref,
    null::varchar(20) as caln_day,
    null::number(38,0) as caln_yr_mo,
    null::number(38,0) as fisc_yr,
    null::number(38,0) as pstng_per,
    fisc_yr_per::number(38,0) as fisc_yr_per,
    null::varchar(3) as b3_base_prod,
    null::varchar(3) as b4_var,
    null::varchar(3) as b5_put_up,
    null::varchar(3) as b1_mega_brnd,
    null::varchar(3) as b2_brnd,
    null::varchar(3) as reg,
    null::varchar(18) as prod_minor,
    null::varchar(18) as prod_maj,
    null::varchar(18) as prod_fran,
    null::varchar(18) as fran,
    null::varchar(18) as gran_grp,
    null::varchar(18) as oper_grp,
    null::varchar(30) as sls_prsn_resp,
    null::varchar(18) as matl_sls,
    null::varchar(18) as prod_hier,
    null::varchar(6) as mgmt_entity,
    fx_amt_cntl_area_crncy::number(20,5) as fx_amt_cntl_area_crncy,
    amt_cntl_area_crncy::number(20,5) as amt_cntl_area_crncy,
    crncy_key::varchar(5) as crncy_key,
    amt_obj_crncy::number(20,5) as amt_obj_crncy,
    obj_crncy_co_obj::varchar(5) as obj_crncy_co_obj,
    grs_amt_trans_crncy::number(20,5) as grs_amt_trans_crncy,
    crncy_key_trans_crncy::varchar(5) as crncy_key_trans_crncy,
    qty::number(20,5) as qty,
    uom::varchar(20) as uom,
    sls_vol::number(20,5) as sls_vol,
    un_sls_vol::varchar(20) as un_sls_vol,
    null::varchar(100) as acct_hier_desc,
    null::varchar(100) as acct_hier_shrt_desc,
    null::timestamp_ntz(9) as crt_dttm,
    null::timestamp_ntz(9) as updt_dttm,
    content::varchar(20) as content,
    content_short::varchar(10) as content_short,
    content_number::number(20,3) as content_number,
    nts_qty_in_net_contents::number(20,3) as nts_qty_in_net_contents,
    fisc_per_date::date as fisc_per_date
 from final