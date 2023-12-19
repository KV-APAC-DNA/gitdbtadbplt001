{{
    config(
        alias= "wks_edw_customer_sales_dim",
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="table",
        tags=["daily"]
    )
}}

--import CTE
with sources as(
     select A.*,
                itg_sls_grp_text.DE as sls_grp_desc,
                itg_sls_off_text.DE as sls_ofc_desc
            from {{ ref('aspitg_integration__itg_cust_sls') }} A
                left outer join {{ ref('aspitg_integration__itg_sls_grp_text') }} itg_sls_grp_text on A.sls_grp = itg_sls_grp_text.sls_grp
                and itg_sls_grp_text.lang_key = 'E'
                left outer join {{ ref('aspitg_integration__itg_sls_off_text') }} itg_sls_off_text on A.sls_ofc = itg_sls_off_text.sls_off
                and itg_sls_off_text.lang_key = 'E'
),

sources2 as(
   SELECT clnt,
        src.cust_no,
        src.sls_org,
        src.dstr_chnl,
        src.div,
        obj_crt_prsn,
        rec_crt_dt,
        auth_grp,
        cust_del_flag,
        cust_stat_grp,
        cust_ord_blk,
        prc_pcdr_asgn,
        cust_grp,
        sls_dstrc,
        prc_grp,
        prc_list_typ,
        ord_prob_itm,
        incoterm1,
        incoterm2,
        cust_delv_blk,
        cmplt_delv_sls_ord,
        max_no_prtl_delv_allw_itm,
        prtl_delv_itm_lvl,
        ord_comb_in,
        btch_splt_allw,
        delv_prir,
        vend_acct_no,
        shipping_cond,
        bill_blk_cust,
        man_invc_maint,
        invc_dt,
        invc_list_sched,
        cost_est_in,
        val_lmt_cost_est,
        crncy,
        cust_clas,
        acct_asgnmt_grp,
        delv_plnt,
        sls_grp,
        sls_grp_desc,
        sls_ofc,
        sls_ofc_desc,
        itm_props,
        cust_grp1,
        cust_grp2,
        cust_grp3,
        cust_grp4,
        cust_grp5,
        cust_rebt_in,
        rebt_indx_cust_strt_prd,
        exch_rt_typ,
        prc_dtrmn_id,
        prod_attr_id1,
        prod_attr_id2,
        prod_attr_id3,
        prod_attr_id4,
        prod_attr_id5,
        prod_attr_id6,
        prod_attr_id7,
        prod_attr_id8,
        prod_attr_id9,
        prod_attr_id10,
        pymt_key_term,
        persnl_num,
        CRT_DTTM,
        UPDT_DTTM
    FROM (
            sources
        )src
),
--logical CTE
final as(
    select * from sources2
)
--final select
select * from final