{{
    config(
        materialized="incremental",
        incremental_strategy= "merge",
        unique_key=  ['cust_no','sls_org','dstr_chnl','div'],
        merge_exclude_columns=["crt_dttm"]
    )
}}

--import CTE
with source as(
    select * from {{ ref('aspwks_integration__wks_itg_cust_sls') }}
),
--logical CTE
final as(
    select 
        mandt as clnt,
        kunnr as cust_no,
        vkorg as sls_org,
        vtweg as dstr_chnl,
        spart as div,
        ernam as obj_crt_prsn,
        erdat as rec_crt_dt,
        begru as auth_grp,
        loevm as cust_del_flag,
        versg as cust_stat_grp,
        aufsd as cust_ord_blk,
        kalks as prc_pcdr_asgn,
        kdgrp as cust_grp,
        bzirk as sls_dstrc,
        konda as prc_grp,
        pltyp as prc_list_typ,
        awahr as ord_prob_itm,
        inco1 as incoterm1,
        inco2 as incoterm2,
        lifsd as cust_delv_blk,
        autlf as cmplt_delv_sls_ord,
        antlf as max_no_prtl_delv_allw_itm,
        kztlf as prtl_delv_itm_lvl,
        kzazu as ord_comb_in,
        chspl as btch_splt_allw,
        lprio as delv_prir,
        eikto as vend_acct_no,
        vsbed as shipping_cond,
        faksd as bill_blk_cust,
        mrnkz as man_invc_maint,
        perfk as invc_dt,
        perrl as invc_list_sched,
        kvakz as cost_est_in,
        kvawt as val_lmt_cost_est,
        waers as crncy,
        klabc as cust_clas,
        ktgrd as acct_asgnmt_grp,
        vwerk as delv_plnt,
        vkgrp as sls_grp,
        vkbur as sls_ofc,
        vsort as itm_props,
        kvgr1 as cust_grp1,
        kvgr2 as cust_grp2,
        kvgr3 as cust_grp3,
        kvgr4 as cust_grp4,
        kvgr5 as cust_grp5,
        bokre as cust_rebt_in,
        boidt as rebt_indx_cust_strt_prd,
        kurst as exch_rt_typ,
        prfre as prc_dtrmn_id,
        prat1 as prod_attr_id1,
        prat2 as prod_attr_id2,
        prat3 as prod_attr_id3,
        prat4 as prod_attr_id4,
        prat5 as prod_attr_id5,
        prat6 as prod_attr_id6,
        prat7 as prod_attr_id7,
        prat8 as prod_attr_id8,
        prat9 as prod_attr_id9,
        prata as prod_attr_id10,
        zterm as pymt_key_term,
        zzsalesrep as persnl_num,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
--final select
select * from final