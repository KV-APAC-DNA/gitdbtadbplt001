with

source as (

    select * from {{ source('bwa_access', 'bwa_customer_sales') }}

),

final as (
    select
        division as division,
        distr_chan as distr_chan,
        salesorg as salesorg,
        cust_sales as cust_sales,
        accnt_asgn as accnt_asgn,
        cust_cla as cust_cla,
        cust_group as cust_group,
        cust_grp1 as cust_grp1,
        cust_grp2 as cust_grp2,
        cust_grp3 as cust_grp3,
        cust_grp4 as cust_grp4,
        cust_grp5 as cust_grp5,
        c_ctr_area as c_ctr_area,
        incoterms as incoterms,
        incoterms2 as incoterms2,
        plant as plant,
        pmnttrms as pmnttrms,
        sales_dist as sales_dist,
        sales_grp as sales_grp,
        sales_off as sales_off,
        bic_zcrslsemp as cur_sls_emp,
        bic_zlcus_gr1 as lcl_cust_grp_1,
        bic_zlcus_gr2 as lcl_cust_grp_2,
        bic_zlcus_gr3 as lcl_cust_grp_3,
        bic_zlcus_gr4 as lcl_cust_grp_4,
        bic_zlcus_gr5 as lcl_cust_grp_5,
        bic_zlcus_gr6 as lcl_cust_grp_6,
        bic_zlcus_gr7 as lcl_cust_grp_7,
        bic_zlcus_gr8 as lcl_cust_grp_8,
        customer as customer,
        bic_zpr_pr0c as prc_proc,
        price_grp as prc_grp,
        price_list as prc_lst_type,
        ship_cond as shpg_con,
        bic_zpditlvl as par_del,
        bic_zmaxpd as max_num_pa,
        bic_zpcust as prnt_cust_key,
        bic_zbanner as bnr_key,
        bic_zbannerf as bnr_frmt_key,
        bic_zgotomod as go_to_mdl_key,
        bic_zchannel as chnl_key,
        bic_zschannel as sub_chnl_key,
        bic_zcsegment as segmt_key,
        bic_zcustset1 as cust_set_1,
        bic_zcustset2 as cust_set_2,
        bic_zcustset3 as cust_set_3,
        bic_zcustset4 as cust_set_4,
        bic_zcustset5 as cust_set_5
    from source
)
select * from final