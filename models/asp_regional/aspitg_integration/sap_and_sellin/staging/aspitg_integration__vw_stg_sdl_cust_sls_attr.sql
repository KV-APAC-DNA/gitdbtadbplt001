with

source as (

    select * from {{ source('bwa_access', 'bwa_customer_sales') }}

),

final as (
    select
        coalesce(division,'') as division,
        coalesce(distr_chan,'') as distr_chan,
        coalesce(salesorg,'') as salesorg,
        coalesce(cust_sales,'') as cust_sales,
        coalesce(accnt_asgn,'') as accnt_asgn,
        coalesce(cust_cla,'') as cust_cla,
        coalesce(cust_group,'') as cust_group,
        coalesce(cust_grp1,'') as cust_grp1,
        coalesce(cust_grp2,'') as cust_grp2,
        coalesce(cust_grp3,'') as cust_grp3,
        coalesce(cust_grp4,'') as cust_grp4,
        coalesce(cust_grp5,'') as cust_grp5,
        coalesce(c_ctr_area,'') as c_ctr_area,
        coalesce(incoterms,'') as incoterms,
        coalesce(incoterms2,'') as incoterms2,
        coalesce(plant,'') as plant,
        coalesce(pmnttrms,'') as pmnttrms,
        coalesce(sales_dist,'') as sales_dist,
        coalesce(sales_grp,'') as sales_grp,
        coalesce(sales_off,'') as sales_off,
        coalesce(bic_zcrslsemp,'') as cur_sls_emp,
        coalesce(bic_zlcus_gr1,'') as lcl_cust_grp_1,
        coalesce(bic_zlcus_gr2,'') as lcl_cust_grp_2,
        coalesce(bic_zlcus_gr3,'') as lcl_cust_grp_3,
        coalesce(bic_zlcus_gr4,'') as lcl_cust_grp_4,
        coalesce(bic_zlcus_gr5,'') as lcl_cust_grp_5,
        coalesce(bic_zlcus_gr6,'') as lcl_cust_grp_6,
        coalesce(bic_zlcus_gr7,'') as lcl_cust_grp_7,
        coalesce(bic_zlcus_gr8,'') as lcl_cust_grp_8,
        coalesce(customer,'') as customer,
        coalesce(bic_zpr_pr0c,'') as prc_proc,
        coalesce(price_grp,'') as prc_grp,
        coalesce(price_list,'') as prc_lst_type,
        coalesce(ship_cond,'') as shpg_con,
        coalesce(bic_zpditlvl,'') as par_del,
        coalesce(bic_zmaxpd,'') as max_num_pa,
        coalesce(bic_zpcust,'') as prnt_cust_key,
        coalesce(bic_zbanner,'') as bnr_key,
        coalesce(bic_zbannerf,'') as bnr_frmt_key,
        coalesce(bic_zgotomod,'') as go_to_mdl_key,
        coalesce(bic_zchannel,'') as chnl_key,
        coalesce(bic_zschannel,'') as sub_chnl_key,
        coalesce(bic_zcsegment,'') as segmt_key,
        coalesce(bic_zcustset1,'') as cust_set_1,
        coalesce(bic_zcustset2,'') as cust_set_2,
        coalesce(bic_zcustset3,'') as cust_set_3,
        coalesce(bic_zcustset4,'') as cust_set_4,
        coalesce(bic_zcustset5,'') as cust_set_5
    from source
)
select * from final