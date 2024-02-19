{{
    config(
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ['matl_num'],
        merge_exclude_columns= ["crt_dttm"]
    )
}}

--Import CTE
with wks_material_dim as (
    select * from {{ ref('aspwks_integration__wks_material_dim') }}
),

/*
These below CTE's are created to compensate the update statement which updates below mentioned columns on this table:
1. prodh1
2. prodh2
3. prodh3
4. prodh4
5. prodh5
6. prodh6
7. prodh1_txtmd
8. prodh2_txtmd
9. prodh3_txtmd
10.prodh4_txtmd
11.prodh5_txtmd
12.prodh6_txtmd
*/


itg_edw_material_dim_updt as (
    select 
        matl_num,
        prodh1,
        prodh2,
        prodh3,
        prodh4,
        prodh5,
        prodh6
    from {{ ref('aspitg_integration__itg_edw_material_dim_updt') }}
),

itg_prod_hier as (
    select * from {{ ref('aspitg_integration__itg_prod_hier') }}
),

itg_prod_hier_1 as (
    SELECT 
        txtmd,
        itg_edw_material_dim_updt.matl_num 
    FROM itg_prod_hier 
    INNER JOIN itg_edw_material_dim_updt ON itg_prod_hier.prod_hier=itg_edw_material_dim_updt.prodh1
),

itg_prod_hier_2 as (
    SELECT 
        txtmd,
        itg_edw_material_dim_updt.matl_num 
    FROM itg_prod_hier 
    INNER JOIN itg_edw_material_dim_updt ON itg_prod_hier.prod_hier=itg_edw_material_dim_updt.prodh2
),

itg_prod_hier_3 as (
    SELECT 
        txtmd,
        itg_edw_material_dim_updt.matl_num 
    FROM itg_prod_hier 
    INNER JOIN itg_edw_material_dim_updt ON itg_prod_hier.prod_hier=itg_edw_material_dim_updt.prodh3
),

itg_prod_hier_4 as (
    SELECT 
        txtmd,
        itg_edw_material_dim_updt.matl_num 
    FROM itg_prod_hier 
    INNER JOIN itg_edw_material_dim_updt ON itg_prod_hier.prod_hier=itg_edw_material_dim_updt.prodh4
),

itg_prod_hier_5 as (
    SELECT 
        txtmd,
        itg_edw_material_dim_updt.matl_num 
    FROM itg_prod_hier 
    INNER JOIN itg_edw_material_dim_updt ON itg_prod_hier.prod_hier=itg_edw_material_dim_updt.prodh5
),

itg_prod_hier_6 as (
    SELECT 
        txtmd,
        itg_edw_material_dim_updt.matl_num 
    FROM itg_prod_hier 
    INNER JOIN itg_edw_material_dim_updt ON itg_prod_hier.prod_hier=itg_edw_material_dim_updt.prodh6
),

edw_material_typ as (
    select * from {{ ref('aspedw_integration__edw_material_typ') }}
    where langu='E'
),

--Logical CTE
transformed as (
    select 
        source.matl_num::varchar(40) as matl_num,
        source.matl_desc::varchar(100) as matl_desc,
        source.crt_on::date as crt_on,
        source.crt_by_nm::varchar(12) as crt_by_nm,
        source.chg_dttm::date as chg_dttm,
        source.chg_by_nm::varchar(12) as chg_by_nm,
        source.maint_sts_cmplt_matl::varchar(15) as maint_sts_cmplt_matl,
        source.maint_sts::varchar(15) as maint_sts,
        source.fl_matl_del_clnt_lvl::varchar(10) as fl_matl_del_clnt_lvl,
        source.matl_type_cd::varchar(10) as matl_type_cd,
        source.indstr_sectr::varchar(10) as indstr_sectr,
        source.matl_grp_cd::varchar(20) as matl_grp_cd,
        source.old_matl_num::varchar(40) as old_matl_num,
        source.base_uom_cd::varchar(10) as base_uom_cd,
        source.prch_uom_cd::varchar(10) as prch_uom_cd,
        source.doc_num::varchar(22) as doc_num,
        source.doc_type::varchar(10) as doc_type,
        source.doc_vers::varchar(10) as doc_vers,
        source.pg_fmt__doc::varchar(10) as pg_fmt__doc,
        source.doc_chg_num::varchar(20) as doc_chg_num,
        source.pg_num_doc::varchar(10) as pg_num_doc,
        source.num_sht::number(18,0) as num_sht,
        source.prdtn_memo_txt::varchar(40) as prdtn_memo_txt,
        source.pg_fmt_prdtn_memo::varchar(10) as pg_fmt_prdtn_memo,
        source.size_dims_txt::varchar(32) as size_dims_txt,
        source.bsc_matl::varchar(100) as bsc_matl,
        source.indstr_std_desc::varchar(40) as indstr_std_desc,
        source.mercia_plan::varchar(10) as mercia_plan,
        source.prchsng_val_key::varchar(10) as prchsng_val_key,
        source.grs_wt_meas::number(13,3) as grs_wt_meas,
        source.net_wt_meas::number(13,3) as net_wt_meas,
        source.wt_uom_cd::varchar(10) as wt_uom_cd,
        source.vol_meas::number(13,3) as vol_meas,
        source.vol_uom_cd::varchar(10) as vol_uom_cd,
        source.cntnr_rqr::varchar(10) as cntnr_rqr,
        source.strg_cond::varchar(10) as strg_cond,
        source.temp_cond_ind::varchar(10) as temp_cond_ind,
        source.low_lvl_cd::varchar(10) as low_lvl_cd,
        source.trspn_grp::varchar(10) as trspn_grp,
        source.haz_matl_num::varchar(40) as haz_matl_num,
        source.div::varchar(10) as div,
        source.cmpt::varchar(10) as cmpt,
        source.ean_obsol::varchar(13) as ean_obsol,
        source.gr_prtd_qty::number(13,3) as gr_prtd_qty,
        source.prcmt_rule::varchar(10) as prcmt_rule,
        source.src_supl::varchar(10) as src_supl,
        source.seasn_cat::varchar(10) as seasn_cat,
        source.lbl_type_cd::varchar(10) as lbl_type_cd,
        source.lbl_form::varchar(10) as lbl_form,
        source.deact::varchar(10) as deact,
        source.prmry_upc_cd::varchar(40) as prmry_upc_cd,
        source.ean_cat::varchar(10) as ean_cat,
        source.lgth_meas::number(13,3) as lgth_meas,
        source.wdth_meas::number(13,3) as wdth_meas,
        source.hght_meas::number(13,3) as hght_meas,
        source.dim_uom_cd::varchar(10) as dim_uom_cd,
        source.prod_hier_cd::varchar(40) as prod_hier_cd,
        source.stk_tfr_chg_cost::varchar(10) as stk_tfr_chg_cost,
        source.cad_ind::varchar(10) as cad_ind,
        source.qm_prcmt_act::varchar(10) as qm_prcmt_act,
        source.allw_pkgng_wt::number(13,3) as allw_pkgng_wt,
        source.wt_unit::varchar(10) as wt_unit,
        source.allw_pkgng_vol::number(13,3) as allw_pkgng_vol,
        source.vol_unit::varchar(10) as vol_unit,
        source.exces_wt_tlrnc::number(3,1) as exces_wt_tlrnc,
        source.exces_vol_tlrnc::number(3,1) as exces_vol_tlrnc,
        source.var_prch_ord_unit::varchar(10) as var_prch_ord_unit,
        source.rvsn_lvl_asgn_matl::varchar(10) as rvsn_lvl_asgn_matl,
        source.configurable_matl_ind::varchar(10) as configurable_matl_ind,
        source.btch_mgmt_reqt_ind::varchar(10) as btch_mgmt_reqt_ind,
        source.pkgng_matl_type_cd::varchar(10) as pkgng_matl_type_cd,
        source.max_lvl_vol::number(3,0) as max_lvl_vol,
        source.stack_fact::number(18,0) as stack_fact,
        source.pkgng_matl_grp::varchar(10) as pkgng_matl_grp,
        source.auth_grp::varchar(10) as auth_grp,
        coalesce(source.vld_from_dt,'1900-01-01')::date as vld_from_dt,
        coalesce(source.del_dt,'1900-01-01')::date as del_dt,
        source.seasn_yr::varchar(10) as seasn_yr,
        source.prc_bnd_cat::varchar(10) as prc_bnd_cat,
        source.bill_of_matl::varchar(10) as bill_of_matl,
        source.extrnl_matl_grp_txt::varchar(40) as extrnl_matl_grp_txt,
        source.cross_plnt_cnfg_matl::varchar(40) as cross_plnt_cnfg_matl,
        source.matl_cat::varchar(10) as matl_cat,
        source.matl_coprod_ind::varchar(10) as matl_coprod_ind,
        source.fllp_matl_ind::varchar(10) as fllp_matl_ind,
        source.prc_ref_matl::varchar(40) as prc_ref_matl,
        source.cros_plnt_matl_sts::varchar(10) as cros_plnt_matl_sts,
        source.cros_dstn_chn_matl_sts::varchar(10) as cros_dstn_chn_matl_sts,
	    coalesce(source.cros_plnt_matl_sts_vld_dt,'1900-01-01')::date as cros_plnt_matl_sts_vld_dt,
        coalesce(source.chn_matl_vld_from_dt,'1900-01-01')::date as chn_matl_vld_from_dt,
        source.tax_clsn_matl::varchar(10) as tax_clsn_matl,
        source.catlg_prfl::varchar(20) as catlg_prfl,
        source.min_rmn_shlf_lif::number(4,0) as min_rmn_shlf_lif,
        source.tot_shlf_lif::number(4,0) as tot_shlf_lif,
        source.strg_pct::number(3,0) as strg_pct,
        source.cntnt_uom_cd::varchar(10) as cntnt_uom_cd,
        source.net_cntnt_meas::number(13,3) as net_cntnt_meas,
        source.cmpr_prc_unit::number(5,0) as cmpr_prc_unit,
        source.isr_matl_grp::varchar(40) as isr_matl_grp,
        source.grs_cntnt_meas::number(13,3) as grs_cntnt_meas,
        source.qty_conv_meth::varchar(10) as qty_conv_meth,
        source.intrnl_obj_num::number(18,0) as intrnl_obj_num,
        source.envmt_rlvnt::varchar(10) as envmt_rlvnt,
        source.prod_allc_dtrmn_proc::varchar(40) as prod_allc_dtrmn_proc,
        source.prc_prfl_vrnt::varchar(10) as prc_prfl_vrnt,
        source.matl_qual_disc::varchar(10) as matl_qual_disc,
        source.mfr_part_num::varchar(40) as mfr_part_num,
        source.mfr_num::varchar(10) as mfr_num,
        source.intrnl_inv_mgmt::varchar(40) as intrnl_inv_mgmt,
        source.mfr_part_prfl::varchar(10) as mfr_part_prfl,
        source.meas_usg_unit::varchar(10) as meas_usg_unit,
        source.rollout_seasn::varchar(10) as rollout_seasn,
        source.dngrs_goods_ind_prof::varchar(10) as dngrs_goods_ind_prof,
        source.hi_viscous_ind::varchar(10) as hi_viscous_ind,
        source.in_bulk_lqd_ind::varchar(10) as in_bulk_lqd_ind,
        source.lvl_explc_ser_num::varchar(10) as lvl_explc_ser_num,
        source.pkgng_matl_clse_pkgng::varchar(10) as pkgng_matl_clse_pkgng,
        source.appr_btch_rec_ind::varchar(10) as appr_btch_rec_ind,
        source.ovrd_chg_num::varchar(10) as ovrd_chg_num,
        source.matl_cmplt_lvl::number(18,0) as matl_cmplt_lvl,
        source.per_ind_shlf_lif_expn_dt::varchar(10) as per_ind_shlf_lif_expn_dt,
        source.rd_rule_sled::varchar(10) as rd_rule_sled,
        source.prod_cmpos_prtd_pkgng::varchar(10) as prod_cmpos_prtd_pkgng,
        source.genl_item_cat_grp::varchar(10) as genl_item_cat_grp,
        source.gn_matl_logl_vrnt::varchar(10) as gn_matl_logl_vrnt,
        source.prod_base::varchar(10) as prod_base,
        source.vrnt::varchar(10) as vrnt,
        source.put_up::varchar(10) as put_up,
        source.mega_brnd_cd::varchar(10) as mega_brnd_cd,
        source.brnd_cd::varchar(10) as brnd_cd,
        source.tech::varchar(10) as tech,
        source.color::varchar(10) as color,
        source.seasonality::varchar(10) as seasonality,
        source.mfg_src_cd::varchar(10) as mfg_src_cd,
        -- case when chng_flg = 'i' then CURRENT_TIMESTAMP() else tgt_crt_dttm end as  crt_dttm,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        source.mega_brnd_desc::varchar(100) as mega_brnd_desc,
        source.ibrnd_desc::varchar(100) as brnd_desc,
        source.varnt_desc::varchar(100) as varnt_desc,
        source.base_prod_desc::varchar(100) as base_prod_desc,
        source.put_up_desc::varchar(100) as put_up_desc,
        iff(itg_edw_material_dim_updt.matl_num is not null,nullif(itg_edw_material_dim_updt.prodh1,''),null::varchar(18)) as prodh1,
        itg_prod_hier_1.txtmd::varchar(100) as prodh1_txtmd,
        iff(itg_edw_material_dim_updt.matl_num is not null,nullif(itg_edw_material_dim_updt.prodh2,''),null::varchar(18)) as prodh2, 
        itg_prod_hier_2.txtmd::varchar(100) as prodh2_txtmd,
        iff(itg_edw_material_dim_updt.matl_num is not null,nullif(itg_edw_material_dim_updt.prodh3,''),null::varchar(18)) as prodh3, 
        itg_prod_hier_3.txtmd::varchar(100) as prodh3_txtmd,
        iff(itg_edw_material_dim_updt.matl_num is not null,nullif(itg_edw_material_dim_updt.prodh4,''),null::varchar(18)) as prodh4, 
        itg_prod_hier_4.txtmd::varchar(100) as prodh4_txtmd,
        iff(itg_edw_material_dim_updt.matl_num is not null,nullif(itg_edw_material_dim_updt.prodh5,''),null::varchar(18)) as prodh5, 
        itg_prod_hier_5.txtmd::varchar(100) as prodh5_txtmd,
        iff(itg_edw_material_dim_updt.matl_num is not null,nullif(itg_edw_material_dim_updt.prodh6,''),null::varchar(18)) as prodh6,
        itg_prod_hier_6.txtmd::varchar(100) as prodh6_txtmd,
        edw_material_typ.txtmd::varchar(40) as matl_type_desc,
        source.mfr_part_num_new::varchar(256) as mfr_part_num_new,
        source.formulation_num::varchar(50) as formulation_num,
        source.pka_franchise_cd::varchar(2) as pka_franchise_cd,
        source.pka_franchise_desc::varchar(30) as pka_franchise_desc,
        source.pka_brand_cd::varchar(2) as pka_brand_cd,
        source.pka_brand_desc::varchar(30) as pka_brand_desc,
        source.pka_sub_brand_cd::varchar(4) as pka_sub_brand_cd,
        source.pka_sub_brand_desc::varchar(30) as pka_sub_brand_desc,
        source.pka_variant_cd::varchar(4) as pka_variant_cd,
        source.pka_variant_desc::varchar(30) as pka_variant_desc,
        source.pka_sub_variant_cd::varchar(4) as pka_sub_variant_cd,
        source.pka_sub_variant_desc::varchar(30) as pka_sub_variant_desc,
        source.pka_flavor_cd::varchar(4) as pka_flavor_cd,
        source.pka_flavor_desc::varchar(30) as pka_flavor_desc,
        source.pka_ingredient_cd::varchar(4) as pka_ingredient_cd,
        source.pka_ingredient_desc::varchar(30) as pka_ingredient_desc,
        source.pka_application_cd::varchar(4) as pka_application_cd,
        source.pka_application_desc::varchar(30) as pka_application_desc,
        source.pka_length_cd::varchar(4) as pka_length_cd,
        source.pka_length_desc::varchar(30) as pka_length_desc,
        source.pka_shape_cd::varchar(4) as pka_shape_cd,
        source.pka_shape_desc::varchar(30) as pka_shape_desc,
        source.pka_spf_cd::varchar(4) as pka_spf_cd,
        source.pka_spf_desc::varchar(30) as pka_spf_desc,
        source.pka_cover_cd::varchar(4) as pka_cover_cd,
        source.pka_cover_desc::varchar(30) as pka_cover_desc,
        source.pka_form_cd::varchar(4) as pka_form_cd,
        source.pka_form_desc::varchar(30) as pka_form_desc,
        source.pka_size_cd::varchar(4) as pka_size_cd,
        source.pka_size_desc::varchar(30) as pka_size_desc,
        source.pka_character_cd::varchar(4) as pka_character_cd,
        source.pka_character_desc::varchar(30) as pka_character_desc,
        source.pka_package_cd::varchar(4) as pka_package_cd,
        source.pka_package_desc::varchar(30) as pka_package_desc,
        source.pka_attribute_13_cd::varchar(4) as pka_attribute_13_cd,
        source.pka_attribute_13_desc::varchar(30) as pka_attribute_13_desc,
        source.pka_attribute_14_cd::varchar(4) as pka_attribute_14_cd,
        source.pka_attribute_14_desc::varchar(30) as pka_attribute_14_desc,
        source.pka_sku_identification_cd::varchar(2) as pka_sku_identification_cd,
        source.pka_sku_identification_desc::varchar(30) as pka_sku_identification_desc,
        source.pka_one_time_relabeling_cd::varchar(2) as pka_one_time_relabeling_cd,
        source.pka_one_time_relabeling_desc::varchar(30) as pka_one_time_relabeling_desc,
        source.pka_product_key::varchar(68) as pka_product_key,
        source.pka_product_key_description::varchar(255) as pka_product_key_description,
        source.pka_product_key_description_2::varchar(255) as pka_product_key_description_2,
        source.pka_root_code::varchar(68) as pka_root_code,
        source.pka_root_code_desc_1::varchar(255) as pka_root_code_desc_1,
        source.pka_root_code_desc_2::varchar(255) as pka_root_code_desc_2
    from wks_material_dim as source
    left join itg_edw_material_dim_updt on source.matl_num=itg_edw_material_dim_updt.matl_num
    left join itg_prod_hier_1 on source.matl_num=itg_prod_hier_1.matl_num
    left join itg_prod_hier_2 on source.matl_num=itg_prod_hier_2.matl_num
    left join itg_prod_hier_3 on source.matl_num=itg_prod_hier_3.matl_num
    left join itg_prod_hier_4 on source.matl_num=itg_prod_hier_4.matl_num
    left join itg_prod_hier_5 on source.matl_num=itg_prod_hier_5.matl_num
    left join itg_prod_hier_6 on source.matl_num=itg_prod_hier_6.matl_num
    left join edw_material_typ on source.matl_type_cd=edw_material_typ.matl_type
)

{% if is_incremental() %}
,
filtered_emd as (
    select * from {{ this }} as emd where emd.matl_num not in (select matl_num from transformed)
),
transformed_emd as (
    select 
        filtered_emd.matl_num,
        filtered_emd.matl_desc,
        filtered_emd.crt_on,
        filtered_emd.crt_by_nm,
        filtered_emd.chg_dttm,
        filtered_emd.chg_by_nm,
        filtered_emd.maint_sts_cmplt_matl,
        filtered_emd.maint_sts,
        filtered_emd.fl_matl_del_clnt_lvl,
        filtered_emd.matl_type_cd,
        filtered_emd.indstr_sectr,
        filtered_emd.matl_grp_cd,
        filtered_emd.old_matl_num,
        filtered_emd.base_uom_cd,
        filtered_emd.prch_uom_cd,
        filtered_emd.doc_num,
        filtered_emd.doc_type,
        filtered_emd.doc_vers,
        filtered_emd.pg_fmt__doc,
        filtered_emd.doc_chg_num,
        filtered_emd.pg_num_doc,
        filtered_emd.num_sht,
        filtered_emd.prdtn_memo_txt,
        filtered_emd.pg_fmt_prdtn_memo,
        filtered_emd.size_dims_txt,
        filtered_emd.bsc_matl,
        filtered_emd.indstr_std_desc,
        filtered_emd.mercia_plan,
        filtered_emd.prchsng_val_key,
        filtered_emd.grs_wt_meas,
        filtered_emd.net_wt_meas,
        filtered_emd.wt_uom_cd,
        filtered_emd.vol_meas,
        filtered_emd.vol_uom_cd,
        filtered_emd.cntnr_rqr,
        filtered_emd.strg_cond,
        filtered_emd.temp_cond_ind,
        filtered_emd.low_lvl_cd,
        filtered_emd.trspn_grp,
        filtered_emd.haz_matl_num,
        filtered_emd.div,
        filtered_emd.cmpt,
        filtered_emd.ean_obsol,
        filtered_emd.gr_prtd_qty,
        filtered_emd.prcmt_rule,
        filtered_emd.src_supl,
        filtered_emd.seasn_cat,
        filtered_emd.lbl_type_cd,
        filtered_emd.lbl_form,
        filtered_emd.deact,
        filtered_emd.prmry_upc_cd,
        filtered_emd.ean_cat,
        filtered_emd.lgth_meas,
        filtered_emd.wdth_meas,
        filtered_emd.hght_meas,
        filtered_emd.dim_uom_cd,
        filtered_emd.prod_hier_cd,
        filtered_emd.stk_tfr_chg_cost,
        filtered_emd.cad_ind,
        filtered_emd.qm_prcmt_act,
        filtered_emd.allw_pkgng_wt,
        filtered_emd.wt_unit,
        filtered_emd.allw_pkgng_vol,
        filtered_emd.vol_unit,
        filtered_emd.exces_wt_tlrnc,
        filtered_emd.exces_vol_tlrnc,
        filtered_emd.var_prch_ord_unit,
        filtered_emd.rvsn_lvl_asgn_matl,
        filtered_emd.configurable_matl_ind,
        filtered_emd.btch_mgmt_reqt_ind,
        filtered_emd.pkgng_matl_type_cd,
        filtered_emd.max_lvl_vol,
        filtered_emd.stack_fact,
        filtered_emd.pkgng_matl_grp,
        filtered_emd.auth_grp,
        filtered_emd.vld_from_dt,
        filtered_emd.del_dt,
        filtered_emd.seasn_yr,
        filtered_emd.prc_bnd_cat,
        filtered_emd.bill_of_matl,
        filtered_emd.extrnl_matl_grp_txt,
        filtered_emd.cross_plnt_cnfg_matl,
        filtered_emd.matl_cat,
        filtered_emd.matl_coprod_ind,
        filtered_emd.fllp_matl_ind,
        filtered_emd.prc_ref_matl,
        filtered_emd.cros_plnt_matl_sts,
        filtered_emd.cros_dstn_chn_matl_sts,
        filtered_emd.cros_plnt_matl_sts_vld_dt,
        filtered_emd.chn_matl_vld_from_dt,
        filtered_emd.tax_clsn_matl,
        filtered_emd.catlg_prfl,
        filtered_emd.min_rmn_shlf_lif,
        filtered_emd.tot_shlf_lif,
        filtered_emd.strg_pct,
        filtered_emd.cntnt_uom_cd,
        filtered_emd.net_cntnt_meas,
        filtered_emd.cmpr_prc_unit,
        filtered_emd.isr_matl_grp,
        filtered_emd.grs_cntnt_meas,
        filtered_emd.qty_conv_meth,
        filtered_emd.intrnl_obj_num,
        filtered_emd.envmt_rlvnt,
        filtered_emd.prod_allc_dtrmn_proc,
        filtered_emd.prc_prfl_vrnt,
        filtered_emd.matl_qual_disc,
        filtered_emd.mfr_part_num,
        filtered_emd.mfr_num,
        filtered_emd.intrnl_inv_mgmt,
        filtered_emd.mfr_part_prfl,
        filtered_emd.meas_usg_unit,
        filtered_emd.rollout_seasn,
        filtered_emd.dngrs_goods_ind_prof,
        filtered_emd.hi_viscous_ind,
        filtered_emd.in_bulk_lqd_ind,
        filtered_emd.lvl_explc_ser_num,
        filtered_emd.pkgng_matl_clse_pkgng,
        filtered_emd.appr_btch_rec_ind,
        filtered_emd.ovrd_chg_num,
        filtered_emd.matl_cmplt_lvl,
        filtered_emd.per_ind_shlf_lif_expn_dt,
        filtered_emd.rd_rule_sled,
        filtered_emd.prod_cmpos_prtd_pkgng,
        filtered_emd.genl_item_cat_grp,
        filtered_emd.gn_matl_logl_vrnt,
        filtered_emd.prod_base,
        filtered_emd.vrnt,
        filtered_emd.put_up,
        filtered_emd.mega_brnd_cd,
        filtered_emd.brnd_cd,
        filtered_emd.tech,
        filtered_emd.color,
        filtered_emd.seasonality,
        filtered_emd.mfg_src_cd,
        filtered_emd.crt_dttm,
        filtered_emd.updt_dttm,
        filtered_emd.mega_brnd_desc,
        filtered_emd.brnd_desc,
        filtered_emd.varnt_desc,
        filtered_emd.base_prod_desc,
        filtered_emd.put_up_desc,
        iff(itg_edw_material_dim_updt.matl_num is not null,coalesce(itg_edw_material_dim_updt.prodh1,''),null) as prodh1,
        itg_prod_hier_1.txtmd as prodh1_txtmd,
        iff(itg_edw_material_dim_updt.matl_num is not null,coalesce(itg_edw_material_dim_updt.prodh2,''),null) as prodh2, 
        itg_prod_hier_2.txtmd as prodh2_txtmd,
        iff(itg_edw_material_dim_updt.matl_num is not null,coalesce(itg_edw_material_dim_updt.prodh3,''),null) as prodh3, 
        itg_prod_hier_3.txtmd as prodh3_txtmd,
        iff(itg_edw_material_dim_updt.matl_num is not null,coalesce(itg_edw_material_dim_updt.prodh4,''),null) as prodh4, 
        itg_prod_hier_4.txtmd as prodh4_txtmd,
        iff(itg_edw_material_dim_updt.matl_num is not null,coalesce(itg_edw_material_dim_updt.prodh5,''),null) as prodh5, 
        itg_prod_hier_5.txtmd as prodh5_txtmd,
        iff(itg_edw_material_dim_updt.matl_num is not null,coalesce(itg_edw_material_dim_updt.prodh6,''),null) as prodh6,
        itg_prod_hier_6.txtmd as prodh6_txtmd, 
        edw_material_typ.txtmd as matl_type_desc,
        filtered_emd.mfr_part_num_new,
        filtered_emd.formulation_num,
        filtered_emd.pka_franchise_cd,
        filtered_emd.pka_franchise_desc,
        filtered_emd.pka_brand_cd,
        filtered_emd.pka_brand_desc,
        filtered_emd.pka_sub_brand_cd,
        filtered_emd.pka_sub_brand_desc,
        filtered_emd.pka_variant_cd,
        filtered_emd.pka_variant_desc,
        filtered_emd.pka_sub_variant_cd,
        filtered_emd.pka_sub_variant_desc,
        filtered_emd.pka_flavor_cd,
        filtered_emd.pka_flavor_desc,
        filtered_emd.pka_ingredient_cd,
        filtered_emd.pka_ingredient_desc,
        filtered_emd.pka_application_cd,
        filtered_emd.pka_application_desc,
        filtered_emd.pka_length_cd,
        filtered_emd.pka_length_desc,
        filtered_emd.pka_shape_cd,
        filtered_emd.pka_shape_desc,
        filtered_emd.pka_spf_cd,
        filtered_emd.pka_spf_desc,
        filtered_emd.pka_cover_cd,
        filtered_emd.pka_cover_desc,
        filtered_emd.pka_form_cd,
        filtered_emd.pka_form_desc,
        filtered_emd.pka_size_cd,
        filtered_emd.pka_size_desc,
        filtered_emd.pka_character_cd,
        filtered_emd.pka_character_desc,
        filtered_emd.pka_package_cd,
        filtered_emd.pka_package_desc,
        filtered_emd.pka_attribute_13_cd,
        filtered_emd.pka_attribute_13_desc,
        filtered_emd.pka_attribute_14_cd,
        filtered_emd.pka_attribute_14_desc,
        filtered_emd.pka_sku_identification_cd,
        filtered_emd.pka_sku_identification_desc,
        filtered_emd.pka_one_time_relabeling_cd,
        filtered_emd.pka_one_time_relabeling_desc,
        filtered_emd.pka_product_key,
        filtered_emd.pka_product_key_description,
        filtered_emd.pka_product_key_description_2,
        filtered_emd.pka_root_code,
        filtered_emd.pka_root_code_desc_1,
        filtered_emd.pka_root_code_desc_2 
    from filtered_emd
    left join itg_edw_material_dim_updt on filtered_emd.matl_num=itg_edw_material_dim_updt.matl_num
    left join itg_prod_hier_1 on filtered_emd.matl_num=itg_prod_hier_1.matl_num
    left join itg_prod_hier_2 on filtered_emd.matl_num=itg_prod_hier_2.matl_num
    left join itg_prod_hier_3 on filtered_emd.matl_num=itg_prod_hier_3.matl_num
    left join itg_prod_hier_4 on filtered_emd.matl_num=itg_prod_hier_4.matl_num
    left join itg_prod_hier_5 on filtered_emd.matl_num=itg_prod_hier_5.matl_num
    left join itg_prod_hier_6 on filtered_emd.matl_num=itg_prod_hier_6.matl_num
    left join edw_material_typ on filtered_emd.matl_type_cd=edw_material_typ.matl_type
)
{% endif %}


--Final select
select * from transformed

--Union logic added here to compensate update on records which exist in edw_material_dim and not in wks_material_dim
{% if is_incremental() %}
union 
select * from transformed_emd
{% endif %}
