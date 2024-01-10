{{
    config(
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["sls_org","dstr_chnl","matl"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}

with 

source as (

    select * from {{ ref('aspwks_integration__wks_itg_material_sales') }}
),

final as (
    select
        salesorg as sls_org,
        distr_chan as dstr_chnl,
        mat_sales as matl,
        base_uom as base_unit,
        matl_grp_1 as matl_grp_1,
        prod_hier as prod_hierarchy,
        prov_group as commsn_grp,
        rebate_grp as vol_rebt_grp,
        ph_refnr as pharma_cent_no,
        del_flag as del_fl,
        matl_grp_2 as matl_grp_2,
        matl_grp_3 as matl_grp_3,
        matl_grp_4 as matl_grp_4,
        matl_grp_5 as matl_grp_5,
        mat_stgrp as matl_stats_grp,
        rt_assgrad as asrtmnt_grade,
        af_vasmg as afs_vas_matl_grp,
        af_prind as afs_prc_in,
        zpredecsr as predecessor,
        zskuid as sku_id,
        zprallocd as prodt_alloc_det_proc,
        zpackpcs as num_pcs_in,
        zean as ean_num,
        zoldmatl as old_matl_num,
        zd_plant as delv_plnt,
        zchdcind as cash_disc_ind,
        zprc_grp as prc_grp_mat,
        accnt_asgn as acct_asgn_grp,
        zitm_cgrp as itm_cat_grp,
        zmin_qty as min_ordr_qty,
        zmin_dqty as min_delv_qty,
        zdel_unit as delv_unit,
        zschme as delv_uom,
        zvrkme as sls_unit,
        zlaunchd as launch_dt,
        znpi_ind as npi_in,
        zlmat_gr1 as lcl_mat_grp_1,
        zlmat_gr2 as lcl_mat_grp_2,
        zlmat_gr3 as lcl_mat_grp_3,
        zlmat_gr4 as lcl_mat_grp_4,
        zlmat_gr5 as lcl_mat_grp_5,
        zlmat_gr6 as lcl_mat_grp_6,
        znpi_indr as npi_in_apo,
        zcpy_hist as copy_hist,
        zabc as prod_classftn,
        zfc_indc as fcst_indc_apo,
        zprdtype as prod_type_apo,
        zparent as mstr_cd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

select * from final