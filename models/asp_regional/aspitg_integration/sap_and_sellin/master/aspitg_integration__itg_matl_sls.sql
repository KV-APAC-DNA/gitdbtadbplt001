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
        salesorg :: varchar(4) as sls_org,
        distr_chan :: varchar(2) as dstr_chnl,
        mat_sales :: varchar(18) as matl,
        base_uom :: varchar(3) as base_unit,
        matl_grp_1 :: varchar(3) as matl_grp_1,
        prod_hier :: varchar(18) as prod_hierarchy,
        prov_group :: varchar(2) as commsn_grp,
        rebate_grp :: varchar(2) as vol_rebt_grp,
        ph_refnr :: varchar(10) as pharma_cent_no,
        del_flag :: varchar(1) as del_fl,
        matl_grp_2 :: varchar(3) as matl_grp_2,
        matl_grp_3 :: varchar(3) as matl_grp_3,
        matl_grp_4 :: varchar(3) as matl_grp_4,
        matl_grp_5 :: varchar(3) as matl_grp_5,
        mat_stgrp :: varchar(1) as matl_stats_grp,
        rt_assgrad :: varchar(2) as asrtmnt_grade,
        af_vasmg :: varchar(3) as afs_vas_matl_grp,
        af_prind :: varchar(2) as afs_prc_in,
        zpredecsr :: varchar(18) as predecessor,
        zskuid :: varchar(2) as sku_id,
        zprallocd :: varchar(18) as prodt_alloc_det_proc,
        zpackpcs as num_pcs_in,
        zean :: varchar(18) as ean_num,
        zoldmatl :: varchar(18) as old_matl_num,
        zd_plant :: varchar(4) as delv_plnt,
        zchdcind :: varchar(1) as cash_disc_ind,
        zprc_grp :: varchar(2) as prc_grp_mat,
        accnt_asgn :: varchar(2) as acct_asgn_grp,
        zitm_cgrp :: varchar(4) as itm_cat_grp,
        zmin_qty as min_ordr_qty,
        zmin_dqty as min_delv_qty,
        zdel_unit as delv_unit,
        zschme :: varchar(3) as delv_uom,
        zvrkme :: varchar(3) as sls_unit,
        zlaunchd as launch_dt,
        znpi_ind :: varchar(1) as npi_in,
        zlmat_gr1 :: varchar(10) as lcl_mat_grp_1,
        zlmat_gr2 :: varchar(10) as lcl_mat_grp_2,
        zlmat_gr3 :: varchar(10) as lcl_mat_grp_3,
        zlmat_gr4 :: varchar(10) as lcl_mat_grp_4,
        zlmat_gr5 :: varchar(10) as lcl_mat_grp_5,
        zlmat_gr6 :: varchar(10) as lcl_mat_grp_6,
        znpi_indr as npi_in_apo,
        zcpy_hist :: varchar(1) as copy_hist,
        zabc :: varchar(2) as prod_classftn,
        zfc_indc :: varchar(1) as fcst_indc_apo,
        zprdtype as prod_type_apo,
        zparent :: varchar(18) as mstr_cd,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

select * from final