{{
    config(
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["sls_org","dstr_chnl","matl_num"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}

with 

source as (

    select * from {{ ref('aspwks_integration__wks_edw_material_sales_dim') }}
),

edw_sap_matl_num_ean_mapping as (
    select * from {{source('aspedw_integration', 'edw_sap_matl_num_ean_mapping')}}
),

edw_material_sales_temp as (
    select
        src.sls_org,
        dstr_chnl,
        matl,
        base_unit,
        matl_grp_1,
        prod_hierarchy,
        commsn_grp,
        vol_rebt_grp,
        pharma_cent_no,
        del_fl,
        matl_grp_2,
        matl_grp_3,
        matl_grp_4,
        matl_grp_5,
        matl_stats_grp,
        asrtmnt_grade,
        afs_vas_matl_grp,
        afs_prc_in,
        predecessor,
        sku_id,
        prodt_alloc_det_proc,
        num_pcs_in,
        case when src.sls_org in ('320A', '320S', '321A') then map.ean_num 
            else src.ean_num 
        end as ean_num,
        old_matl_num,
        delv_plnt,
        cash_disc_ind,
        prc_grp_mat,
        acct_asgn_grp,
        itm_cat_grp,
        min_ordr_qty,
        min_delv_qty,
        delv_unit,
        delv_uom,
        sls_unit,
        launch_dt,
        npi_in,
        lcl_mat_grp_1,
        lcl_mat_grp_2,
        lcl_mat_grp_3,
        lcl_mat_grp_4,
        lcl_mat_grp_5,
        lcl_mat_grp_6,
        npi_in_apo,
        copy_hist,
        prod_classftn,
        fcst_indc_apo,
        prod_type_apo,
        mstr_cd,
        med_desc
        from source as src
        left join edw_sap_matl_num_ean_mapping as map
        on replace(ltrim(replace(trim(src.matl), '0', ' ')), ' ', '0') = map.matl_num
),

final as (
    select
        sls_org,
        dstr_chnl,
        matl as matl_num,
        base_unit,
        matl_grp_1,
        prod_hierarchy,
        commsn_grp,
        vol_rebt_grp,
        pharma_cent_no,
        del_fl,
        matl_grp_2,
        matl_grp_3,
        matl_grp_4,
        matl_grp_5,
        matl_stats_grp,
        asrtmnt_grade,
        afs_vas_matl_grp,
        afs_prc_in,
        predecessor,
        sku_id,
        prodt_alloc_det_proc,
        num_pcs_in,
        ean_num,
        old_matl_num,
        delv_plnt,
        cash_disc_ind,
        prc_grp_mat as prc_grp_matl,
        acct_asgn_grp,
        itm_cat_grp,
        min_ordr_qty,
        min_delv_qty,
        delv_unit,
        delv_uom,
        sls_unit,
        launch_dt,
        npi_in,
        lcl_mat_grp_1 as lcl_matl_grp_1,
        lcl_mat_grp_2 as lcl_matl_grp_2,
        lcl_mat_grp_3 as lcl_matl_grp_3,
        lcl_mat_grp_4 as lcl_matl_grp_4,
        lcl_mat_grp_5 as lcl_matl_grp_5,
        lcl_mat_grp_6 as lcl_matl_grp_6,
        npi_in_apo,
        copy_hist,
        prod_classftn,
        fcst_indc_apo,
        prod_type_apo,
        mstr_cd,
        med_desc,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from edw_material_sales_temp
)

select * from final