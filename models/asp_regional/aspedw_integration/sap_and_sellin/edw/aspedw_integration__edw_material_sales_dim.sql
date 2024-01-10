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
    select cntry,ean_num,matl_num,sls_org,crt_dttm
    from
        (select *, row_number() over (partition by matl_num order by ean_num) as rnk from {{source('aspedw_integration', 'edw_sap_matl_num_ean_mapping')}}) a
    where rnk=1
),

transformed as (
    select
        src.sls_org,
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
        case when src.sls_org in ('320A', '320S', '321A') then map.ean_num 
            else src.ean_num 
        end as ean_num,
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
        from source as src
        left join edw_sap_matl_num_ean_mapping as map
            on replace(ltrim(replace(trim(src.matl), '0', ' ')), ' ', '0') = map.matl_num
)

{% if is_incremental() %}
,
filtered_emsd as (
    select * from {{ this }} as emsd where concat(emsd.matl_num,emsd.dstr_chnl,emsd.sls_org) not in (select concat(matl_num,dstr_chnl,sls_org) from transformed)
),
transformed_emsd as (
    select 
        filtered.sls_org,
        filtered.dstr_chnl,
        filtered.matl_num,
        filtered.base_unit,
        filtered.matl_grp_1,
        filtered.prod_hierarchy,
        filtered.commsn_grp,
        filtered.vol_rebt_grp,
        filtered.pharma_cent_no,
        filtered.del_fl,
        filtered.matl_grp_2,
        filtered.matl_grp_3,
        filtered.matl_grp_4,
        filtered.matl_grp_5,
        filtered.matl_stats_grp,
        filtered.asrtmnt_grade,
        filtered.afs_vas_matl_grp,
        filtered.afs_prc_in,
        filtered.predecessor,
        filtered.sku_id,
        filtered.prodt_alloc_det_proc,
        filtered.num_pcs_in,
        case when filtered.sls_org in ('320A', '320S', '321A') then map.ean_num 
            else filtered.ean_num 
        end as ean_num,
        filtered.old_matl_num,
        filtered.delv_plnt,
        filtered.cash_disc_ind,
        filtered.prc_grp_matl,
        filtered.acct_asgn_grp,
        filtered.itm_cat_grp,
        filtered.min_ordr_qty,
        filtered.min_delv_qty,
        filtered.delv_unit,
        filtered.delv_uom,
        filtered.sls_unit,
        filtered.launch_dt,
        filtered.npi_in,
        filtered.lcl_matl_grp_1,
        filtered.lcl_matl_grp_2,
        filtered.lcl_matl_grp_3,
        filtered.lcl_matl_grp_4,
        filtered.lcl_matl_grp_5,
        filtered.lcl_matl_grp_6,
        filtered.npi_in_apo,
        filtered.copy_hist,
        filtered.prod_classftn,
        filtered.fcst_indc_apo,
        filtered.prod_type_apo,
        filtered.mstr_cd,
        filtered.med_desc,
        filtered.crt_dttm,
        filtered.updt_dttm
    from filtered_emsd as filtered
    left join edw_sap_matl_num_ean_mapping as map
        on replace(ltrim(replace(trim(filtered.matl_num), '0', ' ')), ' ', '0') = map.matl_num

)
{% endif %}


--Final select
select * from transformed

--Union logic added here to compensate update on records which exist in edw_material_dim and not in wks_material_dim
{% if is_incremental() %}
union 
select * from transformed_emsd
{% endif %}
