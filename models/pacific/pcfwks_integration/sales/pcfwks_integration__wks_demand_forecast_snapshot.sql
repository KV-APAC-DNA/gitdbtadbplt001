{{
    config(
        sql_header='use warehouse DEV_DNA_CORE_app2_wh;'
    )
}}

with edw_demand_forecast_snapshot as(
    select * from {{ ref('pcfedw_integration__edw_demand_forecast_snapshot_temp') }}
),
vw_dmnd_frcst_customer_dim as(
    select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.vw_dmnd_frcst_customer_dim
),
vw_material_dim as(
    select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.vw_material_dim
),
vw_apo_parent_child_dim as(
    select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.vw_apo_parent_child_dim
),
mstrcd as(
    select distinct
        master_code,
        parent_matl_desc
    from vw_apo_parent_child_dim
    where
    to_char(cmp_id) = '7470'
    union all
    select distinct
        to_char(master_code),
        parent_matl_desc
    from vw_apo_parent_child_dim
    where
    not master_code in (
        select distinct
        to_char(master_code)
        from vw_apo_parent_child_dim
        where
        to_char(cmp_id) = '7470'
    )
),
union1 as(
    select
        edfs.pac_source_type,
        edfs.pac_subsource_type,
        edfs.snap_shot_dt,
        edfs.snapshot_week_no,
        edfs.snapshot_mnth_week_no,
        edfs.snapshot_mnth_shrt,
        edfs.snapshot_year,
        edfs.final_version_indicator,
        edfs.jj_period,
        edfs.jj_week_no,
        edfs.jj_wk,
        edfs.jj_mnth,
        edfs.jj_mnth_shrt,
        edfs.jj_mnth_long,
        edfs.jj_qrtr,
        edfs.jj_year,
        edfs.jj_mnth_tot,
        ltrim(vmd.matl_id, 0) as matl_no,
        vmd.matl_desc,
        mstrcd.master_code,
        ltrim(vapcd.parent_id, 0) as parent_id,
        mstrcd.parent_matl_desc,
        vmd.mega_brnd_cd,
        vmd.mega_brnd_desc,
        vmd.brnd_cd,
        vmd.brnd_desc,
        vmd.base_prod_cd,
        vmd.base_prod_desc,
        vmd.variant_cd,
        vmd.variant_desc,
        vmd.fran_cd,
        vmd.fran_desc,
        vmd.grp_fran_cd,
        vmd.grp_fran_desc,
        vmd.matl_type_cd,
        vmd.matl_type_desc,
        vmd.prod_fran_cd,
        vmd.prod_fran_desc,
        vmd.prod_hier_cd,
        vmd.prod_hier_desc,
        vmd.prod_mjr_cd,
        vmd.prod_mjr_desc,
        vmd.prod_mnr_cd,
        vmd.prod_mnr_desc,
        vmd.mercia_plan,
        vmd.putup_cd,
        vmd.putup_desc,
        vmd.bar_cd,
        ltrim(vcd.cust_no, 0) as cust_no,
        vcd.cmp_id,
        vcd.ctry_key,
        vcd.country,
        vcd.state_cd,
        vcd.post_cd,
        vcd.cust_suburb,
        vcd.cust_nm,
        vcd.fcst_chnl,
        vcd.fcst_chnl_desc,
        vcd.sales_office_cd,
        vcd.sales_office_desc,
        vcd.sales_grp_cd,
        vcd.sales_grp_desc,
        vcd.curr_cd,
        edfs.actual_sales_qty,
        edfs.apo_tot_frcst,
        edfs.apo_base_frcst,
        edfs.apo_promo_frcst,
        edfs.px_tot_frcst,
        edfs.px_base_frcst,
        edfs.px_promo_frcst
    from edw_demand_forecast_snapshot as edfs, vw_dmnd_frcst_customer_dim as vcd, vw_material_dim as vmd, vw_apo_parent_child_dim as vapcd, mstrcd
    where
    edfs.pac_subsource_type <> 'SAPBW_APO_FORECAST'
    and edfs.cust_no = ltrim(vcd.cust_no(+), '0')
    and edfs.matl_no = ltrim(vmd.matl_id(+), '0')
    and (
    edfs.cmp_id = vapcd.cmp_id(+) and edfs.matl_no = ltrim(vapcd.matl_id(+), '0')
)
and vapcd.master_code = mstrcd.master_code(+)
),
mstrcdd as(
    select distinct
        master_code,
        parent_matl_desc
        from vw_apo_parent_child_dim
        where
        to_char(cmp_id) = '7470'
        union all
        select distinct
        master_code,
        parent_matl_desc
    from vw_apo_parent_child_dim
    where
    not master_code in (
        select distinct
        master_code
        from vw_apo_parent_child_dim
        where
        to_char(cmp_id) = '7470'
    )
),
union2 as(
    select
        edfs.pac_source_type,
        edfs.pac_subsource_type,
        edfs.snap_shot_dt,
        edfs.snapshot_week_no,
        edfs.snapshot_mnth_week_no,
        edfs.snapshot_mnth_shrt,
        edfs.snapshot_year,
        edfs.final_version_indicator,
        edfs.jj_period,
        edfs.jj_week_no,
        edfs.jj_wk,
        edfs.jj_mnth,
        edfs.jj_mnth_shrt,
        edfs.jj_mnth_long,
        edfs.jj_qrtr,
        edfs.jj_year,
        edfs.jj_mnth_tot,
        ltrim(vmd.matl_id, 0) as matl_no,
        vmd.matl_desc,
        mstrcdd.master_code,
        ltrim(vapcd.parent_id, 0) as parent_id,
        mstrcdd.parent_matl_desc,
        vmd.mega_brnd_cd,
        vmd.mega_brnd_desc,
        vmd.brnd_cd,
        vmd.brnd_desc,
        vmd.base_prod_cd,
        vmd.base_prod_desc,
        vmd.variant_cd,
        vmd.variant_desc,
        vmd.fran_cd,
        vmd.fran_desc,
        vmd.grp_fran_cd,
        vmd.grp_fran_desc,
        vmd.matl_type_cd,
        vmd.matl_type_desc,
        vmd.prod_fran_cd,
        vmd.prod_fran_desc,
        vmd.prod_hier_cd,
        vmd.prod_hier_desc,
        vmd.prod_mjr_cd,
        vmd.prod_mjr_desc,
        vmd.prod_mnr_cd,
        vmd.prod_mnr_desc,
        vmd.mercia_plan,
        vmd.putup_cd,
        vmd.putup_desc,
        vmd.bar_cd,
        null as cust_no,
        vcd.cmp_id,
        null as ctry_key,
        vcd.country,
        null as state_cd,
        null as post_cd,
        null as cust_suburb,
        null as cust_nm,
        vcd.fcst_chnl,
        vcd.fcst_chnl_desc,
        null as sales_office_cd,
        null as sales_office_desc,
        null as sales_grp_cd,
        null as sales_grp_desc,
        null as curr_cd,
        edfs.actual_sales_qty,
        edfs.apo_tot_frcst,
        edfs.apo_base_frcst,
        edfs.apo_promo_frcst,
        edfs.px_tot_frcst,
        edfs.px_base_frcst,
        edfs.px_promo_frcst
    from edw_demand_forecast_snapshot as edfs, (
    select distinct
        cmp_id,
        country,
        sls_org,
        fcst_chnl,
        fcst_chnl_desc
    from vw_dmnd_frcst_customer_dim
    ) as vcd, vw_material_dim as vmd, vw_apo_parent_child_dim as vapcd,  mstrcdd
    where
    edfs.pac_subsource_type = 'SAPBW_APO_FORECAST'
    and edfs.fcst_chnl = vcd.fcst_chnl(+)
    and edfs.matl_no = ltrim(vmd.matl_id(+), '0')
    and (
    edfs.cmp_id = vapcd.cmp_id(+) and edfs.matl_no = ltrim(vapcd.matl_id(+), '0')
    )
    and vapcd.master_code =  mstrcdd.master_code(+)
),
transformed as(
    select * from union1
    union all
    select * from union2
)
select * from transformed