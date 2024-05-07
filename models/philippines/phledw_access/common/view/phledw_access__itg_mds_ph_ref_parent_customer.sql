with source as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_ref_parent_customer') }}
),
final as
(
    select rpt_grp_1_desc AS "rpt_grp_1_desc",
        rpt_grp_1 AS "rpt_grp_1",
        rpt_grp_11 AS "rpt_grp_11",
        parent_cust_cd AS "parent_cust_cd",
        rpt_grp_2 AS "rpt_grp_2",
        channel AS "channel",
        rpt_grp_2_desc AS "rpt_grp_2_desc",
        customer_segmentation1 AS "customer_segmentation1",
        customer_segmentation2 AS "customer_segmentation2",
        rpt_grp_12_desc AS "rpt_grp_12_desc",
        effective_from AS "effective_from",
        crtd_dttm AS "crtd_dttm",
        rpt_grp_12 AS "rpt_grp_12",
        active AS "active",
        last_chg_datetime AS "last_chg_datetime",
        rpt_grp_11_desc AS "rpt_grp_11_desc",
        effective_to AS "effective_to",
        updt_dttm AS "updt_dttm",
        parent_cust_nm AS "parent_cust_nm"
    from source
)
select * from final