with source as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_ref_distributors') }}
),
final as
(
    select primary_sold_to AS "primary_sold_to",
        rpt_grp_14_desc AS "rpt_grp_14_desc",
        active AS "active",
        rpt_grp_10 AS "rpt_grp_10",
        rpt_grp_8 AS "rpt_grp_8",
        updt_dttm AS "updt_dttm",
        primary_sold_to_nm AS "primary_sold_to_nm",
        rpt_grp_14 AS "rpt_grp_14",
        last_chg_datetime AS "last_chg_datetime",
        effective_to AS "effective_to",
        rpt_grp_8_desc AS "rpt_grp_8_desc",
        rpt_grp_12 AS "rpt_grp_12",
        crtd_dttm AS "crtd_dttm",
        rpt_grp_13_desc AS "rpt_grp_13_desc",
        dstrbtr_grp_cd AS "dstrbtr_grp_cd",
        effective_from AS "effective_from",
        rpt_grp_12_desc AS "rpt_grp_12_desc",
        rpt_grp_10_desc AS "rpt_grp_10_desc",
        rpt_grp_13 AS "rpt_grp_13",
        dstrbtr_grp_nm AS "dstrbtr_grp_nm"
    from source
)
select * from final