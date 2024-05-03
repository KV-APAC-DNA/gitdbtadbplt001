with
itg_mds_ph_gt_customer as 
(
    select * from {{ ref('phlitg_integration__itg_mds_ph_gt_customer') }}
),
itg_mds_ph_ref_distributors as 
(
    select * from {{ ref('phlitg_integration__itg_mds_ph_ref_distributors') }}
),
itg_mds_ph_ref_rka_master as 
(
    select * from {{ ref('phlitg_integration__itg_mds_ph_ref_rka_master') }}
),

final as
(
    SELECT 'PH' AS cntry_cd,
        'Philippines' AS cntry_nm,
        (impgc.dstrbtr_grp_cd)::character varying(20) AS dstrbtr_grp_cd,
        NULL AS dstrbtr_soldto_code,
        eprcgd.primary_sold_to AS sap_soldto_code,
        impgc.dstrbtr_cust_id AS cust_cd,
        impgc.dstrbtr_cust_nm AS cust_nm,
        NULL AS alt_cust_cd,
        NULL AS alt_cust_nm,
        impgc.address AS addr,
        NULL AS area_cd,
        NULL AS area_nm,
        NULL AS state_cd,
        NULL AS state_nm,
        (impgc.rep_grp2)::character varying(30) AS region_cd,
        (impgc.rep_grp2_desc)::character varying(100) AS region_nm,
        (impgc.rep_grp3)::character varying(30) AS prov_cd,
        (impgc.rep_grp3_desc)::character varying(100) AS prov_nm,
        (impgc.rep_grp5)::character varying(30) AS town_cd,
        (impgc.rep_grp5_desc)::character varying(100) AS town_nm,
        NULL AS city_cd,
        NULL AS city_nm,
        impgc.zip AS post_cd,
        NULL AS post_nm,
        impgc.rep_grp6 AS slsmn_cd,
        (impgc.rep_grp6_desc)::character varying(50) AS slsmn_nm,
        (impgc.rpt_grp9)::character varying(30) AS chnl_cd,
        (impgc.rpt_grp9_desc)::character varying(100) AS chnl_desc,
        (impgc.rpt_grp11)::character varying(30) AS sub_chnl_cd,
        (impgc.rpt_grp11_desc)::character varying(100) AS sub_chnl_desc,
        NULL AS chnl_attr1_cd,
        NULL AS chnl_attr1_desc,
        NULL AS chnl_attr2_cd,
        NULL AS chnl_attr2_desc,
        (impgc.rpt_grp9)::character varying(30) AS outlet_type_cd,
        impgc.rpt_grp9_desc AS outlet_type_desc,
        NULL AS cust_grp_cd,
        NULL AS cust_grp_desc,
        NULL AS cust_grp_attr1_cd,
        NULL AS cust_grp_attr1_desc,
        NULL AS cust_grp_attr2_cd,
        NULL AS cust_grp_attr2_desc,
        (impgc.sls_dist)::character varying(10) AS sls_dstrct_cd,
        (rka.rka_nm)::character varying(100) AS sls_dstrct_nm,
        NULL AS sls_office_cd,
        NULL AS sls_office_desc,
        NULL AS sls_grp_cd,
        NULL AS sls_grp_desc,
        (impgc.status)::character varying(20) AS status
    FROM (
            (
                itg_mds_ph_gt_customer impgc
                LEFT JOIN (
                    SELECT DISTINCT itg_mds_ph_ref_distributors.dstrbtr_grp_cd,
                        itg_mds_ph_ref_distributors.primary_sold_to
                    FROM itg_mds_ph_ref_distributors
                    WHERE (
                            (itg_mds_ph_ref_distributors.active)::text = ('Y'::character varying)::text
                        )
                ) eprcgd ON (
                    (
                        (eprcgd.dstrbtr_grp_cd)::text = (impgc.dstrbtr_grp_cd)::text
                    )
                )
            )
            LEFT JOIN (
                SELECT itg_mds_ph_ref_rka_master.rka_cd,
                    itg_mds_ph_ref_rka_master.rka_nm,
                    itg_mds_ph_ref_rka_master.last_chg_datetime,
                    itg_mds_ph_ref_rka_master.effective_from,
                    itg_mds_ph_ref_rka_master.effective_to,
                    itg_mds_ph_ref_rka_master.active,
                    itg_mds_ph_ref_rka_master.crtd_dttm,
                    itg_mds_ph_ref_rka_master.updt_dttm
                FROM itg_mds_ph_ref_rka_master
                WHERE (
                        (itg_mds_ph_ref_rka_master.active)::text = ('Y'::character varying)::text
                    )
            ) rka ON (
                (
                    ltrim((impgc.sls_dist)::text) = ltrim((rka.rka_cd)::text)
                )
            )
        )
    WHERE (
            (impgc.active)::text = ('Y'::character varying)::text
))
select * from final