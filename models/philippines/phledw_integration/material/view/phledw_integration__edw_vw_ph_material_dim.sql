with
edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_gch_producthierarchy as
(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
edw_material_plant_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_plant_dim') }}
),
final as
(
    SELECT  
        ph.cntry_key,
        ph.sap_matl_num,
        ph.sap_mat_desc,
        ph.ean_num,
        ph.sap_mat_type_cd,
        ph.sap_mat_type_desc,
        ph.sap_base_uom_cd,
        ph.sap_prchse_uom_cd,
        ph.sap_prod_sgmt_cd,
        ph.sap_prod_sgmt_desc,
        ph.sap_base_prod_cd,
        ph.sap_base_prod_desc,
        ph.sap_mega_brnd_cd,
        ph.sap_mega_brnd_desc,
        ph.sap_brnd_cd,
        ph.sap_brnd_desc,
        ph.sap_vrnt_cd,
        ph.sap_vrnt_desc,
        ph.sap_put_up_cd,
        ph.sap_put_up_desc,
        ph.sap_grp_frnchse_cd,
        ph.sap_grp_frnchse_desc,
        ph.sap_frnchse_cd,
        ph.sap_frnchse_desc,
        ph.sap_prod_frnchse_cd,
        ph.sap_prod_frnchse_desc,
        ph.sap_prod_mjr_cd,
        ph.sap_prod_mjr_desc,
        ph.sap_prod_mnr_cd,
        ph.sap_prod_mnr_desc,
        ph.sap_prod_hier_cd,
        ph.sap_prod_hier_desc,
        ph.gph_region,
        ph.gph_reg_frnchse,
        ph.gph_reg_frnchse_grp,
        ph.gph_prod_frnchse,
        ph.gph_prod_brnd,
        ph.gph_prod_sub_brnd,
        ph.gph_prod_vrnt,
        ph.gph_prod_needstate,
        ph.gph_prod_ctgry,
        ph.gph_prod_subctgry,
        ph.gph_prod_sgmnt,
        ph.gph_prod_subsgmnt,
        ph.gph_prod_put_up_cd,
        ph.gph_prod_put_up_desc,
        ph.gph_prod_size,
        ph.gph_prod_size_uom,
        ph.launch_dt,
        ph.qty_shipper_pc,
        ph.prft_ctr,
        ph.shlf_life
        FROM (
                SELECT DISTINCT (empd.cntry_key)::character varying(4) AS cntry_key,
                    emd.matl_num AS sap_matl_num,
                    emd.matl_desc AS sap_mat_desc,
                    (NULL::character varying)::character varying(100) AS ean_num,
                    emd.matl_type_cd AS sap_mat_type_cd,
                    emd.matl_type_desc AS sap_mat_type_desc,
                    emd.base_uom_cd AS sap_base_uom_cd,
                    emd.prch_uom_cd AS sap_prchse_uom_cd,
                    emd.prodh1 AS sap_prod_sgmt_cd,
                    emd.prodh1_txtmd AS sap_prod_sgmt_desc,
                    emd.prod_base AS sap_base_prod_cd,
                    emd.base_prod_desc AS sap_base_prod_desc,
                    emd.mega_brnd_cd AS sap_mega_brnd_cd,
                    emd.mega_brnd_desc AS sap_mega_brnd_desc,
                    emd.brnd_cd AS sap_brnd_cd,
                    emd.brnd_desc AS sap_brnd_desc,
                    emd.vrnt AS sap_vrnt_cd,
                    emd.varnt_desc AS sap_vrnt_desc,
                    emd.put_up AS sap_put_up_cd,
                    emd.put_up_desc AS sap_put_up_desc,
                    emd.prodh2 AS sap_grp_frnchse_cd,
                    emd.prodh2_txtmd AS sap_grp_frnchse_desc,
                    emd.prodh3 AS sap_frnchse_cd,
                    emd.prodh3_txtmd AS sap_frnchse_desc,
                    emd.prodh4 AS sap_prod_frnchse_cd,
                    emd.prodh4_txtmd AS sap_prod_frnchse_desc,
                    emd.prodh5 AS sap_prod_mjr_cd,
                    emd.prodh5_txtmd AS sap_prod_mjr_desc,
                    emd.prodh5 AS sap_prod_mnr_cd,
                    emd.prodh5_txtmd AS sap_prod_mnr_desc,
                    emd.prodh6 AS sap_prod_hier_cd,
                    emd.prodh6_txtmd AS sap_prod_hier_desc,
                    egph."region" AS gph_region,
                    egph.regional_franchise AS gph_reg_frnchse,
                    egph.regional_franchise_group AS gph_reg_frnchse_grp,
                    egph.gcph_franchise AS gph_prod_frnchse,
                    egph.gcph_brand AS gph_prod_brnd,
                    egph.gcph_subbrand AS gph_prod_sub_brnd,
                    egph.gcph_variant AS gph_prod_vrnt,
                    egph.gcph_needstate AS gph_prod_needstate,
                    egph.gcph_category AS gph_prod_ctgry,
                    egph.gcph_subcategory AS gph_prod_subctgry,
                    egph.gcph_segment AS gph_prod_sgmnt,
                    egph.gcph_subsegment AS gph_prod_subsgmnt,
                    egph.put_up_code AS gph_prod_put_up_cd,
                    egph.put_up_description AS gph_prod_put_up_desc,
                    egph.size AS gph_prod_size,
                    egph.unit_of_measure AS gph_prod_size_uom,
                    (current_timestamp::text)::timestamp without time zone AS launch_dt,
                    (NULL::character varying)::character varying(100) AS qty_shipper_pc,
                    (NULL::character varying)::character varying(100) AS prft_ctr,
                    (NULL::character varying)::character varying(100) AS shlf_life
                FROM edw_material_dim emd,
                    edw_gch_producthierarchy egph,
                    (
                        SELECT DISTINCT edw_material_plant_dim.matl_num,
                            CASE
                                WHEN (
                                    ('2300'::text = 'PH'::text)
                                    OR (
                                        ('2300' IS NULL)
                                        AND ('PH' IS NULL)
                                    )
                                ) THEN '230A'::text
                                ELSE 'PH'::text
                            END AS cntry_key
                        FROM edw_material_plant_dim
                        WHERE (
                                (
                                    (edw_material_plant_dim.plnt)::text = '2300'::text
                                )
                                OR (
                                    (edw_material_plant_dim.plnt)::text = '230A'::text
                                )
                            )
                    ) empd
                WHERE (
                        (
                            (
                                (
                                    (emd.matl_num)::text = (egph.materialnumber)::text
                                )
                                AND ((emd.matl_num)::text = (empd.matl_num)::text)
                            )
                            AND ((emd.prod_hier_cd)::text <> ''::text)
                        )
                        AND (
                            (
                                (
                                    (
                                        (
                                            ((emd.matl_type_cd)::text = 'FERT'::text)
                                            OR ((emd.matl_type_cd)::text = 'HALB'::text)
                                        )
                                        OR ((emd.matl_type_cd)::text = 'PROM'::text)
                                    )
                                    OR ((emd.matl_type_cd)::text = 'SAPR'::text)
                                )
                                OR ((emd.matl_type_cd)::text = 'ROH'::text)
                            )
                            OR ((emd.matl_type_cd)::text = 'FER2'::text)
                        )
                    )
            ) ph
)
select * from final