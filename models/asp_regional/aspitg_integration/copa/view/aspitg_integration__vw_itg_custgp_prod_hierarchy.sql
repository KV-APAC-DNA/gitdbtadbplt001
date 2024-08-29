{{
    config(
        materialized='view'
    )
}}

with itg_my_material_dim as
(
    select * from {{ ref('mysitg_integration__itg_my_material_dim') }}
),
edw_gch_producthierarchy as
(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
itg_mds_sg_product_hierarchy as
(
    select * from {{ ref('sgpitg_integration__itg_mds_sg_product_hierarchy') }}
),
edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
itg_mds_hk_product_hierarchy as
(
    select * from {{ ref('ntaitg_integration__itg_mds_hk_product_hierarchy') }}
),
edw_material_plant_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_plant_dim') }}
),
edw_material_sales_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_product_attr_dim as
(
    select * from {{ ref('aspedw_integration__edw_product_attr_dim') }}
),
itg_mds_ph_lav_product as
(
    select * from {{ ref('phlitg_integration__itg_mds_ph_lav_product') }}
),
edw_copa_trans_fact as
(
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
final as
(
        SELECT derived_table3.ctry_nm,
            derived_table3.matl_num,
            COALESCE(derived_table3.loc_prod1,'Not Available'::CHARACTER VARYING) AS loc_prod1,
            COALESCE(derived_table3.loc_prod2,'Not Available'::CHARACTER VARYING) AS loc_prod2,
            COALESCE(derived_table3.loc_prod3,'Not Available'::CHARACTER VARYING) AS loc_prod3,
            COALESCE(derived_table3.loc_prod4,'Not Available'::CHARACTER VARYING) AS loc_prod4,
            COALESCE(derived_table3.loc_prod5,'Not Available'::CHARACTER VARYING) AS loc_prod5,
            COALESCE(derived_table3.loc_prod6,'Not Available'::CHARACTER VARYING) AS loc_prod6,
            COALESCE(derived_table3.loc_prod7,'Not Available'::TEXT) AS loc_prod7,
            COALESCE(derived_table3.loc_prod8,'Not Available'::TEXT) AS loc_prod8,
            COALESCE(derived_table3.loc_prod9,'Not Available'::TEXT) AS loc_prod9,
            COALESCE(derived_table3.loc_prod10,'Not Available'::TEXT) AS loc_prod10
        FROM ((((((SELECT DISTINCT 'Malaysia'::TEXT AS ctry_nm,
                        my_mat.item_cd AS matl_num,
                        gph.gcph_franchise AS loc_prod1,
                        my_mat.frnchse_desc AS loc_prod2,
                        my_mat.brnd_desc AS loc_prod3,
                        my_mat.vrnt_desc AS loc_prod4,
                        my_mat.putup_desc AS loc_prod5,
                        my_mat.promo_reg_ind AS loc_prod6,
                        'Not Available'::TEXT AS loc_prod7,
                        'Not Available'::TEXT AS loc_prod8,
                        'Not Available'::TEXT AS loc_prod9,
                        'Not Available'::TEXT AS loc_prod10
                FROM (itg_my_material_dim my_mat
                    LEFT JOIN edw_gch_producthierarchy gph ON ( ( (my_mat.item_cd)::TEXT = LTRIM ( (gph.materialnumber)::TEXT,'0'::TEXT))))
                UNION ALL
                SELECT DISTINCT 'Singapore'::TEXT AS ctry_nm,
                        sg_mat.material AS matl_num,
                        sg_mat.category AS loc_prod1,
                        sg_mat.brand AS loc_prod2,
                        sg_mat.productvarient AS loc_prod3,
                        sg_mat.producttype AS loc_prod4,
                        'Not Available'::TEXT AS loc_prod5,
                        'Not Available'::TEXT AS loc_prod6,
                        'Not Available'::TEXT AS loc_prod7,
                        'Not Available'::TEXT AS loc_prod8,
                        'Not Available'::TEXT AS loc_prod9,
                        'Not Available'::TEXT AS loc_prod10
                FROM itg_mds_sg_product_hierarchy sg_mat)
                UNION ALL
                SELECT DISTINCT 'Korea'::TEXT AS ctry_nm,
                        (LTRIM((mat.matl_num)::TEXT,'0'::TEXT))::CHARACTER VARYING AS matl_num,
                        gph.gcph_franchise AS loc_prod1,
                        NULL::TEXT AS loc_prod2,
                        mat.prodh2_txtmd AS loc_prod3,
                        mat.prodh3_txtmd AS loc_prod4,
                        mat.prodh4_txtmd AS loc_prod5,
                        mat.prodh5_txtmd AS loc_prod6,
                        'Not Available'::TEXT AS loc_prod7,
                        'Not Available'::TEXT AS loc_prod8,
                        'Not Available'::TEXT AS loc_prod9,
                        'Not Available'::TEXT AS loc_prod10
                FROM (edw_material_dim mat
                    LEFT JOIN edw_gch_producthierarchy gph ON ( (LTRIM ( (mat.matl_num)::TEXT,'0'::TEXT) = LTRIM ( (gph.materialnumber)::TEXT,'0'::TEXT)))))
                UNION ALL
                SELECT DISTINCT 'Thailand'::TEXT AS ctry_nm,
                        (LTRIM((mat.matl_num)::TEXT,'0'::TEXT))::CHARACTER VARYING AS matl_num,
                        gph.gcph_franchise AS loc_prod1,
                        CASE
                            WHEN mega_brnd_desc LIKE 'Jupiter%' OR mega_brnd_desc LIKE '%Jupiter%' THEN 'Self Care - Jupiter'
                            ELSE mat.pka_franchise_desc
                        END AS loc_prod2,
                        mat.pka_brand_desc AS loc_prod3,
                        mat.pka_form_desc AS loc_prod4,
                        mat.pka_sub_brand_desc AS loc_prod5,
                        mat.pka_variant_desc AS loc_prod6,
                        mat.pka_package_desc AS loc_prod7,
                        mat.pka_size_desc AS loc_prod8,
                        (LTRIM((mat.matl_num)::TEXT,'0'::TEXT))::CHARACTER VARYING AS loc_prod9,
                        mat.matl_desc AS loc_prod10
                FROM (edw_material_dim mat
                    LEFT JOIN edw_gch_producthierarchy gph ON ( (LTRIM ( (mat.matl_num)::TEXT,'0'::TEXT) = LTRIM ( (gph.materialnumber)::TEXT,'0'::TEXT)))))
                UNION ALL
                (
    SELECT DISTINCT 'Hong Kong'::TEXT AS ctry_nm,
                        (LTRIM((mat.matl_num)::TEXT,'0'::TEXT))::CHARACTER VARYING AS matl_num,
                        gph.gcph_franchise AS loc_prod1,
                        mat.prodh3_txtmd AS loc_prod2,
                        mat.mega_brnd_desc AS loc_prod3,
                        hk_prod.hk_brand_code AS loc_prod4,
                        hk_prod.hk_base_product_code AS loc_prod5,
                        matplnt.mstr_cd AS loc_prod6,
                        mat.pka_size_desc AS loc_prod7,
                        mat.pka_package_desc AS loc_prod8,
                        mat.pka_product_key_description AS loc_prod9  ,
                        'Not Available'::TEXT AS loc_prod10
                FROM edw_material_dim mat
                    LEFT JOIN edw_gch_producthierarchy gph ON LTRIM  (mat.matl_num,'0') = LTRIM (gph.materialnumber,'0')
                    LEFT JOIN itg_mds_hk_product_hierarchy hk_prod
                            ON mat.base_prod_desc = hk_prod.sap_base_product
                        AND mat.brnd_desc = hk_prod.sap_brand
                    LEFT JOIN (SELECT edw_material_plant_dim.plnt,
                                    LTRIM(edw_material_plant_dim.matl_plnt_view,'0') AS matl_num,
                                    edw_material_plant_dim.mstr_cd
                                FROM edw_material_plant_dim
                                WHERE edw_material_plant_dim.plnt = '110S'
                                GROUP BY edw_material_plant_dim.plnt,
                                        LTRIM (edw_material_plant_dim.matl_plnt_view,'0'),
                                        edw_material_plant_dim.mstr_cd) matplnt ON   ltrim(mat.matl_num,'0') = matplnt.matl_num
    where ltrim(mat.matl_num,'0') in 
    (select distinct ltrim(matl_num,'0') as matl_num
    from edw_copa_trans_fact
    where co_cd = '3820'))
                UNION ALL
                SELECT DISTINCT 'Taiwan'::TEXT AS ctry_nm,
                        (prodattr.matl_num)::CHARACTER VARYING AS matl_num,
                        prodattr.prod_hier_l2 AS loc_prod1,
                        prodattr.prod_hier_l4 AS loc_prod2,
                        prodattr.prod_hier_l8 AS loc_prod3,
                        prodattr.prod_hier_l5 AS loc_prod4,
                        prodattr.prod_hier_l6 AS loc_prod5,
                        prodattr.prod_hier_l7 AS loc_prod6,
                        prodattr.ean AS loc_prod7,
                        prodattr.lcl_prod_nm AS loc_prod8,
                        prodattr.sap_matl_num AS loc_prod9,
                        prodattr.prod_hier_l9 AS loc_prod10
                FROM (SELECT matsls.sls_org,
                                matsls.matl_num,
                                pa.prod_hier_l2,
                                pa.prod_hier_l4,
                                pa.prod_hier_l8,
                                pa.prod_hier_l5,
                                pa.prod_hier_l6,
                                pa.prod_hier_l7,
                                pa.ean,
                                pa.lcl_prod_nm,
                                pa.sap_matl_num,
                                pa.prod_hier_l9
                        FROM ((SELECT DISTINCT derived_table1.sls_org,
                                    LTRIM((derived_table1.matl_num)::TEXT,'0'::TEXT) AS matl_num,
                                    derived_table1.ean_num
                                FROM (SELECT edw_material_sales_dim.sls_org,
                                            edw_material_sales_dim.matl_num,
                                            edw_material_sales_dim.ean_num,
                                            row_number() OVER (PARTITION BY edw_material_sales_dim.sls_org,edw_material_sales_dim.matl_num ORDER BY edw_material_sales_dim.ean_num DESC) AS rn
                                    FROM edw_material_sales_dim
                                    WHERE (((edw_material_sales_dim.ean_num IS NOT NULL) AND ((edw_material_sales_dim.ean_num)::TEXT <> ''::TEXT)) AND ((edw_material_sales_dim.sls_org)::TEXT = (1200)::TEXT))) derived_table1
                                WHERE (derived_table1.rn = 1)) matsls LEFT JOIN (SELECT derived_table2.prod_hier_l2,
                                                                                        derived_table2.prod_hier_l4,
                                                                                        derived_table2.prod_hier_l8,
                                                                                        derived_table2.prod_hier_l5,
                                                                                        derived_table2.prod_hier_l6,
                                                                                        derived_table2.prod_hier_l7,
                                                                                        derived_table2.ean,
                                                                                        derived_table2.lcl_prod_nm,
                                                                                        derived_table2.sap_matl_num,
                                                                                        derived_table2.prod_hier_l9
                                                                                FROM (SELECT edw_product_attr_dim.prod_hier_l2,
                                                                                            edw_product_attr_dim.prod_hier_l4,
                                                                                            edw_product_attr_dim.prod_hier_l8,
                                                                                            edw_product_attr_dim.prod_hier_l5,
                                                                                            edw_product_attr_dim.prod_hier_l6,
                                                                                            edw_product_attr_dim.prod_hier_l7,
                                                                                            edw_product_attr_dim.ean,
                                                                                            edw_product_attr_dim.lcl_prod_nm,
                                                                                            edw_product_attr_dim.sap_matl_num,
                                                                                            edw_product_attr_dim.prod_hier_l9,
                                                                                            row_number() OVER (PARTITION BY LTRIM((edw_product_attr_dim.ean)::TEXT,'0'::TEXT) ORDER BY LTRIM((edw_product_attr_dim.ean)::TEXT,'0'::TEXT) DESC) AS rn
                                                                                    FROM edw_product_attr_dim
                                                                                    WHERE ((edw_product_attr_dim.cntry)::TEXT = 'TW'::TEXT)) derived_table2
                                                                                WHERE (derived_table2.rn = 1)) pa ON ((LTRIM((matsls.ean_num)::TEXT,'0'::TEXT) = LTRIM((pa.ean)::TEXT,'0'::TEXT))))) prodattr)
                UNION ALL
    SELECT DISTINCT 'Philippines'::TEXT AS ctry_nm,
                        ltrim(gph.materialnumber,'0') AS matl_num,
                        gph.gcph_brand AS loc_prod1,
                        gph.gcph_franchise AS loc_prod2,
                        ph_prod.rpt_grp_2_desc AS loc_prod3,
                        gph.put_up_description AS loc_prod4,
                        gph.gcph_variant AS loc_prod5,
                        ph_prod.promo_reg_ind AS loc_prod6,
                        matplnt.mstr_cd AS loc_prod7,
                        'Not Available'::TEXT AS loc_prod8,
                        'Not Available'::TEXT AS loc_prod9,
                        'Not Available'::TEXT AS loc_prod10
    FROM
    (select ltrim(materialnumber,'0') as materialnumber,gcph_franchise,gcph_brand,put_up_description,
    gcph_variant
    from
    (select ltrim(materialnumber,'0') as materialnumber,gcph_franchise,gcph_brand,put_up_description,
    gcph_variant,row_number() over (partition by ltrim(materialnumber,'0') order by ltrim(materialnumber,'0') desc) as rn
    from
    edw_gch_producthierarchy) where rn = 1) gph
    left join 
    (SELECT DISTINCT itg_mds_ph_lav_product.item_cd,
                                itg_mds_ph_lav_product.promo_reg_ind,
                                itg_mds_ph_lav_product.rpt_grp_2_desc
                        FROM itg_mds_ph_lav_product
                        WHERE ((itg_mds_ph_lav_product.active)::TEXT = 'Y'::TEXT))
                        ph_prod on ltrim(gph.materialnumber,'0') = ltrim(ph_prod.item_cd)
    left join                    
    (SELECT edw_material_plant_dim.plnt,
    LTRIM((edw_material_plant_dim.matl_plnt_view)::TEXT,'0'::TEXT) AS matl_num,
    edw_material_plant_dim.mstr_cd
    FROM edw_material_plant_dim
    WHERE ((edw_material_plant_dim.plnt)::TEXT = (2300)::TEXT)
    GROUP BY edw_material_plant_dim.plnt,
    LTRIM((edw_material_plant_dim.matl_plnt_view)::TEXT,'0'::TEXT),
    edw_material_plant_dim.mstr_cd) matplnt ON ltrim(gph.materialnumber,'0')= matplnt.matl_num
                UNION ALL
                SELECT DISTINCT 'Vietnam'::TEXT AS ctry_nm,
                        LTRIM(mat.matl_num,'0') AS matl_num,
                        mat.pka_franchise_desc AS loc_prod1,
                        gph.gcph_brand || ' / ' || gph.gcph_segment AS loc_prod2,
                        gph.gcph_variant AS loc_prod3,
                        mat.pka_size_desc AS loc_prod4,
                        mat.pka_package_desc AS loc_prod5,
                        mat.pka_product_key_description AS loc_prod6,
                        'Not Available'::TEXT AS loc_prod7,
                        'Not Available'::TEXT AS loc_prod8,
                        'Not Available'::TEXT AS loc_prod9,
                        'Not Available'::TEXT AS loc_prod10
                FROM edw_material_dim mat
                    LEFT JOIN edw_gch_producthierarchy gph ON LTRIM (matl_num,'0') = LTRIM (gph.materialnumber,'0'))) derived_table3 
                    
    union all 
    Select distinct 'India' as ctry_nm,
                    ltrim(matl_num,'0') as  matl_num,
                    COALESCE(NULLIF(gph.gcph_franchise,''),'Not Available'::CHARACTER VARYING) AS loc_prod1,
                    COALESCE(NULLIF(mat.prodh3_txtmd,''),'Not Available'::CHARACTER VARYING) AS loc_prod2,
                    COALESCE(NULLIF(mat.mega_brnd_desc,''),'Not Available'::CHARACTER VARYING) AS loc_prod3,
                    COALESCE(NULLIF(mat.brnd_desc,''),'Not Available'::CHARACTER VARYING) AS loc_prod4,
                    COALESCE(NULLIF(mat.pka_sub_brand_desc,''),'Not Available'::CHARACTER VARYING) AS loc_prod5,
                    COALESCE(NULLIF(mat.prodh5_txtmd,''),'Not Available'::CHARACTER VARYING) AS loc_prod6,
                    COALESCE(NULLIF(mat.varnt_desc,''),'Not Available'::CHARACTER VARYING) AS loc_prod7,
                    COALESCE(NULLIF(mat.pka_size_desc,''),'Not Available'::CHARACTER VARYING) AS loc_prod8,
                    COALESCE(NULLIF(pka_package_desc,''),'Not Available'::CHARACTER VARYING) AS loc_prod9,
                    COALESCE(NULLIF(pka_product_key_description,''),'Not Available'::CHARACTER VARYING) AS loc_prod10
                    from edw_material_dim mat
                    LEFT JOIN edw_gch_producthierarchy gph ON LTRIM (matl_num,'0') = LTRIM (gph.materialnumber,'0')
)
select * from final