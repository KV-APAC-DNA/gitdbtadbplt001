with edw_material_plant_dim as(
    select * from {{ ref('aspedw_integration__edw_material_plant_dim') }}
),
edw_material_dim as(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_gch_producthierarchy as(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
edw_material_sales_dim as(
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_material_uom as(
    select * from {{ ref('aspedw_integration__edw_material_uom') }}
),
---logical cte---
remu as (
          SELECT
            edw_material_uom.material,
            edw_material_uom.unit,
            edw_material_uom.base_uom,
            edw_material_uom.record_mode,
            edw_material_uom.uomz1d,
            edw_material_uom.uomn1d,
            edw_material_uom.cdl_dttm,
            edw_material_uom.crtd_dttm,
            edw_material_uom.updt_dttm
          FROM edw_material_uom
          WHERE
            (
              (
                CAST((
                  edw_material_uom.base_uom
                ) AS TEXT) = CAST('PC' AS TEXT)
              )
              AND (
                CAST((
                  edw_material_uom.unit
                ) AS TEXT) = CAST('CSE' AS TEXT)
              )
            )
        ),
b as (
              SELECT
                edw_material_sales_dim.matl_num,
                MAX(CAST((
                  edw_material_sales_dim.dstr_chnl
                ) AS TEXT)) AS chnl
              FROM edw_material_sales_dim
              WHERE
                (
                  (
                    CAST((
                      edw_material_sales_dim.sls_org
                    ) AS TEXT) = CAST('2100' AS TEXT)
                  )
                  AND (
                    CAST((
                      edw_material_sales_dim.ean_num
                    ) AS TEXT) <> CAST('' AS TEXT)
                  )
                )
              GROUP BY
                edw_material_sales_dim.matl_num
            ) ,
derived_table1 as (
                SELECT
                  edw_material_sales_dim.matl_num,
                  edw_material_sales_dim.ean_num,
                  edw_material_sales_dim.dstr_chnl,
                  MAX(edw_material_sales_dim.launch_dt) AS launch_dt
                FROM edw_material_sales_dim
                WHERE
                  (
                    (
                      CAST((
                        edw_material_sales_dim.sls_org
                      ) AS TEXT) = CAST('2100' AS TEXT)
                    )
                    AND (
                      CAST((
                        edw_material_sales_dim.ean_num
                      ) AS TEXT) <> CAST('' AS TEXT)
                    )
                  )
                GROUP BY
                  edw_material_sales_dim.matl_num,
                  edw_material_sales_dim.ean_num,
                  edw_material_sales_dim.dstr_chnl
              ),
a as (
              SELECT
                derived_table1.matl_num,
                derived_table1.ean_num,
                derived_table1.dstr_chnl,
                derived_table1.launch_dt
              FROM  derived_table1
              GROUP BY
                derived_table1.matl_num,
                derived_table1.ean_num,
                derived_table1.dstr_chnl,
                derived_table1.launch_dt
            ),
rmsd as (
            SELECT
              a.matl_num,
              a.ean_num,
              a.launch_dt
            FROM  a, b
            WHERE
              (
                (
                  CAST((
                    a.matl_num
                  ) AS TEXT) = CAST((
                    b.matl_num
                  ) AS TEXT)
                )
                AND (
                  CAST((
                    a.dstr_chnl
                  ) AS TEXT) = b.chnl
                )
              )
          ) ,
rempd as (
        SELECT DISTINCT
          edw_material_plant_dim.matl_num,
          CAST('MY' AS TEXT) AS cntry_key,
          edw_material_plant_dim.prft_ctr
        FROM edw_material_plant_dim
        WHERE
          (
            (
              CAST((
                edw_material_plant_dim.plnt
              ) AS TEXT) = CAST('2100' AS TEXT)
            )
            OR (
              CAST((
                edw_material_plant_dim.plnt
              ) AS TEXT) = CAST('210A' AS TEXT)
            )
          )
      ),
my as (SELECT DISTINCT
        CAST((
          rempd.cntry_key
        ) AS VARCHAR(4)) AS cntry_key,
        LTRIM(CAST((
          remd.matl_num
        ) AS TEXT), CAST('0' AS TEXT)) AS sap_matl_num,
        remd.matl_desc AS sap_mat_desc,
        rmsd.ean_num,
        remd.matl_type_cd AS sap_mat_type_cd,
        remd.matl_type_desc AS sap_mat_type_desc,
        remd.base_uom_cd AS sap_base_uom_cd,
        remd.prch_uom_cd AS sap_prchse_uom_cd,
        remd.prodh1 AS sap_prod_sgmt_cd,
        remd.prodh1_txtmd AS sap_prod_sgmt_desc,
        remd.prod_base AS sap_base_prod_cd,
        remd.base_prod_desc AS sap_base_prod_desc,
        remd.mega_brnd_cd AS sap_mega_brnd_cd,
        remd.mega_brnd_desc AS sap_mega_brnd_desc,
        remd.brnd_cd AS sap_brnd_cd,
        remd.brnd_desc AS sap_brnd_desc,
        remd.vrnt AS sap_vrnt_cd,
        remd.varnt_desc AS sap_vrnt_desc,
        remd.put_up AS sap_put_up_cd,
        remd.put_up_desc AS sap_put_up_desc,
        remd.prodh2 AS sap_grp_frnchse_cd,
        remd.prodh2_txtmd AS sap_grp_frnchse_desc,
        remd.prodh3 AS sap_frnchse_cd,
        remd.prodh3_txtmd AS sap_frnchse_desc,
        remd.prodh4 AS sap_prod_frnchse_cd,
        remd.prodh4_txtmd AS sap_prod_frnchse_desc,
        remd.prodh5 AS sap_prod_mjr_cd,
        remd.prodh5_txtmd AS sap_prod_mjr_desc,
        remd.prodh5 AS sap_prod_mnr_cd,
        remd.prodh5_txtmd AS sap_prod_mnr_desc,
        remd.prodh6 AS sap_prod_hier_cd,
        remd.prodh6_txtmd AS sap_prod_hier_desc,
        regph."region" AS gph_region,
        regph.regional_franchise AS gph_reg_frnchse,
        regph.regional_franchise_group AS gph_reg_frnchse_grp,
        regph.gcph_franchise AS gph_prod_frnchse,
        regph.gcph_brand AS gph_prod_brnd,
        regph.gcph_subbrand AS gph_prod_sub_brnd,
        regph.gcph_variant AS gph_prod_vrnt,
        regph.gcph_needstate AS gph_prod_needstate,
        regph.gcph_category AS gph_prod_ctgry,
        regph.gcph_subcategory AS gph_prod_subctgry,
        regph.gcph_segment AS gph_prod_sgmnt,
        regph.gcph_subsegment AS gph_prod_subsgmnt,
        regph.put_up_code AS gph_prod_put_up_cd,
        regph.put_up_description AS gph_prod_put_up_desc,
        regph.size AS gph_prod_size,
        regph.unit_of_measure AS gph_prod_size_uom,
        rmsd.launch_dt,
        CAST((
          (
            remu.uomz1d / remu.uomn1d
          )
        ) AS VARCHAR) AS qty_shipper_pc,
        LTRIM(CAST((
          rempd.prft_ctr
        ) AS TEXT), CAST('0' AS TEXT)) AS prft_ctr,
        LTRIM(CAST((
          remd.tot_shlf_lif
        ) AS TEXT), CAST('0' AS TEXT)) AS shlf_life
      FROM rempd, (
        (
          (
           edw_material_dim AS remd
              LEFT JOIN edw_gch_producthierarchy AS regph
                ON (
                  (
                    (
                      LTRIM(CAST((
                        remd.matl_num
                      ) AS TEXT), CAST('0' AS TEXT)) = LTRIM(CAST((
                        regph.materialnumber
                      ) AS TEXT), CAST('0' AS TEXT))
                    )
                  )
                )
          )
          LEFT JOIN  rmsd
            ON (
              (
                (
                  LTRIM(CAST((
                    remd.matl_num
                  ) AS TEXT), CAST('0' AS TEXT)) = LTRIM(CAST((
                    rmsd.matl_num
                  ) AS TEXT), CAST('0' AS TEXT))
                )
              )
            )
        )
        LEFT JOIN  remu
          ON (
            (
              (
                LTRIM(CAST((
                  remd.matl_num
                ) AS TEXT), CAST('0' AS TEXT)) = LTRIM(CAST((
                  remu.material
                ) AS TEXT), CAST('0' AS TEXT))
              )
            )
          )
      )
      WHERE
        (
          (
            (
              LTRIM(CAST((
                remd.matl_num
              ) AS TEXT), CAST('0' AS TEXT)) = LTRIM(CAST((
                rempd.matl_num
              ) AS TEXT), CAST('0' AS TEXT))
            )
            AND (
              CAST((
                remd.prod_hier_cd
              ) AS TEXT) <> CAST('' AS TEXT)
            )
          )
          AND (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            CAST((
                              remd.matl_type_cd
                            ) AS TEXT) = CAST('FERT' AS TEXT)
                          )
                          OR (
                            CAST((
                              remd.matl_type_cd
                            ) AS TEXT) = CAST('HALB' AS TEXT)
                          )
                        )
                        OR (
                          CAST((
                            remd.matl_type_cd
                          ) AS TEXT) = CAST('ROH' AS TEXT)
                        )
                      )
                      OR (
                        CAST((
                          remd.matl_type_cd
                        ) AS TEXT) = CAST('FER2' AS TEXT)
                      )
                    )
                    OR (
                      CAST((
                        remd.matl_type_cd
                      ) AS TEXT) = CAST('PROM' AS TEXT)
                    )
                  )
                  OR (
                    CAST((
                      remd.matl_type_cd
                    ) AS TEXT) = CAST('SAPR' AS TEXT)
                  )
                )
                OR (
                  CAST((
                    remd.matl_type_cd
                  ) AS TEXT) = CAST('DISP' AS TEXT)
                )
              )
              OR (
                CAST((
                  remd.matl_type_cd
                ) AS TEXT) = CAST('ERSA' AS TEXT)
              )
            )
            OR (
              CAST((
                remd.matl_type_cd
              ) AS TEXT) = CAST('Z001' AS TEXT)
            )
          )
        )
    ),
final as 
(
    SELECT
      my.cntry_key,
      my.sap_matl_num,
      my.sap_mat_desc,
      my.ean_num,
      my.sap_mat_type_cd,
      my.sap_mat_type_desc,
      my.sap_base_uom_cd,
      my.sap_prchse_uom_cd,
      my.sap_prod_sgmt_cd,
      my.sap_prod_sgmt_desc,
      my.sap_base_prod_cd,
      my.sap_base_prod_desc,
      my.sap_mega_brnd_cd,
      my.sap_mega_brnd_desc,
      my.sap_brnd_cd,
      my.sap_brnd_desc,
      my.sap_vrnt_cd,
      my.sap_vrnt_desc,
      my.sap_put_up_cd,
      my.sap_put_up_desc,
      my.sap_grp_frnchse_cd,
      my.sap_grp_frnchse_desc,
      my.sap_frnchse_cd,
      my.sap_frnchse_desc,
      my.sap_prod_frnchse_cd,
      my.sap_prod_frnchse_desc,
      my.sap_prod_mjr_cd,
      my.sap_prod_mjr_desc,
      my.sap_prod_mnr_cd,
      my.sap_prod_mnr_desc,
      my.sap_prod_hier_cd,
      my.sap_prod_hier_desc,
      my.gph_region,
      my.gph_reg_frnchse,
      my.gph_reg_frnchse_grp,
      my.gph_prod_frnchse,
      my.gph_prod_brnd,
      my.gph_prod_sub_brnd,
      my.gph_prod_vrnt,
      my.gph_prod_needstate,
      my.gph_prod_ctgry,
      my.gph_prod_subctgry,
      my.gph_prod_sgmnt,
      my.gph_prod_subsgmnt,
      my.gph_prod_put_up_cd,
      my.gph_prod_put_up_desc,
      my.gph_prod_size,
      my.gph_prod_size_uom,
      my.launch_dt,
      my.qty_shipper_pc,
      my.prft_ctr,
      my.shlf_life
    FROM 
       my
  )
select * from final