with edw_vw_os_time_dim as (
select * from DEV_DNA_CORE.SNENAV01_WORKSPACE.EDW_VW_OS_TIME_DIM
),
itg_mds_ph_distributor_product as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_MDS_PH_DISTRIBUTOR_PRODUCT
),
itg_mds_ph_lav_product as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_MDS_PH_LAV_PRODUCT
),
itg_mds_ph_pos_pricelist as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_MDS_PH_POS_PRICELIST
),
final as (
  SELECT 
    'PH' AS cntry_cd, 
    'Philippines' AS cntry_nm, 
    (impdp.dstrbtr_grp_cd):: character varying(20) AS dstrbtr_grp_cd, 
   null AS dstrbtr_soldto_code, 
   null AS sap_soldto_code, 
    impdp.dstrbtr_item_cd AS dstrbtr_matl_num, 
    (impdp.dstrbtr_item_nm):: character varying(500) AS dstrbtr_matl_desc, 
    (NULL :: character varying):: character varying(20) AS dstrbtr_alt_matl_num, 
   null AS dstrbtr_alt_matl_desc, 
    (NULL :: character varying):: character varying(20) AS dstrbtr_alt2_matl_num, 
   null AS dstrbtr_alt2_matl_desc, 
   null AS dstrbtr_bar_cd, 
    (implp.item_cd):: character varying(255) AS sap_matl_num, 
   null AS pc_per_case, 
    (
      CASE WHEN (
        (
          (epp.status):: text = '**' :: text
        ) 
        AND (
          (
            veotd.mnth_id >= epp.launch_period
          ) 
          AND (veotd.mnth_id <= epp.end_period)
        )
      ) THEN 'Y' :: text ELSE 'N' :: text END
    ):: character varying AS is_npi, 
    (epp.launch_period):: character varying(52) AS npi_str_period, 
    (epp.end_period):: character varying(52) AS npi_end_period, 
    (
      CASE WHEN (
        upper(
          (implp.promo_reg_ind):: text
        ) = 'REG' :: text
      ) THEN 'Y' :: text ELSE 'N' :: text END
    ):: character varying AS is_reg, 
    (
      CASE WHEN (
        upper(
          (implp.promo_reg_ind):: text
        ) = 'PROMO' :: text
      ) THEN 'Y' :: text ELSE 'N' :: text END
    ):: character varying AS is_promo, 
    (implp.promo_strt_period):: character varying(10) AS promo_strt_period, 
    (implp.promo_end_period):: character varying(10) AS promo_end_period, 
   null AS is_mcl, 
    (implp.hero_sku_ind):: character varying(10) AS is_hero, 
   null AS eff_strt_dt, 
   null AS eff_end_dt 
  FROM 
    (
      SELECT 
        edw_vw_os_time_dim.mnth_id 
      FROM 
        edw_vw_os_time_dim 
      WHERE 
        (
          edw_vw_os_time_dim.cal_date = to_date(
            current_timestamp():: timestamp without time zone
          )
        )
    ) veotd, 
    (
      (
        SELECT 
          itg_mds_ph_distributor_product.dstrbtr_item_cd, 
          itg_mds_ph_distributor_product.dstrbtr_item_nm, 
          itg_mds_ph_distributor_product.sap_item_cd, 
          itg_mds_ph_distributor_product.sap_item_nm, 
          itg_mds_ph_distributor_product.dstrbtr_grp_cd, 
          itg_mds_ph_distributor_product.dstrbtr_grp_nm, 
          itg_mds_ph_distributor_product.promo_strt_period, 
          itg_mds_ph_distributor_product.promo_end_period, 
          itg_mds_ph_distributor_product.promo_reg_ind, 
          itg_mds_ph_distributor_product.promo_reg_nm, 
          itg_mds_ph_distributor_product.last_chg_datetime, 
          itg_mds_ph_distributor_product.effective_from, 
          itg_mds_ph_distributor_product.effective_to, 
          itg_mds_ph_distributor_product.active, 
          itg_mds_ph_distributor_product.crtd_dttm, 
          itg_mds_ph_distributor_product.updt_dttm 
        FROM 
          itg_mds_ph_distributor_product 
        WHERE 
          (
            (
              itg_mds_ph_distributor_product.active
            ):: text = 'Y' :: text
          )
      ) impdp 
      LEFT JOIN (
        (
          SELECT 
            itg_mds_ph_lav_product.item_cd, 
            itg_mds_ph_lav_product.item_nm, 
            itg_mds_ph_lav_product.ims_otc_tag, 
            itg_mds_ph_lav_product.ims_otc_tag_nm, 
            itg_mds_ph_lav_product.npi_strt_period, 
            itg_mds_ph_lav_product.price_lst_period, 
            itg_mds_ph_lav_product.promo_strt_period, 
            itg_mds_ph_lav_product.promo_end_period, 
            itg_mds_ph_lav_product.promo_reg_ind, 
            itg_mds_ph_lav_product.promo_reg_nm, 
            itg_mds_ph_lav_product.hero_sku_ind, 
            itg_mds_ph_lav_product.hero_sku_nm, 
            itg_mds_ph_lav_product.rpt_grp_1, 
            itg_mds_ph_lav_product.rpt_grp_1_desc, 
            itg_mds_ph_lav_product.rpt_grp_2, 
            itg_mds_ph_lav_product.rpt_grp_2_desc, 
            itg_mds_ph_lav_product.rpt_grp_3, 
            itg_mds_ph_lav_product.rpt_grp_3_desc, 
            itg_mds_ph_lav_product.rpt_grp_4, 
            itg_mds_ph_lav_product.rpt_grp_4_desc, 
            itg_mds_ph_lav_product.rpt_grp_5, 
            itg_mds_ph_lav_product.rpt_grp_5_desc, 
            itg_mds_ph_lav_product.scard_brand_cd, 
            itg_mds_ph_lav_product.scard_brand_desc, 
            itg_mds_ph_lav_product.scard_franchise_cd, 
            itg_mds_ph_lav_product.scard_franchise_desc, 
            itg_mds_ph_lav_product.scard_put_up_cd, 
            itg_mds_ph_lav_product.scard_put_up_desc, 
            itg_mds_ph_lav_product.scard_varient_cd, 
            itg_mds_ph_lav_product.scard_varient_desc, 
            itg_mds_ph_lav_product.last_chg_datetime, 
            itg_mds_ph_lav_product.effective_from, 
            itg_mds_ph_lav_product.effective_to, 
            itg_mds_ph_lav_product.active, 
            itg_mds_ph_lav_product.crtd_dttm, 
            itg_mds_ph_lav_product.updt_dttm 
          FROM 
            itg_mds_ph_lav_product 
          WHERE 
            (
              (itg_mds_ph_lav_product.active):: text = 'Y' :: text
            )
        ) implp 
        LEFT JOIN (
          SELECT 
            itg_mds_ph_pos_pricelist.status, 
            itg_mds_ph_pos_pricelist.item_cd, 
            min(
              (
                itg_mds_ph_pos_pricelist.jj_mnth_id
              ):: text
            ) AS launch_period, 
            min(
              to_char(
                add_months(
                  (
                    (
                      concat(
                        (
                          itg_mds_ph_pos_pricelist.jj_mnth_id
                        ):: text, 
                        '01' :: text
                      )
                    ):: date
                  ):: timestamp without time zone, 
                  (11):: bigint
                ), 
                'YYYYMM' :: text
              )
            ) AS end_period 
          FROM 
            itg_mds_ph_pos_pricelist 
          WHERE 
            (
              (
                (
                  itg_mds_ph_pos_pricelist.status
                ):: text = '**' :: text
              ) 
              AND (
                (
                  itg_mds_ph_pos_pricelist.active
                ):: text = 'Y' :: text
              )
            ) 
          GROUP BY 
            itg_mds_ph_pos_pricelist.status, 
            itg_mds_ph_pos_pricelist.item_cd
        ) epp ON (
          (
            (
              ltrim((epp.item_cd):: text, '0' :: text
              ) = ltrim((implp.item_cd):: text, 
                '0' :: text
              )
            )
          )
        )
      ) ON (
        (
          (
            ltrim(
              (impdp.sap_item_cd):: text, 
              '0' :: text
            ) = ltrim(
              (implp.item_cd):: text, 
              '0' :: text
            )
          )
        )
      )
    ) 
  
  ) 
  select * from final
