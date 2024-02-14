with edw_vw_my_curr_dim as (
  select * from {{ ref('mysedw_integration__edw_vw_my_curr_dim') }}
),
edw_vw_my_pos_sales_fact as
(
  select * from {{ ref('mysedw_integration__edw_vw_my_pos_sales_fact') }}
),
edw_vw_os_time_dim as
(
  select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_vw_my_material_dim as 
(
  select * from {{ ref('mysedw_integration__edw_vw_my_material_dim') }}
),
edw_vw_my_customer_dim as 
(
  select * from {{ ref('mysedw_integration__edw_vw_my_customer_dim') }}
),
edw_vw_my_pos_material_dim as 
(
  select * from {{ ref('mysedw_integration__edw_vw_my_pos_material_dim') }}
),
edw_vw_my_pos_customer_dim as 
(
   select * from {{ ref('mysedw_integration__edw_vw_my_pos_customer_dim') }}
),
edw_vw_my_billing_fact as
(
  select * from {{ ref('mysedw_integration__edw_vw_my_billing_fact') }}
),
itg_my_pos_cust_mstr as
(
  select * from {{ ref('mysitg_integration__itg_my_pos_cust_mstr') }}
),
itg_my_customer_dim as
(
  select * from {{ ref('mysitg_integration__itg_my_customer_dim') }}
),
itg_my_material_dim as
(
  select * from {{ ref('mysitg_integration__itg_my_material_dim') }}
),
final as (
select
  'POS' as data_src,
  veposf.year as jj_year,
  cast((
    veposf.qrtr
  ) as varchar) as jj_qtr,
  cast((
    veposf.mnth_id
  ) as varchar) as jj_mnth_id,
  veposf.mnth_no as jj_mnth_no,
  veposf.jj_yr_week_no as jj_year_wk_no,
  veposf.cntry_nm,
  vopcd.cust_cd,
  vopcd.brnch_cd as cust_brnch_cd,
  vopcd.brnch_nm as mt_cust_brnch_nm,
  vopcd.dept_cd as cust_dept_cd,
  vopcd.dept_nm as mt_cust_dept_nm,
  vopcd.region_cd,
  vopcd.region_nm,
  vopmd.item_cd,
  cast((
    vopmd.item_nm
  ) as varchar) as mt_item_nm,
  cast((
    upper(trim(cast((
      vopcd.sold_to
    ) as text)))
  ) as varchar) as sold_to,
  veocd.sap_cust_nm as sold_to_nm,
  'MODERN TRADE' as trade_type,
  imcd.dstrbtr_grp_cd,
  imcd.dstrbtr_grp_nm,
  veocd.sap_state_cd,
  veocd.sap_sls_org,
  veocd.sap_cmp_id,
  veocd.sap_cntry_cd,
  veocd.sap_cntry_nm,
  veocd.sap_addr,
  veocd.sap_region,
  veocd.sap_city,
  veocd.sap_post_cd,
  veocd.sap_chnl_cd,
  veocd.sap_chnl_desc,
  veocd.sap_sls_office_cd,
  veocd.sap_sls_office_desc,
  veocd.sap_sls_grp_cd,
  veocd.sap_sls_grp_desc,
  veocd.sap_curr_cd,
  veocd.gch_region,
  veocd.gch_cluster,
  veocd.gch_subcluster,
  veocd.gch_market,
  veocd.gch_retail_banner,
  cast((
    ltrim(cast((
      veomd.sap_matl_num
    ) as text), cast((
      cast('0' as varchar)
    ) as text))
  ) as varchar) as sku,
  immd.frnchse_desc,
  immd.brnd_desc,
  immd.vrnt_desc,
  immd.putup_desc,
  immd.item_desc2,
  veomd.sap_mat_desc as sku_desc,
  veomd.sap_mat_type_cd,
  veomd.sap_mat_type_desc,
  veomd.sap_base_uom_cd,
  veomd.sap_prchse_uom_cd,
  veomd.sap_prod_sgmt_cd,
  veomd.sap_prod_sgmt_desc,
  veomd.sap_base_prod_cd,
  veomd.sap_base_prod_desc,
  veomd.sap_mega_brnd_cd,
  veomd.sap_mega_brnd_desc,
  veomd.sap_brnd_cd,
  veomd.sap_brnd_desc,
  veomd.sap_vrnt_cd,
  veomd.sap_vrnt_desc,
  veomd.sap_put_up_cd,
  veomd.sap_put_up_desc,
  veomd.sap_grp_frnchse_cd,
  veomd.sap_grp_frnchse_desc,
  veomd.sap_frnchse_cd,
  veomd.sap_frnchse_desc,
  veomd.sap_prod_frnchse_cd,
  veomd.sap_prod_frnchse_desc,
  veomd.sap_prod_mjr_cd,
  veomd.sap_prod_mjr_desc,
  veomd.sap_prod_mnr_cd,
  veomd.sap_prod_mnr_desc,
  veomd.sap_prod_hier_cd,
  veomd.sap_prod_hier_desc,
  veomd.gph_region as global_mat_region,
  veomd.gph_prod_frnchse as global_prod_franchise,
  veomd.gph_prod_brnd as global_prod_brand,
  veomd.gph_prod_vrnt as global_prod_variant,
  veomd.gph_prod_put_up_cd as global_prod_put_up_cd,
  veomd.gph_prod_put_up_desc as global_put_up_desc,
  veomd.gph_prod_sub_brnd as global_prod_sub_brand,
  veomd.gph_prod_needstate as global_prod_need_state,
  veomd.gph_prod_ctgry as global_prod_category,
  veomd.gph_prod_subctgry as global_prod_subcategory,
  veomd.gph_prod_sgmnt as global_prod_segment,
  veomd.gph_prod_subsgmnt as global_prod_subsegment,
  veomd.gph_prod_size as global_prod_size,
  veomd.gph_prod_size_uom as global_prod_size_uom,
  veocurd.from_ccy,
  veocurd.to_ccy,
  veocurd.exch_rate,
  null as bill_type,
  0 as bill_qty_pc,
  0 as billing_grs_trd_sls,
  0 as billing_subtot2,
  0 as billing_subtot3,
  0 as billing_subtot4,
  0 as billing_net_amt,
  0 as billing_est_nts,
  0 as billing_invoice_val,
  0 as billing_gross_val,
  veposf.pos_qty,
  (
    veposf.pos_gts * cast((
      veocurd.exch_rate
    ) as double)
  ) as pos_gts,
  (
    veposf.pos_item_prc * cast((
      veocurd.exch_rate
    ) as double)
  ) as pos_item_prc,
  veposf.pos_tax,
  (
    veposf.pos_nts * cast((
      veocurd.exch_rate
    ) as double)
  ) as pos_nts,
  veposf.conv_factor,
  veposf.jj_qty_pc as jj_pos_qty_pc,
  (
    veposf.jj_item_prc_per_pc * veocurd.exch_rate
  ) as jj_pos_item_prc_per_pc,
  (
    veposf.jj_gts * veocurd.exch_rate
  ) as jj_pos_gts,
  veposf.jj_vat_amt as jj_pos_vat_amt,
  (
    veposf.jj_nts * veocurd.exch_rate
  ) as jj_pos_nts,
  immd.npi_ind as is_npi,
  immd.npi_strt_period as npi_str_period,
  null  as npi_end_period,
  null  as is_reg,
  immd.promo_reg_ind as is_promo,
  null  as promo_strt_period,
  null  as promo_end_period,
  null  as is_mcl,
  immd.hero_ind as is_hero
from (
  select
    d.cntry_key,
    d.cntry_nm,
    d.rate_type,
    d.from_ccy,
    d.to_ccy,
    d.valid_date,
    d.jj_year,
    d.start_period,
    case
      when (
        d.end_mnth_id = b.max_period
      )
      then cast((
        cast('209912' as varchar)
      ) as text)
      else d.end_mnth_id
    end as end_period,
    d.exch_rate
  from (
    select
      a.cntry_key,
      a.cntry_nm,
      a.rate_type,
      a.from_ccy,
      a.to_ccy,
      a.valid_date,
      a.jj_year,
      min(cast((
        a.jj_mnth_id
      ) as text)) as start_period,
      max(cast((
        a.jj_mnth_id
      ) as text)) as end_mnth_id,
      a.exch_rate
    from edw_vw_my_curr_dim as a
    where
      (
        cast((
          a.cntry_key
        ) as text) = cast((
          cast('MY' as varchar)
        ) as text)
      )
    group by
      a.cntry_key,
      a.cntry_nm,
      a.rate_type,
      a.from_ccy,
      a.to_ccy,
      a.valid_date,
      a.jj_year,
      a.exch_rate
  ) as d, (
    select
      max(cast((
        a.jj_mnth_id
      ) as text)) as max_period
    from edw_vw_my_curr_dim as a
    where
      (
        cast((
          a.cntry_key
        ) as text) = cast((
          cast('MY' as varchar)
        ) as text)
      )
  ) as b
) as veocurd, (
  (
    (
      (
        (
          (
            (
              select
                case
                  when (
                    (
                      NOT a.jj_yr_week_no IS null
                    )
                    OR (
                      cast((
                        a.jj_yr_week_no
                      ) as text) <> cast((
                        cast('' as varchar)
                      ) as text)
                    )
                  )
                  then b1.year
                  else b2.year
                end as year,
                case
                  when (
                    (
                      NOT a.jj_yr_week_no IS null
                    )
                    OR (
                      cast((
                        a.jj_yr_week_no
                      ) as text) <> cast((
                        cast('' as varchar)
                      ) as text)
                    )
                  )
                  then b1.qrtr
                  else b2.qrtr
                end as qrtr,
                case
                  when (
                    (
                      NOT a.jj_yr_week_no IS null
                    )
                    OR (
                      cast((
                        a.jj_yr_week_no
                      ) as text) <> cast((
                        cast('' as varchar)
                      ) as text)
                    )
                  )
                  then b1.mnth_id
                  else b2.mnth_id
                end as mnth_id,
                case
                  when (
                    (
                      NOT a.jj_yr_week_no IS null
                    )
                    OR (
                      cast((
                        a.jj_yr_week_no
                      ) as text) <> cast((
                        cast('' as varchar)
                      ) as text)
                    )
                  )
                  then b1.mnth_no
                  else b2.mnth_no
                end as mnth_no,
                a.jj_yr_week_no,
                a.cntry_nm,
                a.cust_cd,
                a.cust_brnch_cd,
                a.item_cd,
                a.sap_matl_num,
                a.pos_qty,
                a.pos_gts,
                a.pos_item_prc,
                a.pos_tax,
                a.pos_nts,
                a.conv_factor,
                a.jj_qty_pc,
                a.jj_item_prc_per_pc,
                a.jj_gts,
                a.jj_vat_amt,
                a.jj_nts
              from (
                (
                  edw_vw_my_pos_sales_fact as a
                    left join (
                      select distinct
                        (
                          cast((
                            cast((
                              edw_vw_os_time_dim.year
                            ) as varchar)
                          ) as text) || RIGHT(
                            (
                              cast((
                                cast('00' as varchar)
                              ) as text) || cast((
                                cast((
                                  edw_vw_os_time_dim.wk
                                ) as varchar)
                              ) as text)
                            ),
                            2
                          )
                        ) as yr_wk,
                        edw_vw_os_time_dim.wk,
                        edw_vw_os_time_dim.mnth_id,
                        edw_vw_os_time_dim.year,
                        edw_vw_os_time_dim.qrtr,
                        edw_vw_os_time_dim.mnth_no
                      from edw_vw_os_time_dim
                    ) as b1
                      ON (
                        case
                          when (
                            (
                              NOT a.jj_yr_week_no IS null
                            )
                            OR (
                              cast((
                                a.jj_yr_week_no
                              ) as text) <> cast((
                                cast('' as varchar)
                              ) as text)
                            )
                          )
                          then (
                            cast((
                              a.jj_yr_week_no
                            ) as text) = b1.yr_wk
                          )
                          else cast(null as BOOLEAN)
                        end
                      )
                )
                left join (
                  select distinct
                    edw_vw_os_time_dim.mnth_id,
                    edw_vw_os_time_dim.year,
                    edw_vw_os_time_dim.qrtr,
                    edw_vw_os_time_dim.mnth_no
                  from edw_vw_os_time_dim
                ) as b2
                  ON (
                    case
                      when (
                        (
                          a.jj_yr_week_no IS null
                        )
                        OR (
                          cast((
                            a.jj_yr_week_no
                          ) as text) = cast((
                            cast('' as varchar)
                          ) as text)
                        )
                      )
                      then (
                        cast((
                          a.jj_mnth_id
                        ) as text) = b2.mnth_id
                      )
                      else cast(null as BOOLEAN)
                    end
                  )
              )
              where
                (
                  cast((
                    a.cntry_cd
                  ) as text) = cast((
                    cast('MY' as varchar)
                  ) as text)
                )
            ) as veposf
            left join (
              select
                edw_vw_my_material_dim.cntry_key,
                edw_vw_my_material_dim.sap_matl_num,
                edw_vw_my_material_dim.sap_mat_desc,
                edw_vw_my_material_dim.ean_num,
                edw_vw_my_material_dim.sap_mat_type_cd,
                edw_vw_my_material_dim.sap_mat_type_desc,
                edw_vw_my_material_dim.sap_base_uom_cd,
                edw_vw_my_material_dim.sap_prchse_uom_cd,
                edw_vw_my_material_dim.sap_prod_sgmt_cd,
                edw_vw_my_material_dim.sap_prod_sgmt_desc,
                edw_vw_my_material_dim.sap_base_prod_cd,
                edw_vw_my_material_dim.sap_base_prod_desc,
                edw_vw_my_material_dim.sap_mega_brnd_cd,
                edw_vw_my_material_dim.sap_mega_brnd_desc,
                edw_vw_my_material_dim.sap_brnd_cd,
                edw_vw_my_material_dim.sap_brnd_desc,
                edw_vw_my_material_dim.sap_vrnt_cd,
                edw_vw_my_material_dim.sap_vrnt_desc,
                edw_vw_my_material_dim.sap_put_up_cd,
                edw_vw_my_material_dim.sap_put_up_desc,
                edw_vw_my_material_dim.sap_grp_frnchse_cd,
                edw_vw_my_material_dim.sap_grp_frnchse_desc,
                edw_vw_my_material_dim.sap_frnchse_cd,
                edw_vw_my_material_dim.sap_frnchse_desc,
                edw_vw_my_material_dim.sap_prod_frnchse_cd,
                edw_vw_my_material_dim.sap_prod_frnchse_desc,
                edw_vw_my_material_dim.sap_prod_mjr_cd,
                edw_vw_my_material_dim.sap_prod_mjr_desc,
                edw_vw_my_material_dim.sap_prod_mnr_cd,
                edw_vw_my_material_dim.sap_prod_mnr_desc,
                edw_vw_my_material_dim.sap_prod_hier_cd,
                edw_vw_my_material_dim.sap_prod_hier_desc,
                edw_vw_my_material_dim.gph_region,
                edw_vw_my_material_dim.gph_reg_frnchse,
                edw_vw_my_material_dim.gph_reg_frnchse_grp,
                edw_vw_my_material_dim.gph_prod_frnchse,
                edw_vw_my_material_dim.gph_prod_brnd,
                edw_vw_my_material_dim.gph_prod_sub_brnd,
                edw_vw_my_material_dim.gph_prod_vrnt,
                edw_vw_my_material_dim.gph_prod_needstate,
                edw_vw_my_material_dim.gph_prod_ctgry,
                edw_vw_my_material_dim.gph_prod_subctgry,
                edw_vw_my_material_dim.gph_prod_sgmnt,
                edw_vw_my_material_dim.gph_prod_subsgmnt,
                edw_vw_my_material_dim.gph_prod_put_up_cd,
                edw_vw_my_material_dim.gph_prod_put_up_desc,
                edw_vw_my_material_dim.gph_prod_size,
                edw_vw_my_material_dim.gph_prod_size_uom,
                edw_vw_my_material_dim.launch_dt,
                edw_vw_my_material_dim.qty_shipper_pc,
                edw_vw_my_material_dim.prft_ctr,
                edw_vw_my_material_dim.shlf_life
              from edw_vw_my_material_dim
              where
                (
                  cast((
                    edw_vw_my_material_dim.cntry_key
                  ) as text) = cast((
                    cast('MY' as varchar)
                  ) as text)
                )
            ) as veomd
              ON (
                (
                  upper(
                    ltrim(
                      cast((
                        veomd.sap_matl_num
                      ) as text),
                      cast((
                        cast((
                          0
                        ) as varchar)
                      ) as text)
                    )
                  ) = ltrim(cast((
                    veposf.sap_matl_num
                  ) as text), cast((
                    cast('0' as varchar)
                  ) as text))
                )
              )
          )
          left join (
            select
              edw_vw_my_customer_dim.sap_cust_id,
              edw_vw_my_customer_dim.sap_cust_nm,
              edw_vw_my_customer_dim.sap_sls_org,
              edw_vw_my_customer_dim.sap_cmp_id,
              edw_vw_my_customer_dim.sap_cntry_cd,
              edw_vw_my_customer_dim.sap_cntry_nm,
              edw_vw_my_customer_dim.sap_addr,
              edw_vw_my_customer_dim.sap_region,
              edw_vw_my_customer_dim.sap_state_cd,
              edw_vw_my_customer_dim.sap_city,
              edw_vw_my_customer_dim.sap_post_cd,
              edw_vw_my_customer_dim.sap_chnl_cd,
              edw_vw_my_customer_dim.sap_chnl_desc,
              edw_vw_my_customer_dim.sap_sls_office_cd,
              edw_vw_my_customer_dim.sap_sls_office_desc,
              edw_vw_my_customer_dim.sap_sls_grp_cd,
              edw_vw_my_customer_dim.sap_sls_grp_desc,
              edw_vw_my_customer_dim.sap_curr_cd,
              edw_vw_my_customer_dim.sap_prnt_cust_key,
              edw_vw_my_customer_dim.sap_prnt_cust_desc,
              edw_vw_my_customer_dim.sap_cust_chnl_key,
              edw_vw_my_customer_dim.sap_cust_chnl_desc,
              edw_vw_my_customer_dim.sap_cust_sub_chnl_key,
              edw_vw_my_customer_dim.sap_sub_chnl_desc,
              edw_vw_my_customer_dim.sap_go_to_mdl_key,
              edw_vw_my_customer_dim.sap_go_to_mdl_desc,
              edw_vw_my_customer_dim.sap_bnr_key,
              edw_vw_my_customer_dim.sap_bnr_desc,
              edw_vw_my_customer_dim.sap_bnr_frmt_key,
              edw_vw_my_customer_dim.sap_bnr_frmt_desc,
              edw_vw_my_customer_dim.retail_env,
              edw_vw_my_customer_dim.gch_region,
              edw_vw_my_customer_dim.gch_cluster,
              edw_vw_my_customer_dim.gch_subcluster,
              edw_vw_my_customer_dim.gch_market,
              edw_vw_my_customer_dim.gch_retail_banner
            from edw_vw_my_customer_dim
            where
              (
                cast((
                  edw_vw_my_customer_dim.sap_cntry_cd
                ) as text) = cast((
                  cast('MY' as varchar)
                ) as text)
              )
          ) as veocd
            ON (
              (
                upper(
                  ltrim(cast((
                    veocd.sap_cust_id
                  ) as text), cast((
                    cast('0' as varchar)
                  ) as text))
                ) = upper(trim(substring(cast((
                  veposf.cust_cd
                ) as text), 0, 7)))
              )
            )
        )
        left join (
          select
            edw_vw_my_pos_material_dim.item_cd,
            max(cast((
              edw_vw_my_pos_material_dim.item_nm
            ) as text)) as item_nm,
            edw_vw_my_pos_material_dim.cust_cd,
            edw_vw_my_pos_material_dim.sap_item_cd
          from edw_vw_my_pos_material_dim
          where
            (
              cast((
                edw_vw_my_pos_material_dim.cntry_cd
              ) as text) = cast((
                cast('MY' as varchar)
              ) as text)
            )
          group by
            edw_vw_my_pos_material_dim.item_cd,
            edw_vw_my_pos_material_dim.cust_cd,
            edw_vw_my_pos_material_dim.sap_item_cd
        ) as vopmd
          ON (
            (
              (
                (
                  upper(
                    ltrim(cast((
                      vopmd.sap_item_cd
                    ) as text), cast((
                      cast('0' as varchar)
                    ) as text))
                  ) = ltrim(cast((
                    veposf.sap_matl_num
                  ) as text), cast((
                    cast('0' as varchar)
                  ) as text))
                )
                AND (
                  upper(
                    substring(
                      ltrim(cast((
                        vopmd.cust_cd
                      ) as text), cast((
                        cast('0' as varchar)
                      ) as text)),
                      0,
                      7
                    )
                  ) = upper(trim(substring(cast((
                    veposf.cust_cd
                  ) as text), 0, 7)))
                )
              )
              AND (
                upper(
                  ltrim(cast((
                    vopmd.item_cd
                  ) as text), cast((
                    cast('0' as varchar)
                  ) as text))
                ) = upper(trim(cast((
                  veposf.item_cd
                ) as text)))
              )
            )
          )
      )
      left join (
        select
          edw_vw_my_pos_customer_dim.cntry_cd,
          edw_vw_my_pos_customer_dim.cntry_nm,
          edw_vw_my_pos_customer_dim.cust_cd,
          edw_vw_my_pos_customer_dim.cust_nm,
          edw_vw_my_pos_customer_dim.sold_to,
          edw_vw_my_pos_customer_dim.brnch_cd,
          edw_vw_my_pos_customer_dim.brnch_nm,
          edw_vw_my_pos_customer_dim.brnch_frmt,
          edw_vw_my_pos_customer_dim.brnch_typ,
          edw_vw_my_pos_customer_dim.dept_cd,
          edw_vw_my_pos_customer_dim.dept_nm,
          edw_vw_my_pos_customer_dim.address1,
          edw_vw_my_pos_customer_dim.address2,
          edw_vw_my_pos_customer_dim.region_cd,
          edw_vw_my_pos_customer_dim.region_nm,
          edw_vw_my_pos_customer_dim.prov_cd,
          edw_vw_my_pos_customer_dim.prov_nm,
          edw_vw_my_pos_customer_dim.city_cd,
          edw_vw_my_pos_customer_dim.city_nm,
          edw_vw_my_pos_customer_dim.mncplty_cd,
          edw_vw_my_pos_customer_dim.mncplty_nm
        from edw_vw_my_pos_customer_dim
        where
          (
            cast((
              edw_vw_my_pos_customer_dim.cntry_cd
            ) as text) = cast((
              cast('MY' as varchar)
            ) as text)
          )
      ) as vopcd
        ON (
          (
            (
              upper(
                ltrim(cast((
                  vopcd.sold_to
                ) as text), cast((
                  cast('0' as varchar)
                ) as text))
              ) = upper(trim(substring(cast((
                veposf.cust_cd
              ) as text), 0, 7)))
            )
            AND (
              upper(
                ltrim(cast((
                  vopcd.brnch_cd
                ) as text), cast((
                  cast('0' as varchar)
                ) as text))
              ) = upper(trim(cast((
                veposf.cust_brnch_cd
              ) as text)))
            )
          )
        )
    )
    left join itg_my_customer_dim as imcd
      ON (
        (
          ltrim(cast((
            imcd.cust_id
          ) as text), cast((
            cast('0' as varchar)
          ) as text)) = upper(trim(substring(cast((
            veposf.cust_cd
          ) as text), 0, 7)))
        )
      )
  )
  left join itg_my_material_dim as immd
    ON (
      (
        ltrim(cast((
          immd.item_cd
        ) as text), cast((
          cast('0' as varchar)
        ) as text)) = ltrim(cast((
          veposf.sap_matl_num
        ) as text), cast((
          cast('0' as varchar)
        ) as text))
      )
    )
)
where
  (
    (
      veposf.mnth_id >= veocurd.start_period
    )
    AND (
      veposf.mnth_id <= veocurd.end_period
    )
  )
UNION ALL
select
  cast('SAP BW BILLING' as varchar) as data_src,
  veosf.year as jj_year,
  cast((
    veosf.qrtr
  ) as varchar) as jj_qtr,
  cast((
    veosf.mnth_id
  ) as varchar) as jj_mnth_id,
  veosf.mnth_no as jj_mnth_no,
  cast((
    (
      cast((
        cast((
          veosf.year
        ) as varchar)
      ) as text) || cast((
        cast((
          veosf.jj_yr_wk_no
        ) as varchar)
      ) as text)
    )
  ) as varchar) as jj_year_wk_no,
  'Malaysia' as cntry_nm,
  null  as cust_cd,
  null as cust_brnch_cd,
  null as mt_cust_brnch_nm,
  null as cust_dept_cd,
  null as mt_cust_dept_nm,
  null as region_cd,
  null as region_nm,
  null as item_cd,
  null as mt_item_nm,
  cast((
    ltrim(cast((
      veocd.sap_cust_id
    ) as text), cast((
      cast('0' as varchar)
    ) as text))
  ) as varchar) as sold_to,
  veocd.sap_cust_nm as sold_to_nm,
  'SELLIN' as trade_type,
  imcd.dstrbtr_grp_cd,
  imcd.dstrbtr_grp_nm,
  veocd.sap_state_cd,
  veocd.sap_sls_org,
  veocd.sap_cmp_id,
  veocd.sap_cntry_cd,
  veocd.sap_cntry_nm,
  veocd.sap_addr,
  veocd.sap_region,
  veocd.sap_city,
  veocd.sap_post_cd,
  veocd.sap_chnl_cd,
  veocd.sap_chnl_desc,
  veocd.sap_sls_office_cd,
  veocd.sap_sls_office_desc,
  veocd.sap_sls_grp_cd,
  veocd.sap_sls_grp_desc,
  veocd.sap_curr_cd,
  veocd.gch_region,
  veocd.gch_cluster,
  veocd.gch_subcluster,
  veocd.gch_market,
  veocd.gch_retail_banner,
  cast((
    ltrim(cast((
      veomd.sap_matl_num
    ) as text), cast((
      cast('0' as varchar)
    ) as text))
  ) as varchar) as sku,
  immd.frnchse_desc,
  immd.brnd_desc,
  immd.vrnt_desc,
  immd.putup_desc,
  immd.item_desc2,
  veomd.sap_mat_desc as sku_desc,
  veomd.sap_mat_type_cd,
  veomd.sap_mat_type_desc,
  veomd.sap_base_uom_cd,
  veomd.sap_prchse_uom_cd,
  veomd.sap_prod_sgmt_cd,
  veomd.sap_prod_sgmt_desc,
  veomd.sap_base_prod_cd,
  veomd.sap_base_prod_desc,
  veomd.sap_mega_brnd_cd,
  veomd.sap_mega_brnd_desc,
  veomd.sap_brnd_cd,
  veomd.sap_brnd_desc,
  veomd.sap_vrnt_cd,
  veomd.sap_vrnt_desc,
  veomd.sap_put_up_cd,
  veomd.sap_put_up_desc,
  veomd.sap_grp_frnchse_cd,
  veomd.sap_grp_frnchse_desc,
  veomd.sap_frnchse_cd,
  veomd.sap_frnchse_desc,
  veomd.sap_prod_frnchse_cd,
  veomd.sap_prod_frnchse_desc,
  veomd.sap_prod_mjr_cd,
  veomd.sap_prod_mjr_desc,
  veomd.sap_prod_mnr_cd,
  veomd.sap_prod_mnr_desc,
  veomd.sap_prod_hier_cd,
  veomd.sap_prod_hier_desc,
  veomd.gph_region as global_mat_region,
  veomd.gph_prod_frnchse as global_prod_franchise,
  veomd.gph_prod_brnd as global_prod_brand,
  veomd.gph_prod_vrnt as global_prod_variant,
  veomd.gph_prod_put_up_cd as global_prod_put_up_cd,
  veomd.gph_prod_put_up_desc as global_put_up_desc,
  veomd.gph_prod_sub_brnd as global_prod_sub_brand,
  veomd.gph_prod_needstate as global_prod_need_state,
  veomd.gph_prod_ctgry as global_prod_category,
  veomd.gph_prod_subctgry as global_prod_subcategory,
  veomd.gph_prod_sgmnt as global_prod_segment,
  veomd.gph_prod_subsgmnt as global_prod_subsegment,
  veomd.gph_prod_size as global_prod_size,
  veomd.gph_prod_size_uom as global_prod_size_uom,
  veocurd.from_ccy,
  veocurd.to_ccy,
  veocurd.exch_rate,
  veosf.bill_type,
  sum(veosf.bill_qty_pc) as bill_qty_pc,
  sum((
    veosf.grs_trd_sls * veocurd.exch_rate
  )) as billing_grs_trd_sls,
  sum((
    veosf.subtotal_2 * veocurd.exch_rate
  )) as billing_subtot2,
  sum((
    veosf.subtotal_3 * veocurd.exch_rate
  )) as billing_subtot3,
  sum((
    veosf.subtotal_4 * veocurd.exch_rate
  )) as billing_subtot4,
  sum((
    veosf.net_amt * veocurd.exch_rate
  )) as billing_net_amt,
  sum((
    veosf.est_nts * veocurd.exch_rate
  )) as billing_est_nts,
  sum((
    veosf.invoice_val * veocurd.exch_rate
  )) as billing_invoice_val,
  sum((
    veosf.gross_val * veocurd.exch_rate
  )) as billing_gross_val,
  0.0 as pos_qty,
  0.0 as pos_gts,
  0.0 as pos_item_prc,
  0.0 as pos_tax,
  0.0 as pos_nts,
  0 as conv_factor,
  0 as jj_pos_qty_pc,
  0 as jj_pos_item_prc_per_pc,
  0 as jj_pos_gts,
  0 as jj_pos_vat_amt,
  0 as jj_pos_nts,
  immd.npi_ind as is_npi,
  immd.npi_strt_period as npi_str_period,
  null as npi_end_period,
  null as is_reg,
  immd.promo_reg_ind as is_promo,
  null as promo_strt_period,
  null as promo_end_period,
  null as is_mcl,
  immd.hero_ind as is_hero
from (
  select
    d.cntry_key,
    d.cntry_nm,
    d.rate_type,
    d.from_ccy,
    d.to_ccy,
    d.valid_date,
    d.jj_year,
    d.start_period,
    case
      when (
        d.end_mnth_id = b.max_period
      )
      then cast((
        cast('209912' as varchar)
      ) as text)
      else d.end_mnth_id
    end as end_period,
    d.exch_rate
  from (
    select
      a.cntry_key,
      a.cntry_nm,
      a.rate_type,
      a.from_ccy,
      a.to_ccy,
      a.valid_date,
      a.jj_year,
      min(cast((
        a.jj_mnth_id
      ) as text)) as start_period,
      max(cast((
        a.jj_mnth_id
      ) as text)) as end_mnth_id,
      a.exch_rate
    from edw_vw_my_curr_dim as a
    where
      (
        cast((
          a.cntry_key
        ) as text) = cast((
          cast('MY' as varchar)
        ) as text)
      )
    group by
      a.cntry_key,
      a.cntry_nm,
      a.rate_type,
      a.from_ccy,
      a.to_ccy,
      a.valid_date,
      a.jj_year,
      a.exch_rate
  ) as d, (
    select
      max(cast((
        a.jj_mnth_id
      ) as text)) as max_period
    from edw_vw_my_curr_dim as a
    where
      (
        cast((
          a.cntry_key
        ) as text) = cast((
          cast('MY' as varchar)
        ) as text)
      )
  ) as b
) as veocurd, (
  (
    (
      (
        (
          (
            select
              veotd.year,
              veotd.qrtr_no,
              veotd.qrtr,
              veotd.mnth_id,
              veotd.mnth_no,
              veotd.mnth_nm,
              veotd.jj_yr_wk_no,
              t1.matl_num as sap_matl_num,
              cast(null as varchar) as dstrbtr_matl_num,
              t1.sold_to as sap_soldto_code,
              t1.bill_type,
              sum(t1.bill_qty_pc) as bill_qty_pc,
              sum((
                t1.grs_trd_sls * abs(t1.exchg_rate)
              )) as grs_trd_sls,
              sum((
                t1.subtotal_2 * abs(t1.exchg_rate)
              )) as subtotal_2,
              sum((
                t1.subtotal_3 * abs(t1.exchg_rate)
              )) as subtotal_3,
              sum((
                t1.subtotal_4 * abs(t1.exchg_rate)
              )) as subtotal_4,
              sum((
                t1.net_amt * abs(t1.exchg_rate)
              )) as net_amt,
              sum((
                t1.est_nts * abs(t1.exchg_rate)
              )) as est_nts,
              sum((
                t1.net_val * abs(t1.exchg_rate)
              )) as invoice_val,
              sum((
                t1.gross_val * abs(t1.exchg_rate)
              )) as gross_val
            from (
              select
                edw_vw_my_billing_fact.cntry_key,
                edw_vw_my_billing_fact.cntry_nm,
                edw_vw_my_billing_fact.bill_dt,
                edw_vw_my_billing_fact.bill_num,
                edw_vw_my_billing_fact.bill_item,
                edw_vw_my_billing_fact.bill_type,
                edw_vw_my_billing_fact.sls_doc_num,
                edw_vw_my_billing_fact.sls_doc_item,
                edw_vw_my_billing_fact.doc_curr,
                edw_vw_my_billing_fact.sd_doc_catgy,
                edw_vw_my_billing_fact.sold_to,
                edw_vw_my_billing_fact.matl_num,
                edw_vw_my_billing_fact.sls_org,
                edw_vw_my_billing_fact.exchg_rate,
                edw_vw_my_billing_fact.bill_qty_pc,
                edw_vw_my_billing_fact.grs_trd_sls,
                edw_vw_my_billing_fact.subtotal_2,
                edw_vw_my_billing_fact.subtotal_3,
                edw_vw_my_billing_fact.subtotal_4,
                edw_vw_my_billing_fact.net_amt,
                edw_vw_my_billing_fact.est_nts,
                edw_vw_my_billing_fact.net_val,
                edw_vw_my_billing_fact.gross_val
              from edw_vw_my_billing_fact
              where
                (
                  (
                    (
                      edw_vw_my_billing_fact.cntry_key = cast((
                        cast('MY' as varchar)
                      ) as text)
                    )
                    AND (
                      (
                        cast((
                          edw_vw_my_billing_fact.bill_type
                        ) as text) = cast((
                          cast('ZF2M' as varchar)
                        ) as text)
                      )
                      OR (
                        cast((
                          edw_vw_my_billing_fact.bill_type
                        ) as text) = cast((
                          cast('ZG2M' as varchar)
                        ) as text)
                      )
                    )
                  )
                  AND (
                    cast((
                      edw_vw_my_billing_fact.sold_to
                    ) as varchar(20)) IN (
                      select distinct
                        b.cust_id
                      from itg_my_pos_cust_mstr as a, itg_my_customer_dim as b
                      where
                        (
                          cast((
                            b.dstrbtr_grp_cd
                          ) as text) = cast((
                            a.cust_id
                          ) as text)
                        )
                    )
                  )
                )
            ) as t1, (
              select distinct
                edw_vw_os_time_dim.year,
                edw_vw_os_time_dim.qrtr_no,
                edw_vw_os_time_dim.qrtr,
                edw_vw_os_time_dim.mnth_id,
                edw_vw_os_time_dim.mnth_no,
                edw_vw_os_time_dim.mnth_desc as mnth_nm,
                edw_vw_os_time_dim.wk as jj_yr_wk_no,
                edw_vw_os_time_dim.cal_date,
                edw_vw_os_time_dim.cal_date_id
              from edw_vw_os_time_dim
            ) as veotd
            where
              (
                  veotd.cal_date
                 = 
                  t1.bill_dt
                
              )
            group by
              veotd.year,
              veotd.qrtr_no,
              veotd.qrtr,
              veotd.mnth_id,
              veotd.mnth_no,
              veotd.mnth_nm,
              veotd.jj_yr_wk_no,
              t1.matl_num,
              t1.sold_to,
              t1.bill_type
          ) as veosf
          left join (
            select
              edw_vw_my_customer_dim.sap_cust_id,
              edw_vw_my_customer_dim.sap_cust_nm,
              edw_vw_my_customer_dim.sap_sls_org,
              edw_vw_my_customer_dim.sap_cmp_id,
              edw_vw_my_customer_dim.sap_cntry_cd,
              edw_vw_my_customer_dim.sap_cntry_nm,
              edw_vw_my_customer_dim.sap_addr,
              edw_vw_my_customer_dim.sap_region,
              edw_vw_my_customer_dim.sap_state_cd,
              edw_vw_my_customer_dim.sap_city,
              edw_vw_my_customer_dim.sap_post_cd,
              edw_vw_my_customer_dim.sap_chnl_cd,
              edw_vw_my_customer_dim.sap_chnl_desc,
              edw_vw_my_customer_dim.sap_sls_office_cd,
              edw_vw_my_customer_dim.sap_sls_office_desc,
              edw_vw_my_customer_dim.sap_sls_grp_cd,
              edw_vw_my_customer_dim.sap_sls_grp_desc,
              edw_vw_my_customer_dim.sap_curr_cd,
              edw_vw_my_customer_dim.sap_prnt_cust_key,
              edw_vw_my_customer_dim.sap_prnt_cust_desc,
              edw_vw_my_customer_dim.sap_cust_chnl_key,
              edw_vw_my_customer_dim.sap_cust_chnl_desc,
              edw_vw_my_customer_dim.sap_cust_sub_chnl_key,
              edw_vw_my_customer_dim.sap_sub_chnl_desc,
              edw_vw_my_customer_dim.sap_go_to_mdl_key,
              edw_vw_my_customer_dim.sap_go_to_mdl_desc,
              edw_vw_my_customer_dim.sap_bnr_key,
              edw_vw_my_customer_dim.sap_bnr_desc,
              edw_vw_my_customer_dim.sap_bnr_frmt_key,
              edw_vw_my_customer_dim.sap_bnr_frmt_desc,
              edw_vw_my_customer_dim.retail_env,
              edw_vw_my_customer_dim.gch_region,
              edw_vw_my_customer_dim.gch_cluster,
              edw_vw_my_customer_dim.gch_subcluster,
              edw_vw_my_customer_dim.gch_market,
              edw_vw_my_customer_dim.gch_retail_banner
            from edw_vw_my_customer_dim
            where
              (
                cast((
                  edw_vw_my_customer_dim.sap_cntry_cd
                ) as text) = cast((
                  cast('MY' as varchar)
                ) as text)
              )
          ) as veocd
            ON (
              (
                ltrim(cast((
                  veocd.sap_cust_id
                ) as text), cast((
                  cast('0' as varchar)
                ) as text)) = ltrim(veosf.sap_soldto_code, cast((
                  cast('0' as varchar)
                ) as text))
              )
            )
        )
        left join (
          select
            edw_vw_my_material_dim.cntry_key,
            edw_vw_my_material_dim.sap_matl_num,
            edw_vw_my_material_dim.sap_mat_desc,
            edw_vw_my_material_dim.ean_num,
            edw_vw_my_material_dim.sap_mat_type_cd,
            edw_vw_my_material_dim.sap_mat_type_desc,
            edw_vw_my_material_dim.sap_base_uom_cd,
            edw_vw_my_material_dim.sap_prchse_uom_cd,
            edw_vw_my_material_dim.sap_prod_sgmt_cd,
            edw_vw_my_material_dim.sap_prod_sgmt_desc,
            edw_vw_my_material_dim.sap_base_prod_cd,
            edw_vw_my_material_dim.sap_base_prod_desc,
            edw_vw_my_material_dim.sap_mega_brnd_cd,
            edw_vw_my_material_dim.sap_mega_brnd_desc,
            edw_vw_my_material_dim.sap_brnd_cd,
            edw_vw_my_material_dim.sap_brnd_desc,
            edw_vw_my_material_dim.sap_vrnt_cd,
            edw_vw_my_material_dim.sap_vrnt_desc,
            edw_vw_my_material_dim.sap_put_up_cd,
            edw_vw_my_material_dim.sap_put_up_desc,
            edw_vw_my_material_dim.sap_grp_frnchse_cd,
            edw_vw_my_material_dim.sap_grp_frnchse_desc,
            edw_vw_my_material_dim.sap_frnchse_cd,
            edw_vw_my_material_dim.sap_frnchse_desc,
            edw_vw_my_material_dim.sap_prod_frnchse_cd,
            edw_vw_my_material_dim.sap_prod_frnchse_desc,
            edw_vw_my_material_dim.sap_prod_mjr_cd,
            edw_vw_my_material_dim.sap_prod_mjr_desc,
            edw_vw_my_material_dim.sap_prod_mnr_cd,
            edw_vw_my_material_dim.sap_prod_mnr_desc,
            edw_vw_my_material_dim.sap_prod_hier_cd,
            edw_vw_my_material_dim.sap_prod_hier_desc,
            edw_vw_my_material_dim.gph_region,
            edw_vw_my_material_dim.gph_reg_frnchse,
            edw_vw_my_material_dim.gph_reg_frnchse_grp,
            edw_vw_my_material_dim.gph_prod_frnchse,
            edw_vw_my_material_dim.gph_prod_brnd,
            edw_vw_my_material_dim.gph_prod_sub_brnd,
            edw_vw_my_material_dim.gph_prod_vrnt,
            edw_vw_my_material_dim.gph_prod_needstate,
            edw_vw_my_material_dim.gph_prod_ctgry,
            edw_vw_my_material_dim.gph_prod_subctgry,
            edw_vw_my_material_dim.gph_prod_sgmnt,
            edw_vw_my_material_dim.gph_prod_subsgmnt,
            edw_vw_my_material_dim.gph_prod_put_up_cd,
            edw_vw_my_material_dim.gph_prod_put_up_desc,
            edw_vw_my_material_dim.gph_prod_size,
            edw_vw_my_material_dim.gph_prod_size_uom,
            edw_vw_my_material_dim.launch_dt,
            edw_vw_my_material_dim.qty_shipper_pc,
            edw_vw_my_material_dim.prft_ctr,
            edw_vw_my_material_dim.shlf_life
          from edw_vw_my_material_dim
          where
            (
              cast((
                edw_vw_my_material_dim.cntry_key
              ) as text) = cast((
                cast('MY' as varchar)
              ) as text)
            )
        ) as veomd
          ON (
            (
              ltrim(cast((
                veomd.sap_matl_num
              ) as text), cast((
                cast('0' as varchar)
              ) as text)) = ltrim(veosf.sap_matl_num, cast((
                cast('0' as varchar)
              ) as text))
            )
          )
      )
      left join itg_my_dstrbtrr_dim as imdd
        ON (
          (
            ltrim(cast((
              imdd.cust_id
            ) as text), cast((
              cast('0' as varchar)
            ) as text)) = ltrim(veosf.sap_soldto_code, cast((
              cast('0' as varchar)
            ) as text))
          )
        )
    )
    left join itg_my_material_dim as immd
      ON (
        (
          ltrim(cast((
            immd.item_cd
          ) as text), cast((
            cast('0' as varchar)
          ) as text)) = ltrim(veosf.sap_matl_num, cast((
            cast('0' as varchar)
          ) as text))
        )
      )
  )
  left join itg_my_customer_dim as imcd
    ON (
      (
        ltrim(cast((
          imcd.cust_id
        ) as text), cast((
          cast('0' as varchar)
        ) as text)) = ltrim(veosf.sap_soldto_code, cast((
          cast('0' as varchar)
        ) as text))
      )
    )
)
where
  (
    (
      veosf.mnth_id >= veocurd.start_period
    )
    AND (
      veosf.mnth_id <= veocurd.end_period
    )
  )
group by
  1,
  veosf.year,
  veosf.qrtr,
  veosf.mnth_id,
  veosf.mnth_no,
  veosf.jj_yr_wk_no,
  ltrim(cast((
    veocd.sap_cust_id
  ) as text), cast((
    cast('0' as varchar)
  ) as text)),
  veocd.sap_cust_nm,
  imcd.dstrbtr_grp_cd,
  imcd.dstrbtr_grp_nm,
  veocd.sap_state_cd,
  veocd.sap_sls_org,
  veocd.sap_cmp_id,
  veocd.sap_cntry_cd,
  veocd.sap_cntry_nm,
  veocd.sap_addr,
  veocd.sap_region,
  veocd.sap_city,
  veocd.sap_post_cd,
  veocd.sap_chnl_cd,
  veocd.sap_chnl_desc,
  veocd.sap_sls_office_cd,
  veocd.sap_sls_office_desc,
  veocd.sap_sls_grp_cd,
  veocd.sap_sls_grp_desc,
  veocd.sap_curr_cd,
  veocd.gch_region,
  veocd.gch_cluster,
  veocd.gch_subcluster,
  veocd.gch_market,
  veocd.gch_retail_banner,
  ltrim(cast((
    veomd.sap_matl_num
  ) as text), cast((
    cast('0' as varchar)
  ) as text)),
  immd.frnchse_desc,
  immd.brnd_desc,
  immd.vrnt_desc,
  immd.putup_desc,
  immd.item_desc2,
  veomd.sap_mat_desc,
  veomd.sap_mat_type_cd,
  veomd.sap_mat_type_desc,
  veomd.sap_base_uom_cd,
  veomd.sap_prchse_uom_cd,
  veomd.sap_prod_sgmt_cd,
  veomd.sap_prod_sgmt_desc,
  veomd.sap_base_prod_cd,
  veomd.sap_base_prod_desc,
  veomd.sap_mega_brnd_cd,
  veomd.sap_mega_brnd_desc,
  veomd.sap_brnd_cd,
  veomd.sap_brnd_desc,
  veomd.sap_vrnt_cd,
  veomd.sap_vrnt_desc,
  veomd.sap_put_up_cd,
  veomd.sap_put_up_desc,
  veomd.sap_grp_frnchse_cd,
  veomd.sap_grp_frnchse_desc,
  veomd.sap_frnchse_cd,
  veomd.sap_frnchse_desc,
  veomd.sap_prod_frnchse_cd,
  veomd.sap_prod_frnchse_desc,
  veomd.sap_prod_mjr_cd,
  veomd.sap_prod_mjr_desc,
  veomd.sap_prod_mnr_cd,
  veomd.sap_prod_mnr_desc,
  veomd.sap_prod_hier_cd,
  veomd.sap_prod_hier_desc,
  veomd.gph_region,
  veomd.gph_prod_frnchse,
  veomd.gph_prod_brnd,
  veomd.gph_prod_vrnt,
  veomd.gph_prod_put_up_cd,
  veomd.gph_prod_put_up_desc,
  veomd.gph_prod_sub_brnd,
  veomd.gph_prod_needstate,
  veomd.gph_prod_ctgry,
  veomd.gph_prod_subctgry,
  veomd.gph_prod_sgmnt,
  veomd.gph_prod_subsgmnt,
  veomd.gph_prod_size,
  veomd.gph_prod_size_uom,
  veocurd.from_ccy,
  veocurd.to_ccy,
  veocurd.exch_rate,
  veosf.bill_type,
  immd.npi_ind,
  immd.npi_strt_period,
  immd.promo_reg_ind,
  immd.hero_ind
)
select * from final 