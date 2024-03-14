with edw_vw_my_sellout_sales_fact as (
  select * from {{ ref('mysedw_integration__edw_vw_my_sellout_sales_fact') }}
),
edw_vw_my_dstrbtr_customer_dim as (
  select * from {{ ref('mysedw_integration__edw_vw_my_dstrbtr_customer_dim') }}
),
edw_vw_my_curr_dim as (
  select * from {{ ref('mysedw_integration__edw_vw_my_curr_dim') }}
),
edw_vw_os_time_dim as (
  select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_vw_my_customer_dim as (
  select * from {{ ref('mysedw_integration__edw_vw_my_customer_dim') }}
),
edw_vw_my_material_dim as (
  select * from {{ ref('mysedw_integration__edw_vw_my_material_dim') }}
),
edw_vw_my_listprice as (
  select * from {{ ref('mysedw_integration__edw_vw_my_listprice') }}
),
edw_vw_my_sellout_inventory_fact as (
  select * from {{ ref('mysedw_integration__edw_vw_my_sellout_inventory_fact') }}
),
edw_vw_my_billing_fact as (
  select * from {{ ref('mysedw_integration__edw_vw_my_billing_fact') }} 
),
itg_my_gt_outlet_exclusion as(
  select * from {{ source('mysitg_integration', 'itg_my_gt_outlet_exclusion') }}
),
itg_my_trgts as (
  select * from {{ ref('mysitg_integration__itg_my_trgts') }}
  ),
itg_my_customer_dim as (
  select * from {{ ref('mysitg_integration__itg_my_customer_dim') }}
),
itg_my_sellout_sales_fact as (
  select * from {{ ref('mysitg_integration__itg_my_sellout_sales_fact') }}
),
itg_my_dstrbtrr_dim as (
  select * from {{ ref('mysitg_integration__itg_my_dstrbtrr_dim') }}
),
itg_my_material_dim as (
  select * from {{ ref('mysitg_integration__itg_my_material_dim') }}
 ),
itg_my_gt_outlet_exclusion as (
  select * from {{ source('mysitg_integration', 'itg_my_gt_outlet_exclusion') }}
),
itg_my_in_transit as (
  select * from {{ ref('mysitg_integration__itg_my_in_transit') }}
),
final as (
(
  (
    select
      'GT Sellout' as data_src,
      veosf.year,
      veosf.qrtr_no,
      veosf.mnth_id,
      veosf.mnth_no,
      veosf.mnth_nm,
      cast((
        substring(
          replace(
            cast((
              cast((
                ym.bill_date
              ) as varchar)
            ) as text),
            cast((
              cast('-' as varchar)
            ) as text),
            cast((
              cast('' as varchar)
            ) as text)
          ),
          0,
          7
        )
      ) as varchar) as max_yearmo,
      'Malaysia' as cntry_nm,
      veosf.dstrbtr_grp_cd,
      imcd.dstrbtr_grp_nm,
      veosf.cust_cd as dstrbtr_cust_cd,
      veosf.cust_nm as dstrbtr_cust_nm,
      cast((
        ltrim(cast((
          imdd.cust_id
        ) as text), cast((
          cast('0' as varchar)
        ) as text))
      ) as varchar) as sap_soldto_code,
      imdd.cust_nm as sap_soldto_nm,
      imdd.lvl1 as dstrbtr_lvl1,
      imdd.lvl2 as dstrbtr_lvl2,
      imdd.lvl3 as dstrbtr_lvl3,
      imdd.lvl4 as dstrbtr_lvl4,
      imdd.lvl5 as dstrbtr_lvl5,
      veosf.region_nm,
      veosf.town_nm,
      veosf.slsmn_cd,
      veosf.chnl_desc,
      veosf.sub_chnl_desc,
      veosf.chnl_attr1_desc,
      veosf.chnl_attr2_desc,
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
      veosf.dstrbtr_matl_num,
      veosf.bar_cd,
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
      veosf.wh_id,
      veosf.doc_type,
      veosf.doc_type_desc,
      veosf.bill_date,
      veosf.bill_doc,
      (
        veosf.base_sls * veocurd.exch_rate
      ) as base_sls,
      veosf.sls_qty,
      veosf.ret_qty,
      veosf.uom,
      veosf.sls_qty_pc,
      veosf.ret_qty_pc,
      0 as in_transit_qty,
      lp.rate as mat_list_price,
      (
        veosf.grs_trd_sls * veocurd.exch_rate
      ) as grs_trd_sls,
      (
        veosf.ret_val * veocurd.exch_rate
      ) as ret_val,
      (
        veosf.trd_discnt * veocurd.exch_rate
      ) as trd_discnt,
      (
        veosf.trd_sls * veocurd.exch_rate
      ) as trd_sls,
      (
        veosf.net_trd_sls * veocurd.exch_rate
      ) as net_trd_sls,
      (
        veosf.jj_grs_trd_sls * veocurd.exch_rate
      ) as jj_grs_trd_sls,
      (
        veosf.jj_ret_val * veocurd.exch_rate
      ) as jj_ret_val,
      (
        veosf.jj_trd_sls * veocurd.exch_rate
      ) as jj_trd_sls,
      (
        veosf.jj_net_trd_sls * veocurd.exch_rate
      ) as jj_net_trd_sls,
      0 as in_transit_val,
      0 as trgt_val,
      null as inv_dt,
      null as warehse_cd,
      0 as end_stock_qty,
      0 as end_stock_val_raw,
      0 as end_stock_val,
      immd.npi_ind as is_npi,
      immd.npi_strt_period as npi_str_period,
      null as npi_end_period,
      null as is_reg,
      immd.promo_reg_ind as is_promo,
      null as promo_strt_period,
      null as promo_end_period,
      null as is_mcl,
      immd.hero_ind as is_hero,
      (
        veosf.contribution * veocurd.exch_rate
      ) as contribution
    from (
      select
        max(edw_vw_my_sellout_sales_fact.bill_date) as bill_date
      from edw_vw_my_sellout_sales_fact
      where
        (
          cast((
            edw_vw_my_sellout_sales_fact.cntry_cd
          ) as text) = cast((
            cast('MY' as varchar)
          ) as text)
        )
    ) as ym, (
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
                    a.year,
                    a.qrtr_no,
                    a.mnth_id,
                    a.mnth_no,
                    a.mnth_nm,
                    a.dstrbtr_grp_cd,
                    a.dstrbtr_soldto_code,
                    a.cust_cd,
                    a.slsmn_cd,
                    a.cust_nm,
                    a.sap_soldto_code,
                    a.sap_matl_num,
                    cast((
                      UPPER(trim(cast((
                        a.dstrbtr_matl_num
                      ) as text)))
                    ) as varchar) as dstrbtr_matl_num,
                    a.bar_cd,
                    a.region_nm,
                    a.town_nm,
                    a.chnl_desc,
                    a.sub_chnl_desc,
                    a.chnl_attr1_desc,
                    a.chnl_attr2_desc,
                    a.wh_id,
                    a.doc_type,
                    a.doc_type_desc,
                    a.base_sls,
                    a.bill_date,
                    a.bill_doc,
                    a.sls_qty,
                    a.ret_qty,
                    a.uom,
                    a.sls_qty_pc,
                    a.ret_qty_pc,
                    a.grs_trd_sls,
                    a.ret_val,
                    a.trd_discnt,
                    a.trd_sls,
                    a.net_trd_sls,
                    a.jj_grs_trd_sls,
                    a.jj_ret_val,
                    a.jj_trd_sls,
                    a.jj_net_trd_sls,
                    0 as contribution
                  from (
                    select
                      veotd.year,
                      veotd.qrtr_no,
                      veotd.mnth_id,
                      veotd.mnth_no,
                      veotd.mnth_nm,
                      t1.dstrbtr_grp_cd,
                      t1.dstrbtr_soldto_code,
                      t1.cust_cd,
                      t1.slsmn_cd,
                      t1.bill_date,
                      t1.bill_doc,
                      evodcd.cust_nm,
                      evodcd.sap_soldto_code,
                      t1.sap_matl_num,
                      t1.dstrbtr_matl_num,
                      t1.bar_cd,
                      evodcd.region_nm,
                      evodcd.town_nm,
                      evodcd.chnl_desc,
                      evodcd.sub_chnl_desc,
                      evodcd.chnl_attr1_desc,
                      evodcd.chnl_attr2_desc,
                      t1.wh_id,
                      t1.doc_type,
                      t1.doc_type_desc,
                      t1.base_sls,
                      t1.sls_qty,
                      t1.ret_qty,
                      t1.uom,
                      t1.sls_qty_pc,
                      t1.ret_qty_pc,
                      t1.grs_trd_sls,
                      t1.ret_val,
                      t1.trd_discnt,
                      t1.trd_sls,
                      t1.net_trd_sls,
                      t1.jj_grs_trd_sls,
                      t1.jj_ret_val,
                      t1.jj_trd_sls,
                      t1.jj_net_trd_sls
                    from (
                      select distinct
                        edw_vw_os_time_dim.cal_year as year,
                        edw_vw_os_time_dim.cal_qrtr_no as qrtr_no,
                        edw_vw_os_time_dim.cal_mnth_id as mnth_id,
                        edw_vw_os_time_dim.cal_mnth_no as mnth_no,
                        edw_vw_os_time_dim.cal_mnth_nm as mnth_nm,
                        edw_vw_os_time_dim.cal_date,
                        edw_vw_os_time_dim.cal_date_id
                      from edw_vw_os_time_dim
                    ) as veotd, (
                      edw_vw_my_sellout_sales_fact as t1
                        left join (
                          select
                            edw_vw_my_dstrbtr_customer_dim.cntry_cd,
                            edw_vw_my_dstrbtr_customer_dim.cntry_nm,
                            edw_vw_my_dstrbtr_customer_dim.dstrbtr_grp_cd,
                            edw_vw_my_dstrbtr_customer_dim.dstrbtr_soldto_code,
                            edw_vw_my_dstrbtr_customer_dim.sap_soldto_code,
                            edw_vw_my_dstrbtr_customer_dim.cust_cd,
                            edw_vw_my_dstrbtr_customer_dim.cust_nm,
                            edw_vw_my_dstrbtr_customer_dim.alt_cust_cd,
                            edw_vw_my_dstrbtr_customer_dim.alt_cust_nm,
                            edw_vw_my_dstrbtr_customer_dim.addr,
                            edw_vw_my_dstrbtr_customer_dim.area_cd,
                            edw_vw_my_dstrbtr_customer_dim.area_nm,
                            edw_vw_my_dstrbtr_customer_dim.state_cd,
                            edw_vw_my_dstrbtr_customer_dim.state_nm,
                            edw_vw_my_dstrbtr_customer_dim.region_cd,
                            edw_vw_my_dstrbtr_customer_dim.region_nm,
                            edw_vw_my_dstrbtr_customer_dim.prov_cd,
                            edw_vw_my_dstrbtr_customer_dim.prov_nm,
                            edw_vw_my_dstrbtr_customer_dim.town_cd,
                            edw_vw_my_dstrbtr_customer_dim.town_nm,
                            edw_vw_my_dstrbtr_customer_dim.city_cd,
                            edw_vw_my_dstrbtr_customer_dim.city_nm,
                            edw_vw_my_dstrbtr_customer_dim.post_cd,
                            edw_vw_my_dstrbtr_customer_dim.post_nm,
                            edw_vw_my_dstrbtr_customer_dim.slsmn_cd,
                            edw_vw_my_dstrbtr_customer_dim.slsmn_nm,
                            edw_vw_my_dstrbtr_customer_dim.chnl_cd,
                            edw_vw_my_dstrbtr_customer_dim.chnl_desc,
                            edw_vw_my_dstrbtr_customer_dim.sub_chnl_cd,
                            edw_vw_my_dstrbtr_customer_dim.sub_chnl_desc,
                            edw_vw_my_dstrbtr_customer_dim.chnl_attr1_cd,
                            edw_vw_my_dstrbtr_customer_dim.chnl_attr1_desc,
                            edw_vw_my_dstrbtr_customer_dim.chnl_attr2_cd,
                            edw_vw_my_dstrbtr_customer_dim.chnl_attr2_desc,
                            edw_vw_my_dstrbtr_customer_dim.outlet_type_cd,
                            edw_vw_my_dstrbtr_customer_dim.outlet_type_desc,
                            edw_vw_my_dstrbtr_customer_dim.cust_grp_cd,
                            edw_vw_my_dstrbtr_customer_dim.cust_grp_desc,
                            edw_vw_my_dstrbtr_customer_dim.cust_grp_attr1_cd,
                            edw_vw_my_dstrbtr_customer_dim.cust_grp_attr1_desc,
                            edw_vw_my_dstrbtr_customer_dim.cust_grp_attr2_cd,
                            edw_vw_my_dstrbtr_customer_dim.cust_grp_attr2_desc,
                            edw_vw_my_dstrbtr_customer_dim.sls_dstrct_cd,
                            edw_vw_my_dstrbtr_customer_dim.sls_dstrct_nm,
                            edw_vw_my_dstrbtr_customer_dim.sls_office_cd,
                            edw_vw_my_dstrbtr_customer_dim.sls_office_desc,
                            edw_vw_my_dstrbtr_customer_dim.sls_grp_cd,
                            edw_vw_my_dstrbtr_customer_dim.sls_grp_desc,
                            edw_vw_my_dstrbtr_customer_dim.STATUS
                          from edw_vw_my_dstrbtr_customer_dim
                          where
                            (
                              cast((
                                edw_vw_my_dstrbtr_customer_dim.cntry_cd
                              ) as text) = cast((
                                cast('MY' as varchar)
                              ) as text)
                            )
                        ) as evodcd
                          on (
                            (
                              (
                                (
                                  ltrim(cast((
                                    evodcd.cust_cd
                                  ) as text), cast((
                                    cast('0' as varchar)
                                  ) as text)) = ltrim(cast((
                                    t1.cust_cd
                                  ) as text), cast((
                                    cast('0' as varchar)
                                  ) as text))
                                )
                                and (
                                  ltrim(
                                    cast((
                                      evodcd.dstrbtr_grp_cd
                                    ) as text),
                                    cast((
                                      cast('0' as varchar)
                                    ) as text)
                                  ) = ltrim(cast((
                                    t1.dstrbtr_grp_cd
                                  ) as text), cast((
                                    cast('0' as varchar)
                                  ) as text))
                                )
                              )
                              and (
                                ltrim(
                                  cast((
                                    evodcd.sap_soldto_code
                                  ) as text),
                                  cast((
                                    cast('0' as varchar)
                                  ) as text)
                                ) = ltrim(
                                  cast((
                                    t1.dstrbtr_soldto_code
                                  ) as text),
                                  cast((
                                    cast('0' as varchar)
                                  ) as text)
                                )
                              )
                            )
                          )
                    )
                    where
                      (
                        (
                          (
                           
                              veotd.cal_date
                             = t1.bill_date
                          )
                          and (
                            cast((
                              t1.cntry_cd
                            ) as text) = cast((
                              cast('MY' as varchar)
                            ) as text)
                          )
                        )
                        and (
                          not (
                            (
                              coalesce(
                                ltrim(
                                  cast((
                                    t1.dstrbtr_soldto_code
                                  ) as text),
                                  cast((
                                    cast('0' as varchar)
                                  ) as text)
                                ),
                                cast((
                                  cast('0' as varchar)
                                ) as text)
                              ) || coalesce(trim(cast((
                                t1.cust_cd
                              ) as text)), cast((
                                cast('0' as varchar)
                              ) as text))
                            ) in (
                              select distinct
                                (
                                  cast((
                                    coalesce(itg_my_gt_outlet_exclusion.dstrbtr_cd, cast('0' as varchar))
                                  ) as text) || cast((
                                    coalesce(itg_my_gt_outlet_exclusion.outlet_cd, cast('0' as varchar))
                                  ) as text)
                                )
                              from itg_my_gt_outlet_exclusion
                            )
                          )
                        )
                      )
                  ) as a
                  union all
                  select
                    trgt.year,
                    trgt.qrtr_no,
                    trgt.trgt_period as mnth_id,
                    trgt.mnth_no,
                    trgt.mnth_nm,
                    cast(null as varchar) as dstrbtr_grp_cd,
                    trgt.dstrbtr_id as dstrbtr_soldto_code,
                    trgt.cust_cd,
                    cast(null as varchar) as slsmn_cd,
                    evodcd.cust_nm,
                    trgt.dstrbtr_id as sap_soldto_code,
                    trgt.sap_matl_num,
                    cast((
                      UPPER(trim(cast((
                        trgt.dstrbtr_matl_num
                      ) as text)))
                    ) as varchar) as dstrbtr_matl_num,
                    trgt.bar_cd,
                    evodcd.region_nm,
                    evodcd.town_nm,
                    evodcd.chnl_desc,
                    evodcd.sub_chnl_desc,
                    evodcd.chnl_attr1_desc,
                    evodcd.chnl_attr2_desc,
                    cast(null as varchar) as wh_id,
                    cast(null as varchar) as doc_type,
                    cast(null as varchar) as doc_type_desc,
                    0 as base_sls,
                    cast(null as timestampntz) as bill_date,
                    cast(null as varchar) as bill_doc,
                    0 as sls_qty,
                    0 as ret_qty,
                    cast(null as varchar) as uom,
                    0 as sls_qty_pc,
                    0 as ret_qty_pc,
                    0 as grs_trd_sls,
                    0 as ret_val,
                    0 as trd_discnt,
                    0 as trd_sls,
                    0 as net_trd_sls,
                    0 as jj_grs_trd_sls,
                    0 as jj_ret_val,
                    0 as jj_trd_sls,
                    0 as jj_net_trd_sls,
                    trgt.contribution
                  from (
                    (
                      select
                        derived_table1.year,
                        derived_table1.qrtr_no,
                        derived_table1.mnth_no,
                        derived_table1.mnth_nm,
                        derived_table1.mnth_id,
                        derived_table1.trgt_period,
                        derived_table1.dstrbtr_id,
                        derived_table1.sap_matl_num,
                        derived_table1.dstrbtr_matl_num,
                        derived_table1.bar_cd,
                        derived_table1.cust_cd,
                        sum(derived_table1.contribution) as contribution
                      from (
                        select
                          t1.year,
                          t1.qrtr_no,
                          t1.mnth_no,
                          t1.mnth_nm,
                          t1.mnth_id,
                          t1.trgt_period,
                          t1.dstrbtr_id,
                          t1.sap_matl_num,
                          t1.dstrbtr_matl_num,
                          t1.bar_cd,
                          t1.cust_cd,
                          (
                            trgt.trgt_val * (
                              t1.base_sls / t1.total_sales
                            )
                          ) as contribution
                        from (
                          select
                            cast((
                              substring(
                                to_char(
                                  cast(dateadd(
                                    month,
                                    cast((
                                      12
                                    ) as bigint),
                                    cast(cast((
                                      to_date(
                                        cast((
                                          cast((
                                            veotd.mnth_id
                                          ) as varchar)
                                        ) as text),
                                        cast((
                                          cast('YYYYMM' as varchar)
                                        ) as text)
                                      )
                                    ) as timestampntz) as timestampntz)
                                  ) as timestampntz),
                                  cast((
                                    cast('YYYYMM' as varchar)
                                  ) as text)
                                ),
                                0,
                                5
                              )
                            ) as int) as year,
                            veotd.qrtr_no,
                            veotd.mnth_no,
                            veotd.mnth_nm,
                            veotd.mnth_id,
                            cast((
                              to_char(
                                cast(dateadd(
                                  month,
                                  cast((
                                    12
                                  ) as bigint),
                                  cast(cast((
                                    to_date(
                                      cast((
                                        cast((
                                          veotd.mnth_id
                                        ) as varchar)
                                      ) as text),
                                      cast((
                                        cast('YYYYMM' as varchar)
                                      ) as text)
                                    )
                                  ) as timestampntz) as timestampntz)
                                ) as timestampntz),
                                cast((
                                  cast('YYYYMM' as varchar)
                                ) as text)
                              )
                            ) as int) as trgt_period,
                            a.dstrbtr_id,
                            a.dstrbtr_prod_cd as dstrbtr_matl_num,
                            a.sap_matl_num,
                            a.ean_num as bar_cd,
                            a.cust_cd,
                            a.total_amt_bfr_tax as base_sls,
                            sum(a.total_amt_bfr_tax) OVER (PARTITION BY veotd.mnth_id, a.dstrbtr_id order by null ROWS BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as total_sales
                          from (
                            select
                              itg_my_sellout_sales_fact.dstrbtr_id,
                              itg_my_sellout_sales_fact.sls_ord_num,
                              itg_my_sellout_sales_fact.sls_ord_dt,
                              itg_my_sellout_sales_fact.type,
                              itg_my_sellout_sales_fact.cust_cd,
                              itg_my_sellout_sales_fact.dstrbtr_wh_id,
                              itg_my_sellout_sales_fact.item_cd,
                              itg_my_sellout_sales_fact.dstrbtr_prod_cd,
                              itg_my_sellout_sales_fact.ean_num,
                              itg_my_sellout_sales_fact.dstrbtr_prod_desc,
                              itg_my_sellout_sales_fact.grs_prc,
                              itg_my_sellout_sales_fact.qty,
                              itg_my_sellout_sales_fact.uom,
                              itg_my_sellout_sales_fact.qty_pc,
                              itg_my_sellout_sales_fact.qty_aft_conv,
                              itg_my_sellout_sales_fact.subtotal_1,
                              itg_my_sellout_sales_fact.discount,
                              itg_my_sellout_sales_fact.subtotal_2,
                              itg_my_sellout_sales_fact.bottom_line_dscnt,
                              itg_my_sellout_sales_fact.total_amt_aft_tax,
                              itg_my_sellout_sales_fact.total_amt_bfr_tax,
                              itg_my_sellout_sales_fact.sls_emp,
                              itg_my_sellout_sales_fact.custom_field1,
                              itg_my_sellout_sales_fact.custom_field2,
                              itg_my_sellout_sales_fact.sap_matl_num,
                              itg_my_sellout_sales_fact.filename,
                              itg_my_sellout_sales_fact.cdl_dttm,
                              itg_my_sellout_sales_fact.crtd_dttm,
                              itg_my_sellout_sales_fact.updt_dttm
                            from itg_my_sellout_sales_fact
                            where
                              (
                                not (
                                  (
                                    coalesce(
                                      ltrim(
                                        cast((
                                          itg_my_sellout_sales_fact.dstrbtr_id
                                        ) as text),
                                        cast((
                                          cast('0' as varchar)
                                        ) as text)
                                      ),
                                      cast((
                                        cast('0' as varchar)
                                      ) as text)
                                    ) || coalesce(
                                      trim(cast((
                                        itg_my_sellout_sales_fact.cust_cd
                                      ) as text)),
                                      cast((
                                        cast('0' as varchar)
                                      ) as text)
                                    )
                                  ) in (
                                    select distinct
                                      (
                                        cast((
                                          coalesce(itg_my_gt_outlet_exclusion.dstrbtr_cd, cast('0' as varchar))
                                        ) as text) || cast((
                                          coalesce(itg_my_gt_outlet_exclusion.outlet_cd, cast('0' as varchar))
                                        ) as text)
                                      )
                                    from itg_my_gt_outlet_exclusion
                                  )
                                )
                              )
                          ) as a, (
                            select distinct
                              edw_vw_os_time_dim.cal_year as year,
                              edw_vw_os_time_dim.cal_qrtr_no as qrtr_no,
                              edw_vw_os_time_dim.cal_mnth_id as mnth_id,
                              edw_vw_os_time_dim.cal_mnth_no as mnth_no,
                              edw_vw_os_time_dim.cal_mnth_nm as mnth_nm,
                              edw_vw_os_time_dim.cal_date,
                              edw_vw_os_time_dim.cal_date_id
                            from edw_vw_os_time_dim
                          ) as veotd
                          where
                            (
                              
                                veotd.cal_date
                               = 
                                a.sls_ord_dt
                              
                            )
                        ) as t1, (
                          select
                            itg_my_trgts.jj_mnth_id,
                            itg_my_trgts.cust_id,
                            sum(itg_my_trgts.trgt_val) as trgt_val
                          from itg_my_trgts
                          where
                            (
                              cast((
                                itg_my_trgts.trgt_type
                              ) as text) = cast((
                                cast('BP' as varchar)
                              ) as text)
                            )
                          group by
                            itg_my_trgts.jj_mnth_id,
                            itg_my_trgts.cust_id
                        ) as trgt
                        where
                          (
                            (
                              cast((
                                trgt.jj_mnth_id
                              ) as text) = cast((
                                cast((
                                  t1.trgt_period
                                ) as varchar)
                              ) as text)
                            )
                            and (
                              cast((
                                coalesce(trgt.cust_id, cast('' as varchar))
                              ) as text) = cast((
                                coalesce(t1.dstrbtr_id, cast('' as varchar))
                              ) as text)
                            )
                          )
                      ) as derived_table1
                      group by
                        derived_table1.year,
                        derived_table1.qrtr_no,
                        derived_table1.mnth_no,
                        derived_table1.mnth_nm,
                        derived_table1.mnth_id,
                        derived_table1.trgt_period,
                        derived_table1.dstrbtr_id,
                        derived_table1.sap_matl_num,
                        derived_table1.cust_cd,
                        derived_table1.dstrbtr_matl_num,
                        derived_table1.bar_cd
                      UNION
                      select
                        derived_table1.year,
                        derived_table1.qrtr_no,
                        derived_table1.mnth_no,
                        derived_table1.mnth_nm,
                        derived_table1.mnth_id,
                        derived_table1.trgt_period,
                        derived_table1.dstrbtr_id,
                        derived_table1.sap_matl_num,
                        derived_table1.dstrbtr_matl_num,
                        derived_table1.bar_cd,
                        derived_table1.cust_cd,
                        derived_table1.contribution
                      from (
                        select
                          trgt1.year,
                          trgt1.qrtr_no,
                          trgt1.mnth_no,
                          trgt1.mnth_nm,
                          trgt1.mnth_id,
                          trgt1.trgt_period,
                          trgt1.dstrbtr_id,
                          trgt1.sap_matl_num,
                          trgt1.dstrbtr_matl_num,
                          trgt1.bar_cd,
                          trgt1.cust_cd,
                          trgt1.contribution,
                          trgt2.trgt_period as trgt_period_present,
                          trgt2.dstrbtr_id as dstrbtr_id_present
                        from (
                          (
                            select
                              derived_table1.year,
                              derived_table1.qrtr_no,
                              derived_table1.mnth_no,
                              derived_table1.mnth_nm,
                              derived_table1.mnth_id,
                              derived_table1.trgt_period,
                              derived_table1.dstrbtr_id,
                              derived_table1.sap_matl_num,
                              derived_table1.dstrbtr_matl_num,
                              derived_table1.bar_cd,
                              derived_table1.cust_cd,
                              sum(derived_table1.contribution) as contribution
                            from (
                              select
                                t1.year,
                                t1.qrtr_no,
                                t1.mnth_no,
                                t1.mnth_nm,
                                t1.mnth_id,
                                t1.trgt_period,
                                t1.dstrbtr_id,
                                t1.sap_matl_num,
                                t1.dstrbtr_matl_num,
                                t1.bar_cd,
                                t1.cust_cd,
                                (
                                  trgt.trgt_val * (
                                    t1.base_sls / t1.total_sales
                                  )
                                ) as contribution
                              from (
                                select
                                  cast((
                                    substring(
                                      to_char(
                                        cast(dateadd(
                                          month,
                                          cast((
                                            0
                                          ) as bigint),
                                          cast(cast((
                                            to_date(
                                              cast((
                                                cast((
                                                  veotd.mnth_id
                                                ) as varchar)
                                              ) as text),
                                              cast((
                                                cast('YYYYMM' as varchar)
                                              ) as text)
                                            )
                                          ) as timestampntz) as timestampntz)
                                        ) as timestampntz),
                                        cast((
                                          cast('YYYYMM' as varchar)
                                        ) as text)
                                      ),
                                      0,
                                      5
                                    )
                                  ) as int) as year,
                                  veotd.qrtr_no,
                                  veotd.mnth_no,
                                  veotd.mnth_nm,
                                  veotd.mnth_id as trgt_period,
                                  cast((
                                    to_char(
                                      cast(dateadd(
                                        month,
                                        (
                                          -cast((
                                            12
                                          ) as bigint)
                                        ),
                                        cast(cast((
                                          to_date(
                                            cast((
                                              cast((
                                                veotd.mnth_id
                                              ) as varchar)
                                            ) as text),
                                            cast((
                                              cast('YYYYMM' as varchar)
                                            ) as text)
                                          )
                                        ) as timestampntz) as timestampntz)
                                      ) as timestampntz),
                                      cast((
                                        cast('YYYYMM' as varchar)
                                      ) as text)
                                    )
                                  ) as int) as mnth_id,
                                  a.dstrbtr_id,
                                  a.dstrbtr_prod_cd as dstrbtr_matl_num,
                                  a.sap_matl_num,
                                  a.ean_num as bar_cd,
                                  a.cust_cd,
                                  a.total_amt_bfr_tax as base_sls,
                                  sum(a.total_amt_bfr_tax) OVER (PARTITION BY veotd.mnth_id, a.dstrbtr_id order by null ROWS BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as total_sales
                                from (
                                  select
                                    itg_my_sellout_sales_fact.dstrbtr_id,
                                    itg_my_sellout_sales_fact.sls_ord_num,
                                    itg_my_sellout_sales_fact.sls_ord_dt,
                                    itg_my_sellout_sales_fact.type,
                                    itg_my_sellout_sales_fact.cust_cd,
                                    itg_my_sellout_sales_fact.dstrbtr_wh_id,
                                    itg_my_sellout_sales_fact.item_cd,
                                    itg_my_sellout_sales_fact.dstrbtr_prod_cd,
                                    itg_my_sellout_sales_fact.ean_num,
                                    itg_my_sellout_sales_fact.dstrbtr_prod_desc,
                                    itg_my_sellout_sales_fact.grs_prc,
                                    itg_my_sellout_sales_fact.qty,
                                    itg_my_sellout_sales_fact.uom,
                                    itg_my_sellout_sales_fact.qty_pc,
                                    itg_my_sellout_sales_fact.qty_aft_conv,
                                    itg_my_sellout_sales_fact.subtotal_1,
                                    itg_my_sellout_sales_fact.discount,
                                    itg_my_sellout_sales_fact.subtotal_2,
                                    itg_my_sellout_sales_fact.bottom_line_dscnt,
                                    itg_my_sellout_sales_fact.total_amt_aft_tax,
                                    itg_my_sellout_sales_fact.total_amt_bfr_tax,
                                    itg_my_sellout_sales_fact.sls_emp,
                                    itg_my_sellout_sales_fact.custom_field1,
                                    itg_my_sellout_sales_fact.custom_field2,
                                    itg_my_sellout_sales_fact.sap_matl_num,
                                    itg_my_sellout_sales_fact.filename,
                                    itg_my_sellout_sales_fact.cdl_dttm,
                                    itg_my_sellout_sales_fact.crtd_dttm,
                                    itg_my_sellout_sales_fact.updt_dttm
                                  from itg_my_sellout_sales_fact
                                  where
                                    (
                                      not (
                                        (
                                          coalesce(
                                            ltrim(
                                              cast((
                                                itg_my_sellout_sales_fact.dstrbtr_id
                                              ) as text),
                                              cast((
                                                cast('0' as varchar)
                                              ) as text)
                                            ),
                                            cast((
                                              cast('0' as varchar)
                                            ) as text)
                                          ) || coalesce(
                                            trim(cast((
                                              itg_my_sellout_sales_fact.cust_cd
                                            ) as text)),
                                            cast((
                                              cast('0' as varchar)
                                            ) as text)
                                          )
                                        ) in (
                                          select distinct
                                            (
                                              cast((
                                                coalesce(itg_my_gt_outlet_exclusion.dstrbtr_cd, cast('0' as varchar))
                                              ) as text) || cast((
                                                coalesce(itg_my_gt_outlet_exclusion.outlet_cd, cast('0' as varchar))
                                              ) as text)
                                            )
                                          from itg_my_gt_outlet_exclusion
                                        )
                                      )
                                    )
                                ) as a, (
                                  select distinct
                                    edw_vw_os_time_dim.cal_year as year,
                                    edw_vw_os_time_dim.cal_qrtr_no as qrtr_no,
                                    edw_vw_os_time_dim.cal_mnth_id as mnth_id,
                                    edw_vw_os_time_dim.cal_mnth_no as mnth_no,
                                    edw_vw_os_time_dim.cal_mnth_nm as mnth_nm,
                                    edw_vw_os_time_dim.cal_date,
                                    edw_vw_os_time_dim.cal_date_id
                                  from edw_vw_os_time_dim
                                ) as veotd
                                where
                                  (
                                    
                                      veotd.cal_date
                                    = 
                                      a.sls_ord_dt
                                    
                                  )
                              ) as t1, (
                                select
                                  itg_my_trgts.jj_mnth_id,
                                  itg_my_trgts.cust_id,
                                  sum(itg_my_trgts.trgt_val) as trgt_val
                                from itg_my_trgts
                                where
                                  (
                                    cast((
                                      itg_my_trgts.trgt_type
                                    ) as text) = cast((
                                      cast('BP' as varchar)
                                    ) as text)
                                  )
                                group by
                                  itg_my_trgts.jj_mnth_id,
                                  itg_my_trgts.cust_id
                              ) as trgt
                              where
                                (
                                  (
                                    cast((
                                      trgt.jj_mnth_id
                                    ) as text) = cast((
                                      cast((
                                        t1.trgt_period
                                      ) as varchar)
                                    ) as text)
                                  )
                                  and (
                                    cast((
                                      coalesce(trgt.cust_id, cast('' as varchar))
                                    ) as text) = cast((
                                      coalesce(t1.dstrbtr_id, cast('' as varchar))
                                    ) as text)
                                  )
                                )
                            ) as derived_table1
                            group by
                              derived_table1.year,
                              derived_table1.qrtr_no,
                              derived_table1.mnth_no,
                              derived_table1.mnth_nm,
                              derived_table1.mnth_id,
                              derived_table1.trgt_period,
                              derived_table1.dstrbtr_id,
                              derived_table1.sap_matl_num,
                              derived_table1.cust_cd,
                              derived_table1.dstrbtr_matl_num,
                              derived_table1.bar_cd
                          ) as trgt1
                          left join (
                            select
                              derived_table1.year,
                              derived_table1.qrtr_no,
                              derived_table1.mnth_no,
                              derived_table1.mnth_nm,
                              derived_table1.mnth_id,
                              derived_table1.trgt_period,
                              derived_table1.dstrbtr_id,
                              derived_table1.sap_matl_num,
                              derived_table1.dstrbtr_matl_num,
                              derived_table1.bar_cd,
                              derived_table1.cust_cd,
                              sum(derived_table1.contribution) as contribution
                            from (
                              select
                                t1.year,
                                t1.qrtr_no,
                                t1.mnth_no,
                                t1.mnth_nm,
                                t1.mnth_id,
                                t1.trgt_period,
                                t1.dstrbtr_id,
                                t1.sap_matl_num,
                                t1.dstrbtr_matl_num,
                                t1.bar_cd,
                                t1.cust_cd,
                                (
                                  trgt.trgt_val * (
                                    t1.base_sls / t1.total_sales
                                  )
                                ) as contribution
                              from (
                                select
                                  cast((
                                    substring(
                                      to_char(
                                        cast(dateadd(
                                          month,
                                          cast((
                                            12
                                          ) as bigint),
                                          cast(cast((
                                            to_date(
                                              cast((
                                                cast((
                                                  veotd.mnth_id
                                                ) as varchar)
                                              ) as text),
                                              cast((
                                                cast('YYYYMM' as varchar)
                                              ) as text)
                                            )
                                          ) as timestampntz) as timestampntz)
                                        ) as timestampntz),
                                        cast((
                                          cast('YYYYMM' as varchar)
                                        ) as text)
                                      ),
                                      0,
                                      5
                                    )
                                  ) as int) as year,
                                  veotd.qrtr_no,
                                  veotd.mnth_no,
                                  veotd.mnth_nm,
                                  veotd.mnth_id,
                                  cast((
                                    to_char(
                                      cast(dateadd(
                                        month,
                                        cast((
                                          12
                                        ) as bigint),
                                        cast(cast((
                                          to_date(
                                            cast((
                                              cast((
                                                veotd.mnth_id
                                              ) as varchar)
                                            ) as text),
                                            cast((
                                              cast('YYYYMM' as varchar)
                                            ) as text)
                                          )
                                        ) as timestampntz) as timestampntz)
                                      ) as timestampntz),
                                      cast((
                                        cast('YYYYMM' as varchar)
                                      ) as text)
                                    )
                                  ) as int) as trgt_period,
                                  a.dstrbtr_id,
                                  a.dstrbtr_prod_cd as dstrbtr_matl_num,
                                  a.sap_matl_num,
                                  a.ean_num as bar_cd,
                                  a.cust_cd,
                                  a.total_amt_bfr_tax as base_sls,
                                  sum(a.total_amt_bfr_tax) OVER (PARTITION BY veotd.mnth_id, a.dstrbtr_id order by null ROWS BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as total_sales
                                from (
                                  select
                                    itg_my_sellout_sales_fact.dstrbtr_id,
                                    itg_my_sellout_sales_fact.sls_ord_num,
                                    itg_my_sellout_sales_fact.sls_ord_dt,
                                    itg_my_sellout_sales_fact.type,
                                    itg_my_sellout_sales_fact.cust_cd,
                                    itg_my_sellout_sales_fact.dstrbtr_wh_id,
                                    itg_my_sellout_sales_fact.item_cd,
                                    itg_my_sellout_sales_fact.dstrbtr_prod_cd,
                                    itg_my_sellout_sales_fact.ean_num,
                                    itg_my_sellout_sales_fact.dstrbtr_prod_desc,
                                    itg_my_sellout_sales_fact.grs_prc,
                                    itg_my_sellout_sales_fact.qty,
                                    itg_my_sellout_sales_fact.uom,
                                    itg_my_sellout_sales_fact.qty_pc,
                                    itg_my_sellout_sales_fact.qty_aft_conv,
                                    itg_my_sellout_sales_fact.subtotal_1,
                                    itg_my_sellout_sales_fact.discount,
                                    itg_my_sellout_sales_fact.subtotal_2,
                                    itg_my_sellout_sales_fact.bottom_line_dscnt,
                                    itg_my_sellout_sales_fact.total_amt_aft_tax,
                                    itg_my_sellout_sales_fact.total_amt_bfr_tax,
                                    itg_my_sellout_sales_fact.sls_emp,
                                    itg_my_sellout_sales_fact.custom_field1,
                                    itg_my_sellout_sales_fact.custom_field2,
                                    itg_my_sellout_sales_fact.sap_matl_num,
                                    itg_my_sellout_sales_fact.filename,
                                    itg_my_sellout_sales_fact.cdl_dttm,
                                    itg_my_sellout_sales_fact.crtd_dttm,
                                    itg_my_sellout_sales_fact.updt_dttm
                                  from itg_my_sellout_sales_fact
                                  where
                                    (
                                      not (
                                        (
                                          coalesce(
                                            ltrim(
                                              cast((
                                                itg_my_sellout_sales_fact.dstrbtr_id
                                              ) as text),
                                              cast((
                                                cast('0' as varchar)
                                              ) as text)
                                            ),
                                            cast((
                                              cast('0' as varchar)
                                            ) as text)
                                          ) || coalesce(
                                            trim(cast((
                                              itg_my_sellout_sales_fact.cust_cd
                                            ) as text)),
                                            cast((
                                              cast('0' as varchar)
                                            ) as text)
                                          )
                                        ) in (
                                          select distinct
                                            (
                                              cast((
                                                coalesce(itg_my_gt_outlet_exclusion.dstrbtr_cd, cast('0' as varchar))
                                              ) as text) || cast((
                                                coalesce(itg_my_gt_outlet_exclusion.outlet_cd, cast('0' as varchar))
                                              ) as text)
                                            )
                                          from itg_my_gt_outlet_exclusion
                                        )
                                      )
                                    )
                                ) as a, (
                                  select distinct
                                    edw_vw_os_time_dim.cal_year as year,
                                    edw_vw_os_time_dim.cal_qrtr_no as qrtr_no,
                                    edw_vw_os_time_dim.cal_mnth_id as mnth_id,
                                    edw_vw_os_time_dim.cal_mnth_no as mnth_no,
                                    edw_vw_os_time_dim.cal_mnth_nm as mnth_nm,
                                    edw_vw_os_time_dim.cal_date,
                                    edw_vw_os_time_dim.cal_date_id
                                  from edw_vw_os_time_dim
                                ) as veotd
                                where
                                  (
                                    
                                      veotd.cal_date
                                     = 
                                      a.sls_ord_dt
                                    
                                  )
                              ) as t1, (
                                select
                                  itg_my_trgts.jj_mnth_id,
                                  itg_my_trgts.cust_id,
                                  sum(itg_my_trgts.trgt_val) as trgt_val
                                from itg_my_trgts
                                where
                                  (
                                    cast((
                                      itg_my_trgts.trgt_type
                                    ) as text) = cast((
                                      cast('BP' as varchar)
                                    ) as text)
                                  )
                                group by
                                  itg_my_trgts.jj_mnth_id,
                                  itg_my_trgts.cust_id
                              ) as trgt
                              where
                                (
                                  (
                                    cast((
                                      trgt.jj_mnth_id
                                    ) as text) = cast((
                                      cast((
                                        t1.trgt_period
                                      ) as varchar)
                                    ) as text)
                                  )
                                  and (
                                    cast((
                                      coalesce(trgt.cust_id, cast('' as varchar))
                                    ) as text) = cast((
                                      coalesce(t1.dstrbtr_id, cast('' as varchar))
                                    ) as text)
                                  )
                                )
                            ) as derived_table1
                            group by
                              derived_table1.year,
                              derived_table1.qrtr_no,
                              derived_table1.mnth_no,
                              derived_table1.mnth_nm,
                              derived_table1.mnth_id,
                              derived_table1.trgt_period,
                              derived_table1.dstrbtr_id,
                              derived_table1.sap_matl_num,
                              derived_table1.cust_cd,
                              derived_table1.dstrbtr_matl_num,
                              derived_table1.bar_cd
                          ) as trgt2
                            on (
                              (
                                (
                                  trgt1.trgt_period = trgt2.trgt_period
                                )
                                and (
                                  cast((
                                    trgt1.dstrbtr_id
                                  ) as text) = cast((
                                    trgt2.dstrbtr_id
                                  ) as text)
                                )
                              )
                            )
                        )
                      ) as derived_table1
                      where
                        (
                          (
                            derived_table1.trgt_period_present IS null
                          )
                          or (
                            derived_table1.dstrbtr_id_present IS null
                          )
                        )
                    ) as trgt
                    left join (
                      select
                        edw_vw_my_dstrbtr_customer_dim.cntry_cd,
                        edw_vw_my_dstrbtr_customer_dim.cntry_nm,
                        edw_vw_my_dstrbtr_customer_dim.dstrbtr_grp_cd,
                        edw_vw_my_dstrbtr_customer_dim.dstrbtr_soldto_code,
                        edw_vw_my_dstrbtr_customer_dim.sap_soldto_code,
                        edw_vw_my_dstrbtr_customer_dim.cust_cd,
                        edw_vw_my_dstrbtr_customer_dim.cust_nm,
                        edw_vw_my_dstrbtr_customer_dim.alt_cust_cd,
                        edw_vw_my_dstrbtr_customer_dim.alt_cust_nm,
                        edw_vw_my_dstrbtr_customer_dim.addr,
                        edw_vw_my_dstrbtr_customer_dim.area_cd,
                        edw_vw_my_dstrbtr_customer_dim.area_nm,
                        edw_vw_my_dstrbtr_customer_dim.state_cd,
                        edw_vw_my_dstrbtr_customer_dim.state_nm,
                        edw_vw_my_dstrbtr_customer_dim.region_cd,
                        edw_vw_my_dstrbtr_customer_dim.region_nm,
                        edw_vw_my_dstrbtr_customer_dim.prov_cd,
                        edw_vw_my_dstrbtr_customer_dim.prov_nm,
                        edw_vw_my_dstrbtr_customer_dim.town_cd,
                        edw_vw_my_dstrbtr_customer_dim.town_nm,
                        edw_vw_my_dstrbtr_customer_dim.city_cd,
                        edw_vw_my_dstrbtr_customer_dim.city_nm,
                        edw_vw_my_dstrbtr_customer_dim.post_cd,
                        edw_vw_my_dstrbtr_customer_dim.post_nm,
                        edw_vw_my_dstrbtr_customer_dim.slsmn_cd,
                        edw_vw_my_dstrbtr_customer_dim.slsmn_nm,
                        edw_vw_my_dstrbtr_customer_dim.chnl_cd,
                        edw_vw_my_dstrbtr_customer_dim.chnl_desc,
                        edw_vw_my_dstrbtr_customer_dim.sub_chnl_cd,
                        edw_vw_my_dstrbtr_customer_dim.sub_chnl_desc,
                        edw_vw_my_dstrbtr_customer_dim.chnl_attr1_cd,
                        edw_vw_my_dstrbtr_customer_dim.chnl_attr1_desc,
                        edw_vw_my_dstrbtr_customer_dim.chnl_attr2_cd,
                        edw_vw_my_dstrbtr_customer_dim.chnl_attr2_desc,
                        edw_vw_my_dstrbtr_customer_dim.outlet_type_cd,
                        edw_vw_my_dstrbtr_customer_dim.outlet_type_desc,
                        edw_vw_my_dstrbtr_customer_dim.cust_grp_cd,
                        edw_vw_my_dstrbtr_customer_dim.cust_grp_desc,
                        edw_vw_my_dstrbtr_customer_dim.cust_grp_attr1_cd,
                        edw_vw_my_dstrbtr_customer_dim.cust_grp_attr1_desc,
                        edw_vw_my_dstrbtr_customer_dim.cust_grp_attr2_cd,
                        edw_vw_my_dstrbtr_customer_dim.cust_grp_attr2_desc,
                        edw_vw_my_dstrbtr_customer_dim.sls_dstrct_cd,
                        edw_vw_my_dstrbtr_customer_dim.sls_dstrct_nm,
                        edw_vw_my_dstrbtr_customer_dim.sls_office_cd,
                        edw_vw_my_dstrbtr_customer_dim.sls_office_desc,
                        edw_vw_my_dstrbtr_customer_dim.sls_grp_cd,
                        edw_vw_my_dstrbtr_customer_dim.sls_grp_desc,
                        edw_vw_my_dstrbtr_customer_dim.STATUS
                      from edw_vw_my_dstrbtr_customer_dim
                      where
                        (
                          cast((
                            edw_vw_my_dstrbtr_customer_dim.cntry_cd
                          ) as text) = cast((
                            cast('MY' as varchar)
                          ) as text)
                        )
                    ) as evodcd
                      on (
                        (
                          (
                            ltrim(cast((
                              evodcd.cust_cd
                            ) as text), cast((
                              cast('0' as varchar)
                            ) as text)) = ltrim(cast((
                              trgt.cust_cd
                            ) as text), cast((
                              cast('0' as varchar)
                            ) as text))
                          )
                          and (
                            ltrim(
                              cast((
                                evodcd.sap_soldto_code
                              ) as text),
                              cast((
                                cast('0' as varchar)
                              ) as text)
                            ) = ltrim(cast((
                              trgt.dstrbtr_id
                            ) as text), cast((
                              cast('0' as varchar)
                            ) as text))
                          )
                        )
                      )
                  )
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
                  on (
                    (
                      ltrim(cast((
                        veocd.sap_cust_id
                      ) as text), cast((
                        cast('0' as varchar)
                      ) as text)) = ltrim(
                        cast((
                          veosf.sap_soldto_code
                        ) as text),
                        cast((
                          cast('0' as varchar)
                        ) as text)
                      )
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
                on (
                  (
                    ltrim(cast((
                      veomd.sap_matl_num
                    ) as text), cast((
                      cast('0' as varchar)
                    ) as text)) = ltrim(cast((
                      veosf.sap_matl_num
                    ) as text), cast((
                      cast('0' as varchar)
                    ) as text))
                  )
                )
            )
            left join itg_my_dstrbtrr_dim as imdd
              on (
                (
                  ltrim(cast((
                    imdd.cust_id
                  ) as text), cast((
                    cast('0' as varchar)
                  ) as text)) = ltrim(
                    cast((
                      veosf.sap_soldto_code
                    ) as text),
                    cast((
                      cast('0' as varchar)
                    ) as text)
                  )
                )
              )
          )
          left join itg_my_material_dim as immd
            on (
              (
                ltrim(cast((
                  immd.item_cd
                ) as text), cast((
                  cast('0' as varchar)
                ) as text)) = ltrim(
                  cast((
                    coalesce(veosf.sap_matl_num, cast('' as varchar))
                  ) as text),
                  cast((
                    cast('0' as varchar)
                  ) as text)
                )
              )
            )
        )
        left join itg_my_customer_dim as imcd
          on (
            (
              trim(cast((
                imcd.cust_id
              ) as text)) = trim(cast((
                veosf.sap_soldto_code
              ) as text))
            )
          )
      )
      left join (
        select
          edw_vw_my_listprice.cntry_key,
          edw_vw_my_listprice.cntry_nm,
          edw_vw_my_listprice.plant,
          edw_vw_my_listprice.cnty,
          edw_vw_my_listprice.item_cd,
          edw_vw_my_listprice.item_desc,
          edw_vw_my_listprice.valid_from,
          edw_vw_my_listprice.valid_to,
          edw_vw_my_listprice.rate,
          edw_vw_my_listprice.currency,
          edw_vw_my_listprice.price_unit,
          edw_vw_my_listprice.uom,
          edw_vw_my_listprice.yearmo,
          edw_vw_my_listprice.mnth_type,
          edw_vw_my_listprice.snapshot_dt
        from edw_vw_my_listprice
        where
          (
            (
              cast((
                edw_vw_my_listprice.cntry_key
              ) as text) = cast((
                cast('MY' as varchar)
              ) as text)
            )
            and (
              cast((
                edw_vw_my_listprice.mnth_type
              ) as text) = cast((
                cast('CAL' as varchar)
              ) as text)
            )
          )
      ) as lp
        on (
          (
            (
              ltrim(cast((
                lp.item_cd
              ) as text), cast((
                cast('0' as varchar)
              ) as text)) = ltrim(cast((
                veosf.sap_matl_num
              ) as text), cast((
                cast('0' as varchar)
              ) as text))
            )
            and (
              cast((
                lp.yearmo
              ) as text) = cast((
                cast((
                  veosf.mnth_id
                ) as varchar)
              ) as text)
            )
          )
        )
    )
    where
      (
        (
          cast((
            cast((
              veosf.mnth_id
            ) as varchar)
          ) as text) >= veocurd.start_period
        )
        and (
          cast((
            cast((
              veosf.mnth_id
            ) as varchar)
          ) as text) <= veocurd.end_period
        )
      )
    union all
    select
      'Target' as data_src,
      veotd.year,
      veotd.qrtr_no,
      veotd.mnth_id,
      veotd.mnth_no,
      veotd.mnth_nm,
      null as max_yearmo,
      'Malaysia' as cntry_nm,
      imcd.dstrbtr_grp_cd,
      imcd.dstrbtr_grp_nm,
      null as dstrbtr_cust_cd,
      null as dstrbtr_cust_nm,
      cast((
        ltrim(cast((
          imdd.cust_id
        ) as text), cast((
          cast('0' as varchar)
        ) as text))
      ) as varchar) as sap_soldto_code,
      imdd.cust_nm as sap_soldto_nm,
      imdd.lvl1 as dstrbtr_lvl1,
      imdd.lvl2 as dstrbtr_lvl2,
      imdd.lvl3 as dstrbtr_lvl3,
      imdd.lvl4 as dstrbtr_lvl4,
      imdd.lvl5 as dstrbtr_lvl5,
      null as region_nm,
      null as town_nm,
      null as slsmn_cd,
      null as chnl_desc,
      null as sub_chnl_desc,
      null as chnl_attr1_desc,
      null as chnl_attr2_desc,
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
      null as dstrbtr_matl_num,
      null as bar_cd,
      null as sku,
      null as frnchse_desc,
      null as brnd_desc,
      null as vrnt_desc,
      null as putup_desc,
      null as item_desc2,
      null as sku_desc,
      null as sap_mat_type_cd,
      null as sap_mat_type_desc,
      null as sap_base_uom_cd,
      null as sap_prchse_uom_cd,
      null as sap_prod_sgmt_cd,
      null as sap_prod_sgmt_desc,
      null as sap_base_prod_cd,
      null as sap_base_prod_desc,
      null as sap_mega_brnd_cd,
      null as sap_mega_brnd_desc,
      null as sap_brnd_cd,
      null as sap_brnd_desc,
      null as sap_vrnt_cd,
      null as sap_vrnt_desc,
      null as sap_put_up_cd,
      null as sap_put_up_desc,
      null as sap_grp_frnchse_cd,
      null as sap_grp_frnchse_desc,
      null as sap_frnchse_cd,
      null as sap_frnchse_desc,
      null as sap_prod_frnchse_cd,
      null as sap_prod_frnchse_desc,
      null as sap_prod_mjr_cd,
      null as sap_prod_mjr_desc,
      null as sap_prod_mnr_cd,
      null as sap_prod_mnr_desc,
      null as sap_prod_hier_cd,
      null as sap_prod_hier_desc,
      null as global_mat_region,
      case
        when (
          cast((
            imt.sub_segment
          ) as text) = cast((
            cast('ALL' as varchar)
          ) as text)
        )
        then veomd.gph_prod_frnchse
        else veomd1.gph_prod_frnchse
      end as global_prod_franchise,
      case
        when (
          cast((
            imt.sub_segment
          ) as text) = cast((
            cast('ALL' as varchar)
          ) as text)
        )
        then veomd.gph_prod_brnd
        else veomd1.gph_prod_brnd
      end as global_prod_brand,
      null as global_prod_variant,
      null as global_prod_put_up_cd,
      null as global_put_up_desc,
      null as global_prod_sub_brand,
      null as global_prod_need_state,
      null as global_prod_category,
      null as global_prod_subcategory,
      null as global_prod_segment,
      veomd1.gph_prod_subsgmnt as global_prod_subsegment,
      null as global_prod_size,
      null as global_prod_size_uom,
      veocurd.from_ccy,
      veocurd.to_ccy,
      veocurd.exch_rate,
      null as wh_id,
      null as doc_type,
      null as doc_type_desc,
      null as bill_date,
      null as bill_doc,
      0 as base_sls,
      0 as sls_qty,
      0 as ret_qty,
      null as uom,
      0 as sls_qty_pc,
      0 as ret_qty_pc,
      0 as in_transit_qty,
      0 as mat_list_price,
      0 as grs_trd_sls,
      0 as ret_val,
      0 as trd_discnt,
      0 as trd_sls,
      0 as net_trd_sls,
      0 as jj_grs_trd_sls,
      0 as jj_ret_val,
      0 as jj_trd_sls,
      0 as jj_net_trd_sls,
      0 as in_transit_val,
      (
        imt.trgt_val * veocurd.exch_rate
      ) as trgt_val,
      null as inv_dt,
      null as warehse_cd,
      0 as end_stock_qty,
      0 as end_stock_val_raw,
      0 as end_stock_val,
      null as is_npi,
      null as npi_str_period,
      null as npi_end_period,
      null as is_reg,
      null as is_promo,
      null as promo_strt_period,
      null as promo_end_period,
      null as is_mcl,
      null as is_hero,
      0 as contribution
    from (
      select
        edw_vw_os_time_dim.cal_year as year,
        edw_vw_os_time_dim.cal_qrtr_no as qrtr_no,
        edw_vw_os_time_dim.cal_mnth_id as mnth_id,
        edw_vw_os_time_dim.cal_mnth_no as mnth_no,
        edw_vw_os_time_dim.cal_mnth_nm as mnth_nm
      from edw_vw_os_time_dim
      group by
        edw_vw_os_time_dim.cal_year,
        edw_vw_os_time_dim.cal_qrtr_no,
        edw_vw_os_time_dim.cal_mnth_id,
        edw_vw_os_time_dim.cal_mnth_no,
        edw_vw_os_time_dim.cal_mnth_nm
    ) as veotd, (
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
              itg_my_trgts as imt
                left join itg_my_customer_dim as imcd
                  on (
                    (
                      ltrim(cast((
                        imcd.cust_id
                      ) as text), cast((
                        cast('0' as varchar)
                      ) as text)) = ltrim(cast((
                        imt.cust_id
                      ) as text), cast((
                        cast('0' as varchar)
                      ) as text))
                    )
                  )
            )
            left join (
              select distinct
                edw_vw_my_material_dim.gph_prod_frnchse,
                edw_vw_my_material_dim.gph_prod_brnd
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
              on (
                case
                  when (
                    cast((
                      imt.sub_segment
                    ) as text) = cast((
                      cast('ALL' as varchar)
                    ) as text)
                  )
                  then (
                    trim(UPPER(cast((
                      veomd.gph_prod_brnd
                    ) as text))) = trim(UPPER(cast((
                      imt.brnd_desc
                    ) as text)))
                  )
                  else cast(null as BOOLEAN)
                end
              )
          )
          left join (
            select distinct
              edw_vw_my_material_dim.gph_prod_frnchse,
              edw_vw_my_material_dim.gph_prod_brnd,
              edw_vw_my_material_dim.gph_prod_subsgmnt
            from edw_vw_my_material_dim
            where
              (
                cast((
                  edw_vw_my_material_dim.cntry_key
                ) as text) = cast((
                  cast('MY' as varchar)
                ) as text)
              )
          ) as veomd1
            on (
              case
                when (
                  cast((
                    imt.sub_segment
                  ) as text) <> cast((
                    cast('ALL' as varchar)
                  ) as text)
                )
                then (
                  (
                    trim(UPPER(cast((
                      veomd1.gph_prod_subsgmnt
                    ) as text))) = trim(UPPER(cast((
                      imt.sub_segment
                    ) as text)))
                  )
                  and (
                    trim(UPPER(cast((
                      veomd1.gph_prod_brnd
                    ) as text))) = trim(UPPER(cast((
                      imt.brnd_desc
                    ) as text)))
                  )
                )
                else cast(null as BOOLEAN)
              end
            )
        )
        left join itg_my_dstrbtrr_dim as imdd
          on (
            (
              ltrim(cast((
                imdd.cust_id
              ) as text), cast((
                cast('0' as varchar)
              ) as text)) = ltrim(cast((
                imt.cust_id
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
        on (
          (
            ltrim(cast((
              veocd.sap_cust_id
            ) as text), cast((
              cast('0' as varchar)
            ) as text)) = ltrim(cast((
              imt.cust_id
            ) as text), cast((
              cast('0' as varchar)
            ) as text))
          )
        )
    )
    where
      (
        (
          (
            (
              cast((
                imt.trgt_type
              ) as text) = cast((
                cast('BP' as varchar)
              ) as text)
            )
            and (
              cast((
                cast((
                  veotd.mnth_id
                ) as varchar)
              ) as text) = cast((
                imt.jj_mnth_id
              ) as text)
            )
          )
          and (
            cast((
              imt.jj_mnth_id
            ) as text) >= veocurd.start_period
          )
        )
        and (
          cast((
            imt.jj_mnth_id
          ) as text) <= veocurd.end_period
        )
      )
  )
  union all
  select
    'GT Inventory' as data_src,
    evosif.year,
    evosif.qrtr_no,
    evosif.mnth_id,
    evosif.mnth_no,
    evosif.mnth_nm,
    null as max_yearmo,
    'Malaysia' as cntry_nm,
    evosif.dstrbtr_grp_cd,
    imcd.dstrbtr_grp_nm,
    null as dstrbtr_cust_cd,
    null as dstrbtr_cust_nm,
    cast((
      ltrim(cast((
        imdd.cust_id
      ) as text), cast((
        cast('0' as varchar)
      ) as text))
    ) as varchar) as sap_soldto_code,
    imdd.cust_nm as sap_soldto_nm,
    imdd.lvl1 as dstrbtr_lvl1,
    imdd.lvl2 as dstrbtr_lvl2,
    imdd.lvl3 as dstrbtr_lvl3,
    imdd.lvl4 as dstrbtr_lvl4,
    imdd.lvl5 as dstrbtr_lvl5,
    imdd.region as region_nm,
    null as town_nm,
    null as slsmn_cd,
    null as chnl_desc,
    null as sub_chnl_desc,
    null as chnl_attr1_desc,
    null as chnl_attr2_desc,
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
      evosif.dstrbtr_matl_num
    ) as varchar) as dstrbtr_matl_num,
    evosif.bar_cd,
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
    null as wh_id,
    null as doc_type,
    null as doc_type_desc,
    null as bill_date,
    null as bill_doc,
    0 as base_sls,
    0 as sls_qty,
    0 as ret_qty,
    null as uom,
    0 as sls_qty_pc,
    0 as ret_qty_pc,
    0 as in_transit_qty,
    lp.rate as mat_list_price,
    0 as grs_trd_sls,
    0 as ret_val,
    0 as trd_discnt,
    0 as trd_sls,
    0 as net_trd_sls,
    0 as jj_grs_trd_sls,
    0 as jj_ret_val,
    0 as jj_trd_sls,
    0 as jj_net_trd_sls,
    0 as in_transit_val,
    0 as trgt_val,
    cast((
     evosif.inv_dt
    ) as varchar) as inv_dt,
    evosif.warehse_cd,
    evosif.end_stock_qty,
    (
      evosif.end_stock_val * veocurd.exch_rate
    ) as end_stock_val_raw,
    (
      (
        evosif.end_stock_qty * veocurd.exch_rate
      ) * lp.rate
    ) as end_stock_val,
    immd.npi_ind as is_npi,
    immd.npi_strt_period as npi_str_period,
    null as npi_end_period,
    null as is_reg,
    immd.promo_reg_ind as is_promo,
    null as promo_strt_period,
    null as promo_end_period,
    null as is_mcl,
    immd.hero_ind as is_hero,
    0 as contribution
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
                  veotd.year,
                  veotd.qrtr_no,
                  veotd.mnth_id,
                  veotd.mnth_no,
                  veotd.mnth_nm,
                  t1.warehse_cd,
                  t1.dstrbtr_grp_cd,
                  t1.dstrbtr_soldto_code,
                  t1.bar_cd,
                  t1.sap_matl_num,
                  trim(cast((
                    t1.dstrbtr_matl_num
                  ) as text)) as dstrbtr_matl_num,
                  t1.inv_dt,
                  t1.soh,
                  t1.soh_val,
                  t1.end_stock_qty,
                  t1.end_stock_val
                from edw_vw_my_sellout_inventory_fact as t1, (
                  select distinct
                    edw_vw_os_time_dim.cal_year as year,
                    edw_vw_os_time_dim.cal_qrtr_no as qrtr_no,
                    edw_vw_os_time_dim.cal_mnth_id as mnth_id,
                    edw_vw_os_time_dim.cal_mnth_no as mnth_no,
                    edw_vw_os_time_dim.cal_mnth_nm as mnth_nm,
                    edw_vw_os_time_dim.cal_date,
                    edw_vw_os_time_dim.cal_date_id
                  from edw_vw_os_time_dim
                ) as veotd
                where
                  (
                    (
                      cast((
                        t1.cntry_cd
                      ) as text) = cast((
                        cast('MY' as varchar)
                      ) as text)
                    )
                    and (
                      
                        veotd.cal_date
                      = t1.inv_dt
                    )
                  )
              ) as evosif
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
                on (
                  (
                    ltrim(cast((
                      veocd.sap_cust_id
                    ) as text), cast((
                      cast('0' as varchar)
                    ) as text)) = ltrim(
                      cast((
                        evosif.dstrbtr_soldto_code
                      ) as text),
                      cast((
                        cast('0' as varchar)
                      ) as text)
                    )
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
              on (
                (
                  ltrim(cast((
                    veomd.sap_matl_num
                  ) as text), cast((
                    cast('0' as varchar)
                  ) as text)) = ltrim(
                    cast((
                      coalesce(evosif.sap_matl_num, cast('' as varchar))
                    ) as text),
                    cast((
                      cast('0' as varchar)
                    ) as text)
                  )
                )
              )
          )
          left join itg_my_dstrbtrr_dim as imdd
            on (
              (
                ltrim(cast((
                  imdd.cust_id
                ) as text), cast((
                  cast('0' as varchar)
                ) as text)) = ltrim(
                  cast((
                    evosif.dstrbtr_soldto_code
                  ) as text),
                  cast((
                    cast('0' as varchar)
                  ) as text)
                )
              )
            )
        )
        left join itg_my_material_dim as immd
          on (
            (
              ltrim(cast((
                immd.item_cd
              ) as text), cast((
                cast('0' as varchar)
              ) as text)) = ltrim(cast((
                evosif.sap_matl_num
              ) as text), cast((
                cast('0' as varchar)
              ) as text))
            )
          )
      )
      left join itg_my_customer_dim as imcd
        on (
          (
            trim(cast((
              imcd.cust_id
            ) as text)) = trim(cast((
              evosif.dstrbtr_soldto_code
            ) as text))
          )
        )
    )
    left join (
      select
        edw_vw_my_listprice.cntry_key,
        edw_vw_my_listprice.cntry_nm,
        edw_vw_my_listprice.plant,
        edw_vw_my_listprice.cnty,
        edw_vw_my_listprice.item_cd,
        edw_vw_my_listprice.item_desc,
        edw_vw_my_listprice.valid_from,
        edw_vw_my_listprice.valid_to,
        edw_vw_my_listprice.rate,
        edw_vw_my_listprice.currency,
        edw_vw_my_listprice.price_unit,
        edw_vw_my_listprice.uom,
        edw_vw_my_listprice.yearmo,
        edw_vw_my_listprice.mnth_type,
        edw_vw_my_listprice.snapshot_dt
      from edw_vw_my_listprice
      where
        (
          (
            cast((
              edw_vw_my_listprice.cntry_key
            ) as text) = cast((
              cast('MY' as varchar)
            ) as text)
          )
          and (
            cast((
              edw_vw_my_listprice.mnth_type
            ) as text) = cast((
              cast('CAL' as varchar)
            ) as text)
          )
        )
    ) as lp
      on (
        (
          (
            ltrim(cast((
              lp.item_cd
            ) as text), cast((
              cast('0' as varchar)
            ) as text)) = ltrim(cast((
              evosif.sap_matl_num
            ) as text), cast((
              cast('0' as varchar)
            ) as text))
          )
          and (
            cast((
              lp.yearmo
            ) as text) = cast((
              cast((
                evosif.mnth_id
              ) as varchar)
            ) as text)
          )
        )
      )
  )
  where
    (
      (
        cast((
          cast((
            evosif.mnth_id
          ) as varchar)
        ) as text) >= veocurd.start_period
      )
      and (
        cast((
          cast((
            evosif.mnth_id
          ) as varchar)
        ) as text) <= veocurd.end_period
      )
    )
)
union all
select
  'IN Transit' as data_src,
  veoint.year,
  veoint.qrtr_no,
  veoint.mnth_id,
  veoint.mnth_no,
  veoint.mnth_nm,
  null as max_yearmo,
  'Malaysia' as cntry_nm,
  imcd.dstrbtr_grp_cd,
  imcd.dstrbtr_grp_nm,
  null as dstrbtr_cust_cd,
  null as dstrbtr_cust_nm,
  cast((
    ltrim(cast((
      imdd.cust_id
    ) as text), cast((
      cast('0' as varchar)
    ) as text))
  ) as varchar) as sap_soldto_code,
  imdd.cust_nm as sap_soldto_nm,
  imdd.lvl1 as dstrbtr_lvl1,
  imdd.lvl2 as dstrbtr_lvl2,
  imdd.lvl3 as dstrbtr_lvl3,
  imdd.lvl4 as dstrbtr_lvl4,
  imdd.lvl5 as dstrbtr_lvl5,
  imdd.region as region_nm,
  null as town_nm,
  null as slsmn_cd,
  null as chnl_desc,
  null as sub_chnl_desc,
  null as chnl_attr1_desc,
  null as chnl_attr2_desc,
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
  null as dstrbtr_matl_num,
  null as bar_cd,
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
  null as wh_id,
  null as doc_type,
  null as doc_type_desc,
  null as bill_date,
  null as bill_doc,
  0 as base_sls,
  0 as sls_qty,
  0 as ret_qty,
  null as uom,
  0 as sls_qty_pc,
  0 as ret_qty_pc,
  veoint.bill_qty_pc as in_transit_qty,
  0 as mat_list_price,
  0 as grs_trd_sls,
  0 as ret_val,
  0 as trd_discnt,
  0 as trd_sls,
  0 as net_trd_sls,
  0 as jj_grs_trd_sls,
  0 as jj_ret_val,
  0 as jj_trd_sls,
  0 as jj_net_trd_sls,
  (
    veoint.billing_gross_val * veocurd.exch_rate
  ) as in_transit_val,
  0 as trgt_val,
  null as inv_dt,
  null as warehse_cd,
  0 as end_stock_qty,
  0 as end_stock_val_raw,
  0 as end_stock_val,
  null as is_npi,
  null as npi_str_period,
  null as npi_end_period,
  null as is_reg,
  null as is_promo,
  null as promo_strt_period,
  null as promo_end_period,
  null as is_mcl,
  null as is_hero,
  0 as contribution
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
              a.bill_doc,
              b.bill_num,
              b.sold_to,
              b.matl_num,
              b.bill_qty_pc,
              b.billing_gross_val,
              veotd.year,
              veotd.qrtr_no,
              veotd.mnth_id,
              veotd.mnth_no,
              veotd.mnth_nm
            from itg_my_in_transit as a, (
              select
                edw_vw_my_billing_fact.bill_num,
                edw_vw_my_billing_fact.sold_to,
                edw_vw_my_billing_fact.matl_num,
                sum(edw_vw_my_billing_fact.bill_qty_pc) as bill_qty_pc,
                sum(
                  (
                    edw_vw_my_billing_fact.grs_trd_sls * ABS(edw_vw_my_billing_fact.exchg_rate)
                  )
                ) as billing_gross_val
              from edw_vw_my_billing_fact
              where
                (
                  edw_vw_my_billing_fact.cntry_key = cast((
                    cast('MY' as varchar)
                  ) as text)
                )
              group by
                edw_vw_my_billing_fact.bill_num,
                edw_vw_my_billing_fact.sold_to,
                edw_vw_my_billing_fact.matl_num
            ) as b, (
              select
                edw_vw_os_time_dim.cal_year as year,
                edw_vw_os_time_dim.cal_qrtr_no as qrtr_no,
                edw_vw_os_time_dim.cal_mnth_id as mnth_id,
                edw_vw_os_time_dim.cal_mnth_no as mnth_no,
                edw_vw_os_time_dim.cal_mnth_nm as mnth_nm,
                edw_vw_os_time_dim.cal_date
              from edw_vw_os_time_dim
              group by
                edw_vw_os_time_dim.cal_year,
                edw_vw_os_time_dim.cal_qrtr_no,
                edw_vw_os_time_dim.cal_mnth_id,
                edw_vw_os_time_dim.cal_mnth_no,
                edw_vw_os_time_dim.cal_mnth_nm,
                edw_vw_os_time_dim.cal_date
            ) as veotd
            where
              (
                (
                  b.bill_num = cast((
                    a.bill_doc
                  ) as text)
                )
                and (
                  a.closing_dt = veotd.cal_date
                )
              )
          ) as veoint
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
            on (
              (
                ltrim(cast((
                  veocd.sap_cust_id
                ) as text), cast((
                  cast('0' as varchar)
                ) as text)) = ltrim(veoint.sold_to, cast((
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
          on (
            (
              ltrim(cast((
                veomd.sap_matl_num
              ) as text), cast((
                cast('0' as varchar)
              ) as text)) = ltrim(veoint.matl_num, cast((
                cast('0' as varchar)
              ) as text))
            )
          )
      )
      left join itg_my_dstrbtrr_dim as imdd
        on (
          (
            ltrim(cast((
              imdd.cust_id
            ) as text), cast((
              cast('0' as varchar)
            ) as text)) = ltrim(veoint.sold_to, cast((
              cast('0' as varchar)
            ) as text))
          )
        )
    )
    left join itg_my_material_dim as immd
      on (
        (
          ltrim(cast((
            immd.item_cd
          ) as text), cast((
            cast('0' as varchar)
          ) as text)) = ltrim(veoint.matl_num, cast((
            cast('0' as varchar)
          ) as text))
        )
      )
  )
  left join itg_my_customer_dim as imcd
    on (
      (
        ltrim(cast((
          imcd.cust_id
        ) as text), cast((
          cast('0' as varchar)
        ) as text)) = ltrim(veoint.sold_to, cast((
          cast('0' as varchar)
        ) as text))
      )
    )
)
where
  (
    (
      cast((
        cast((
          veoint.mnth_id
        ) as varchar)
      ) as text) >= veocurd.start_period
    )
    and (
      cast((
        cast((
          veoint.mnth_id
        ) as varchar)
      ) as text) <= veocurd.end_period
    )
  )
)
select * from final
