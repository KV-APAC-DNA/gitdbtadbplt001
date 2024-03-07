with 
edw_vw_os_sellout_sales_fact as (
    select * from {{ ref('thaedw_integration__edw_vw_th_sellout_sales_fact') }}
),

edw_vw_os_sellout_inventory_fact as (
    select * from {{ ref('thaedw_integration__edw_vw_th_sellout_inventory_fact') }}
),

itg_th_dstrbtr_material_dim as (
    select * from {{ ref('thaitg_integration__itg_th_dstrbtr_material_dim') }}
),

edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),

edw_vw_os_material_dim as (
    select * from {{ ref('thaedw_integration__edw_vw_th_material_dim') }}
),

edw_vw_os_dstrbtr_customer_dim as (
    select * from {{ ref('thaedw_integration__edw_vw_th_dstrbtr_customer_dim') }}
),

edw_vw_os_sellin_sales_fact as (
    select * from {{ ref('thaedw_integration__edw_vw_th_sellin_sales_fact') }}
),

edw_customer_sales_dim as (
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),

edw_vw_os_customer_dim as (
    select * from {{ ref('thaedw_integration__edw_vw_th_customer_dim') }}
),
edw_company_dim as (
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
v_edw_customer_sales_dim as (
    select * from {{ ref('aspedw_integration__v_edw_customer_sales_dim') }}
),
itg_th_dtsdistributor as (
    select * from {{ ref('thaitg_integration__itg_th_dtsdistributor') }}
),
edw_vw_os_dstrbtr_material_dim as (
    select * from {{ ref('thaedw_integration__edw_vw_th_dstrbtr_material_dim') }}
),

sales as 
(
      select
        sales.typ,
        sales.cntry_cd,
        sales.cntry_nm,
        "time"."year",
        "time".qrtr,
        "time".mnth_id,
        "time".mnth_no,
        "time".wk,
        "time".mnth_wk_no,
        "time".cal_date,
        sales.warehse_cd,
        sales.warehse_grp,
        sales.bill_date,
        sales.dstrbtr_grp_cd,
        sales.dstrbtr_matl_num,
        sales.sls_qty,
        sales.ret_qty,
        sales.grs_trd_sls,
        sales.soh,
        sales.amt_bfr_disc,
        sales.amount_sls
      from (
        (
          select
            cast('Sales' as varchar) as typ,
            edw_vw_os_sellout_sales_fact.cntry_cd,
            edw_vw_os_sellout_sales_fact.cntry_nm,
            edw_vw_os_sellout_sales_fact.bill_date,
            edw_vw_os_sellout_sales_fact.dstrbtr_grp_cd,
            edw_vw_os_sellout_sales_fact.dstrbtr_matl_num,
            cast(null as varchar) as warehse_cd,
            cast(null as varchar) as warehse_grp,
            sum(edw_vw_os_sellout_sales_fact.sls_qty) as sls_qty,
            sum(edw_vw_os_sellout_sales_fact.ret_qty) as ret_qty,
            sum(
              case
                when (
                  (
                    edw_vw_os_sellout_sales_fact.cn_reason_cd is null
                  )
                  or (
                    left(cast((
                      edw_vw_os_sellout_sales_fact.cn_reason_cd
                    ) as text), 1) <> cast((
                      cast('N' as varchar)
                    ) as text)
                  )
                )
                then cast((
                  (
                    edw_vw_os_sellout_sales_fact.grs_trd_sls + edw_vw_os_sellout_sales_fact.ret_val
                  )
                ) as double)
                else cast(null as double)
              end
            ) as grs_trd_sls,
            0 as soh,
            0 as amt_bfr_disc,
            0 as amount_sls
          from edw_vw_os_sellout_sales_fact
          where
            (
              cast((
                edw_vw_os_sellout_sales_fact.cntry_cd
              ) as text) = cast((
                cast('TH' as varchar)
              ) as text)
            )
          group by
            edw_vw_os_sellout_sales_fact.cntry_cd,
            edw_vw_os_sellout_sales_fact.cntry_nm,
            edw_vw_os_sellout_sales_fact.bill_date,
            edw_vw_os_sellout_sales_fact.dstrbtr_grp_cd,
            edw_vw_os_sellout_sales_fact.dstrbtr_matl_num
          union all
          select
            cast('Inventory' as varchar) as typ,
            cast('TH' as varchar) as cntry_cd,
            cast('Thailand' as varchar) as cntry_nm,
            inventory.inv_dt,
            inventory.dstrbtr_grp_cd,
            inventory.dstrbtr_matl_num,
            inventory.warehse_cd,
            case
              when (
                substring(cast((
                  inventory.warehse_cd
                ) as text), 2, 1) = cast((
                  cast('7' as varchar)
                ) as text)
              )
              then cast('Damage Goods' as varchar)
              when (
                cast((
                  inventory.warehse_cd
                ) as text) = cast((
                  cast('V902' as varchar)
                ) as text)
              )
              then cast('Damage Goods' as varchar)
              when (
                (
                  substring(cast((
                    inventory.warehse_cd
                  ) as text), 2, 1) <> cast((
                    cast('7' as varchar)
                  ) as text)
                )
                or (
                  cast((
                    inventory.warehse_cd
                  ) as text) <> cast((
                    cast('V902' as varchar)
                  ) as text)
                )
              )
              then cast('Normal Goods' as varchar)
              else cast(null as varchar)
            end as warehse_grp,
            0 as sls_qty,
            0 as ret_qty,
            0 as net_trd_sls,
            inventory.soh,
            (
              inventory.soh * cast((
                itg_material.sls_prc3
              ) as double)
            ) as amt_bfr_disc,
            (
              cast((
                itg_material.sls_prc_credit
              ) as double) * inventory.soh
            ) as amount_inv
          from (
            (
              select distinct
                inventory.warehse_cd,
                inventory.dstrbtr_grp_cd,
                inventory.dstrbtr_matl_num,
                inventory.inv_dt,
                (
                  cast((
                    sum(inventory.soh)
                  ) as double) / cast((
                    12
                  ) as double)
                ) as soh
              from edw_vw_os_sellout_inventory_fact as inventory
              where
                (
                  (
                    cast((
                      inventory.cntry_cd
                    ) as text) = cast((
                      cast('TH' as varchar)
                    ) as text)
                  )
                  and (
                    inventory.soh > cast((
                      cast((
                        cast((
                          0
                        ) as decimal)
                      ) as decimal(18, 0))
                    ) as decimal(22, 6))
                  )
                )
              group by
                inventory.warehse_cd,
                inventory.dstrbtr_grp_cd,
                inventory.dstrbtr_matl_num,
                inventory.inv_dt
            ) as inventory
            left join (
              select distinct
                itg_th_dstrbtr_material_dim.item_cd,
                itg_th_dstrbtr_material_dim.sls_prc3,
                itg_th_dstrbtr_material_dim.sls_prc_credit
              from itg_th_dstrbtr_material_dim
            ) as itg_material
              on (
                (
                  cast((
                    inventory.dstrbtr_matl_num
                  ) as text) = cast((
                    itg_material.item_cd
                  ) as text)
                )
              )
          )
        ) as sales
        join (
          select distinct
            edw_vw_os_time_dim."year",
            edw_vw_os_time_dim.qrtr,
            edw_vw_os_time_dim.mnth_id,
            edw_vw_os_time_dim.mnth_no,
            edw_vw_os_time_dim.wk,
            edw_vw_os_time_dim.mnth_wk_no,
            edw_vw_os_time_dim.cal_date
          from edw_vw_os_time_dim as edw_vw_os_time_dim
          where
            (
              (
                edw_vw_os_time_dim."year" >(date_part(year, current_timestamp()) - 6)
              )
              or (
                edw_vw_os_time_dim."year" >(date_part(year, current_timestamp()) - 6)
              )
            )
        ) as "time"
          on (
            (
              sales.bill_date = cast((
                "time".cal_date
              ) as timestampntz)
            )
          )
      )
    ) ,
matl as 
(
      select distinct
        edw_vw_os_material_dim.cntry_key,
        edw_vw_os_material_dim.sap_matl_num,
        edw_vw_os_material_dim.sap_mat_desc,
        edw_vw_os_material_dim.gph_region,
        edw_vw_os_material_dim.gph_prod_frnchse,
        edw_vw_os_material_dim.gph_prod_brnd,
        edw_vw_os_material_dim.gph_prod_vrnt,
        edw_vw_os_material_dim.gph_prod_sgmnt,
        edw_vw_os_material_dim.gph_prod_put_up_desc,
        edw_vw_os_material_dim.gph_prod_sub_brnd as prod_sub_brand,
        edw_vw_os_material_dim.gph_prod_subsgmnt as prod_subsegment,
        edw_vw_os_material_dim.gph_prod_ctgry as prod_category,
        edw_vw_os_material_dim.gph_prod_subctgry as prod_subcategory
      from edw_vw_os_material_dim
      where
        (
          cast((
            edw_vw_os_material_dim.cntry_key
          ) as text) = cast((
            cast('TH' as varchar)
          ) as text)
        )
    ),
cust as 
(
    select
      sellout_cust.dstrbtr_grp_cd,
      sellin_cust.sap_cust_id,
      sellin_cust.sap_cust_nm,
      sellin_cust.sap_sls_org,
      sellin_cust.sap_cmp_id,
      sellin_cust.sap_cntry_cd,
      sellin_cust.sap_cntry_nm,
      sellin_cust.sap_addr,
      sellin_cust.sap_region,
      sellin_cust.sap_state_cd,
      sellin_cust.sap_city,
      sellin_cust.sap_post_cd,
      sellin_cust.sap_chnl_cd,
      sellin_cust.sap_chnl_desc,
      sellin_cust.sap_sls_office_cd,
      sellin_cust.sap_sls_office_desc,
      sellin_cust.sap_sls_grp_cd,
      sellin_cust.sap_sls_grp_desc,
      sellin_cust.sap_prnt_cust_key,
      sellin_cust.sap_prnt_cust_desc,
      sellin_cust.sap_cust_chnl_key,
      sellin_cust.sap_cust_chnl_desc,
      sellin_cust.sap_cust_sub_chnl_key,
      sellin_cust.sap_sub_chnl_desc,
      sellin_cust.sap_go_to_mdl_key,
      sellin_cust.sap_go_to_mdl_desc,
      sellin_cust.sap_bnr_key,
      sellin_cust.sap_bnr_desc,
      sellin_cust.sap_bnr_frmt_key,
      sellin_cust.sap_bnr_frmt_desc,
      sellin_cust.retail_env
    from (
      (
        select distinct
          edw_vw_os_dstrbtr_customer_dim.dstrbtr_grp_cd,
          edw_vw_os_dstrbtr_customer_dim.sap_soldto_code
        from edw_vw_os_dstrbtr_customer_dim
        where
          (
            cast((
              edw_vw_os_dstrbtr_customer_dim.cntry_cd
            ) as text) = cast((
              cast('TH' as varchar)
            ) as text)
          )
      ) as sellout_cust
      join edw_vw_os_customer_dim as sellin_cust
        on (
          (
            cast((
              sellout_cust.sap_soldto_code
            ) as text) = cast((
              sellin_cust.sap_cust_id
            ) as text)
          )
        )
    )
  ) ,
sellin_fact as 
(
              select
                edw_vw_os_sellin_sales_fact.item_cd,
                edw_vw_os_sellin_sales_fact.cust_id,
                edw_vw_os_sellin_sales_fact.sls_org,
                edw_vw_os_sellin_sales_fact.sls_grp as sap_sls_grp_cd,
                sls_grp_lkp.sls_grp_desc as sap_sls_grp_desc,
                edw_vw_os_sellin_sales_fact.sls_ofc as sap_sls_office_cd,
                sls_ofc_lkp.sls_ofc_desc as sap_sls_office_desc,
                edw_vw_os_sellin_sales_fact.plnt,
                edw_vw_os_sellin_sales_fact.acct_no,
                edw_vw_os_sellin_sales_fact.cust_grp,
                edw_vw_os_sellin_sales_fact.cust_sls,
                edw_vw_os_sellin_sales_fact.pstng_per,
                edw_vw_os_sellin_sales_fact.dstr_chnl,
                edw_vw_os_sellin_sales_fact.jj_mnth_id,
                sum(edw_vw_os_sellin_sales_fact.base_val) as base_val,
                sum(edw_vw_os_sellin_sales_fact.sls_qty) as sls_qty,
                sum(edw_vw_os_sellin_sales_fact.ret_qty) as ret_qty,
                sum(edw_vw_os_sellin_sales_fact.sls_less_rtn_qty) as sls_less_rtn_qty,
                sum(edw_vw_os_sellin_sales_fact.gts_val) as gts_val,
                sum(edw_vw_os_sellin_sales_fact.ret_val) as ret_val,
                sum(edw_vw_os_sellin_sales_fact.gts_less_rtn_val) as gts_less_rtn_val,
                sum(edw_vw_os_sellin_sales_fact.tp_val) as tp_val,
                sum(edw_vw_os_sellin_sales_fact.nts_val) as nts_val,
                sum(edw_vw_os_sellin_sales_fact.nts_qty) as nts_qty
              from (
                (
                  edw_vw_os_sellin_sales_fact as edw_vw_os_sellin_sales_fact
                    left join (
                      select distinct
                        edw_customer_sales_dim.sls_ofc as sap_sls_office_cd,
                        edw_customer_sales_dim.sls_ofc_desc
                      from edw_customer_sales_dim
                      where
                        (
                          (
                            (
                              cast((
                                edw_customer_sales_dim.sls_org
                              ) as text) = cast((
                                cast('2400' as varchar)
                              ) as text)
                            )
                            or (
                              cast((
                                edw_customer_sales_dim.sls_org
                              ) as text) = cast((
                                cast('2500' as varchar)
                              ) as text)
                            )
                          )
                          and (
                            not case
                              when (
                                cast((
                                  edw_customer_sales_dim.sls_ofc
                                ) as text) = cast((
                                  cast('' as varchar)
                                ) as text)
                              )
                              then cast(null as varchar)
                              else edw_customer_sales_dim.sls_ofc
                            end is null
                          )
                        )
                    ) as sls_ofc_lkp
                      on (
                        (
                          cast((
                            sls_ofc_lkp.sap_sls_office_cd
                          ) as text) = cast((
                            edw_vw_os_sellin_sales_fact.sls_ofc
                          ) as text)
                        )
                      )
                )
                left join (
                  select distinct
                    edw_customer_sales_dim.sls_grp,
                    edw_customer_sales_dim.sls_grp_desc
                  from edw_customer_sales_dim
                  where
                    (
                      (
                        (
                          cast((
                            edw_customer_sales_dim.sls_org
                          ) as text) = cast((
                            cast('2400' as varchar)
                          ) as text)
                        )
                        or (
                          cast((
                            edw_customer_sales_dim.sls_org
                          ) as text) = cast((
                            cast('2500' as varchar)
                          ) as text)
                        )
                      )
                      and (
                        not case
                          when (
                            cast((
                              edw_customer_sales_dim.sls_grp
                            ) as text) = cast((
                              cast('' as varchar)
                            ) as text)
                          )
                          then cast(null as varchar)
                          else edw_customer_sales_dim.sls_grp
                        end is null
                      )
                    )
                ) as sls_grp_lkp
                  on (
                    (
                      cast((
                        sls_grp_lkp.sls_grp
                      ) as text) = cast((
                        edw_vw_os_sellin_sales_fact.sls_grp
                      ) as text)
                    )
                  )
              )
              where
                (
                  cast((
                    edw_vw_os_sellin_sales_fact.cntry_nm
                  ) as text) = cast((
                    cast('TH' as varchar)
                  ) as text)
                )
              group by
                edw_vw_os_sellin_sales_fact.item_cd,
                edw_vw_os_sellin_sales_fact.cust_id,
                edw_vw_os_sellin_sales_fact.sls_grp,
                edw_vw_os_sellin_sales_fact.sls_org,
                edw_vw_os_sellin_sales_fact.sls_ofc,
                edw_vw_os_sellin_sales_fact.plnt,
                edw_vw_os_sellin_sales_fact.acct_no,
                edw_vw_os_sellin_sales_fact.dstr_chnl,
                edw_vw_os_sellin_sales_fact.cust_grp,
                edw_vw_os_sellin_sales_fact.cust_sls,
                edw_vw_os_sellin_sales_fact.pstng_per,
                edw_vw_os_sellin_sales_fact.jj_mnth_id,
                sls_ofc_lkp.sls_ofc_desc,
                sls_grp_lkp.sls_grp_desc
            ) ,
cmp as 
(
          select
            edw_company_dim.co_cd,
            edw_company_dim.company_nm
          from edw_company_dim
          where
            (
              cast((
                edw_company_dim.ctry_key
              ) as text) = cast((
                cast('TH' as varchar)
              ) as text)
            )
        ) ,
sellin_mat as 
(
          select distinct
            edw_vw_os_material_dim.sap_matl_num,
            edw_vw_os_material_dim.sap_mat_desc,
            edw_vw_os_material_dim.gph_region,
            edw_vw_os_material_dim.gph_prod_frnchse,
            edw_vw_os_material_dim.gph_prod_brnd,
            edw_vw_os_material_dim.gph_prod_vrnt,
            edw_vw_os_material_dim.gph_prod_sgmnt,
            edw_vw_os_material_dim.gph_prod_put_up_desc,
            edw_vw_os_material_dim.gph_prod_sub_brnd as prod_sub_brand,
            edw_vw_os_material_dim.gph_prod_subsgmnt as prod_subsegment,
            edw_vw_os_material_dim.gph_prod_ctgry as prod_category,
            edw_vw_os_material_dim.gph_prod_subctgry as prod_subcategory
          from edw_vw_os_material_dim as edw_vw_os_material_dim
          where
            (
              cast((
                edw_vw_os_material_dim.cntry_key
              ) as text) = cast((
                cast('TH' as varchar)
              ) as text)
            )
        ) ,
sellout_mat as 
(
          select distinct
            edw_vw_os_dstrbtr_material_dim.dstrbtr_matl_num,
            edw_vw_os_dstrbtr_material_dim.is_npi,
            edw_vw_os_dstrbtr_material_dim.npi_str_period,
            edw_vw_os_dstrbtr_material_dim.npi_end_period,
            edw_vw_os_dstrbtr_material_dim.is_reg,
            edw_vw_os_dstrbtr_material_dim.is_promo,
            edw_vw_os_dstrbtr_material_dim.promo_strt_period,
            edw_vw_os_dstrbtr_material_dim.promo_end_period,
            edw_vw_os_dstrbtr_material_dim.is_mcl,
            edw_vw_os_dstrbtr_material_dim.is_hero
          from edw_vw_os_dstrbtr_material_dim
          where
            (
              cast((
                edw_vw_os_dstrbtr_material_dim.cntry_cd
              ) as text) = cast((
                cast('TH' as varchar)
              ) as text)
            )
        ),
mat as 
(
      select
        sellin_mat.sap_matl_num,
        sellin_mat.sap_mat_desc,
        sellin_mat.gph_region,
        sellin_mat.gph_prod_frnchse,
        sellin_mat.gph_prod_brnd,
        sellin_mat.gph_prod_vrnt,
        sellin_mat.gph_prod_sgmnt,
        sellin_mat.gph_prod_put_up_desc,
        sellin_mat.prod_sub_brand,
        sellin_mat.prod_subsegment,
        sellin_mat.prod_category,
        sellin_mat.prod_subcategory,
        sellout_mat.is_npi,
        sellout_mat.npi_str_period,
        sellout_mat.npi_end_period,
        sellout_mat.is_reg,
        sellout_mat.is_promo,
        sellout_mat.promo_strt_period,
        sellout_mat.promo_end_period,
        sellout_mat.is_mcl,
        sellout_mat.is_hero
      from (
         sellin_mat
        left join  sellout_mat
          on (
            (
              cast((
                sellin_mat.sap_matl_num
              ) as text) = cast((
                sellout_mat.dstrbtr_matl_num
              ) as text)
            )
          )
      )
    ) ,


sellin as 
(
  select
    "time"."year" as year_jnj,
    "time".qrtr as year_quarter_jnj,
    "time".mnth_id as year_month_jnj,
    "time".mnth_no as month_number_jnj,
    sellin_fact.cust_id as customer_id,
    cmp.company_nm as sap_company_name,
    cust.sap_cust_id,
    cust.sap_cust_nm,
    cust.sap_sls_org,
    cust.sap_cmp_id,
    cust.sap_cntry_cd,
    cust.sap_cntry_nm,
    cust.sap_addr,
    cust.sap_region,
    cust.sap_state_cd,
    cust.sap_city,
    cust.sap_post_cd,
    cust.sap_chnl_cd,
    cust.sap_chnl_desc,
    sellin_fact.sap_sls_office_cd,
    sellin_fact.sap_sls_office_desc,
    sellin_fact.sap_sls_grp_cd,
    sellin_fact.sap_sls_grp_desc,
    cust.sap_prnt_cust_key,
    cust.sap_prnt_cust_desc,
    cust.sap_cust_chnl_key,
    cust.sap_cust_chnl_desc,
    cust.sap_cust_sub_chnl_key,
    cust.sap_sub_chnl_desc,
    cust.sap_go_to_mdl_key,
    cust.sap_go_to_mdl_desc as go_to_model_description,
    cust.sap_bnr_key,
    cust.sap_bnr_desc as banner_description,
    cust.sap_bnr_frmt_key,
    cust.sap_bnr_frmt_desc,
    cust.retail_env,
    so_cust.dstrbtr_grp_cd,
    sellin_fact.plnt as plant,
    sellin_fact.acct_no as account_number,
    sellin_fact.cust_grp as customer_group,
    sellin_fact.cust_sls as customer_sales,
    sellin_fact.pstng_per as posting_per,
    sellin_fact.dstr_chnl as distributor_channel,
    sellin_fact.item_cd as item_code,
    mat.sap_mat_desc as item_description,
    mat.gph_prod_frnchse as franchise,
    mat.gph_prod_brnd as brand,
    mat.gph_prod_vrnt as variant,
    mat.gph_prod_sgmnt as segment,
    mat.gph_prod_put_up_desc as put_up,
    mat.prod_sub_brand,
    mat.prod_subsegment,
    mat.prod_category,
    mat.prod_subcategory,
    mat.is_npi as npi_indicator,
    mat.npi_str_period as npi_start_date,
    mat.npi_end_period as npi_end_date,
    mat.is_reg as reg_indicator,
    mat.is_hero as hero_indicator,
    sellin_fact.base_val as base_value,
    sellin_fact.sls_qty as sales_quantity,
    sellin_fact.ret_qty as return_quantity,
    sellin_fact.sls_less_rtn_qty as sales_less_return_quantity,
    sellin_fact.gts_val as gross_trade_sales_value,
    sellin_fact.ret_val as return_value,
    sellin_fact.gts_less_rtn_val as gross_trade_sales_less_return_value,
    sellin_fact.tp_val as tp_value,
    sellin_fact.nts_val as net_trade_sales_value,
    sellin_fact.nts_qty as net_trade_sales_quantity
  from (
    (
      (
        (
          (
             sellin_fact
            join (
              select distincT
                edw_vw_os_time_dim."year",
                edw_vw_os_time_dim.qrtr,
                edw_vw_os_time_dim.mnth_id,
                edw_vw_os_time_dim.mnth_no
              from edw_vw_os_time_dim as edw_vw_os_time_dim
              where
                (
                  edw_vw_os_time_dim."year" > (date_part(year, current_timestamp()) - 6)
                )
            ) as "time"
              on (
                (
                  cast((
                    sellin_fact.jj_mnth_id
                  ) as text) = "time".mnth_id
                )
              )
          )
          left join (
            select
              edw_vw_os_customer_dim.sap_cust_id,
              edw_vw_os_customer_dim.sap_cust_nm,
              edw_vw_os_customer_dim.sap_sls_org,
              edw_vw_os_customer_dim.sap_cmp_id,
              edw_vw_os_customer_dim.sap_cntry_cd,
              edw_vw_os_customer_dim.sap_cntry_nm,
              edw_vw_os_customer_dim.sap_addr,
              edw_vw_os_customer_dim.sap_region,
              edw_vw_os_customer_dim.sap_state_cd,
              edw_vw_os_customer_dim.sap_city,
              edw_vw_os_customer_dim.sap_post_cd,
              edw_vw_os_customer_dim.sap_chnl_cd,
              edw_vw_os_customer_dim.sap_chnl_desc,
              edw_vw_os_customer_dim.sap_prnt_cust_key,
              edw_vw_os_customer_dim.sap_prnt_cust_desc,
              edw_vw_os_customer_dim.sap_cust_chnl_key,
              edw_vw_os_customer_dim.sap_cust_chnl_desc,
              edw_vw_os_customer_dim.sap_cust_sub_chnl_key,
              edw_vw_os_customer_dim.sap_sub_chnl_desc,
              edw_vw_os_customer_dim.sap_go_to_mdl_key,
              edw_vw_os_customer_dim.sap_go_to_mdl_desc,
              edw_vw_os_customer_dim.sap_bnr_key,
              edw_vw_os_customer_dim.sap_bnr_desc,
              edw_vw_os_customer_dim.sap_bnr_frmt_key,
              edw_vw_os_customer_dim.sap_bnr_frmt_desc,
              edw_vw_os_customer_dim.retail_env
            from edw_vw_os_customer_dim as edw_vw_os_customer_dim
            where
              (
                cast((
                  edw_vw_os_customer_dim.sap_cntry_cd
                ) as text) = cast((
                  cast('TH' AS VARCHAR)
                ) AS TEXT)
              )
          ) AS cust
            ON (
              (
                CAST((
                  sellin_fact.cust_id
                ) AS TEXT) = CAST((
                  cust.sap_cust_id
                ) AS TEXT)
              )
            )
        )
        LEFT JOIN  cmp
          ON (
            (
              CAST((
                cust.sap_cmp_id
              ) AS TEXT) = CAST((
                cmp.co_cd
              ) AS TEXT)
            )
          )
      )
      JOIN (
        SELECT DISTINCT
          dist.dstrbtr_id AS dstrbtr_grp_cd,
          LTRIM(
            CAST((
              cust.cust_num
            ) AS TEXT),
            CAST((
              CAST((
                0
              ) AS VARCHAR)
            ) AS TEXT)
          ) AS sap_soldto_code
        FROM (
          v_edw_customer_sales_dim AS cust
            LEFT JOIN itg_th_dtsdistributor AS dist
              ON (
                (
                  CAST((
                    cust."parent customer"
                  ) AS TEXT) = CAST((
                    dist.dist_nm
                  ) AS TEXT)
                )
              )
        )
        WHERE
          (
            (
              (
                CAST((
                  cust."go to model"
                ) AS TEXT) = CAST((
                  CAST('Indirect Accounts' as varchar)
                ) as text)
              )
              and (
                cast((
                  cust."sub channel"
                ) as text) = cast((
                  cast('Distributor (General)' as varchar)
                ) as text)
              )
            )
            and (
              (
                cast((
                  cust.sls_ofc_desc
                ) as text) = cast((
                  cast('General Trade' as varchar)
                ) as text)
              )
              or (
                cast((
                  cust.sls_ofc_desc
                ) as text) = cast((
                  cast('General Trade (OTC)' as varchar)
                ) as text)
              )
            )
          )
      ) as so_cust
        on (
          (
            cast((
              cust.sap_cust_id
            ) as text) = cast((
              cast((
                so_cust.sap_soldto_code
              ) as varchar)
            ) as text)
          )
        )
    )
    left join  mat
      on (
        (
          cast((
            sellin_fact.item_cd
          ) as text) = cast((
            mat.sap_matl_num
          ) as text)
        )
      )
  )
),

final as    
(
select
  sales.typ as data_type,
  cast((
    sales.bill_date
  ) as varchar) as order_date,
  sales."year",
  cast((
    sales.qrtr
  ) as varchar) as year_quarter,
  cast((
    sales.mnth_id
  ) as varchar) as month_year,
  sales.mnth_no as month_number,
  cast((
    sales.wk
  ) as varchar) as year_week_number,
  cast((
    sales.mnth_wk_no
  ) as varchar) as month_week_number,
  sales.cntry_cd as country_code,
  sales.cntry_nm as country_name,
  sales.dstrbtr_grp_cd as distributor_id,
  sales.warehse_cd as whcode,
  sales.warehse_grp as whgroup,
  sales.dstrbtr_matl_num as sku_code,
  matl.sap_mat_desc as sku_description,
  matl.gph_prod_frnchse as franchise,
  matl.gph_prod_brnd as brand,
  matl.gph_prod_vrnt as variant,
  matl.gph_prod_sgmnt as segment,
  matl.gph_prod_put_up_desc as put_up_description,
  matl.prod_sub_brand,
  matl.prod_subsegment,
  matl.prod_category,
  matl.prod_subcategory,
  cust.sap_cust_id,
  cust.sap_cust_nm,
  cust.sap_sls_org,
  cust.sap_cmp_id,
  cust.sap_cntry_cd,
  cust.sap_cntry_nm,
  cust.sap_addr,
  cust.sap_region,
  cust.sap_state_cd,
  cust.sap_city,
  cust.sap_post_cd,
  cust.sap_chnl_cd,
  cust.sap_chnl_desc,
  cust.sap_sls_office_cd,
  cust.sap_sls_office_desc,
  cust.sap_sls_grp_cd,
  cust.sap_sls_grp_desc,
  cust.sap_prnt_cust_key,
  cust.sap_prnt_cust_desc,
  cust.sap_cust_chnl_key,
  cust.sap_cust_chnl_desc,
  cust.sap_cust_sub_chnl_key,
  cust.sap_sub_chnl_desc,
  cust.sap_go_to_mdl_key,
  cust.sap_go_to_mdl_desc,
  cust.sap_bnr_key,
  cust.sap_bnr_desc,
  cust.sap_bnr_frmt_key,
  cust.sap_bnr_frmt_desc,
  cust.retail_env,
  sales.sls_qty as sales_quantity,
  sales.ret_qty as return_quantity,
  sales.grs_trd_sls as gross_trade_sales,
  sales.soh as inventory_quantity,
  sales.amt_bfr_disc as amount_before_discount,
  sales.amount_sls as inventory,
  0 as si_gross_trade_sales_value,
  0 as si_tp_value,
  0 as si_net_trade_sales_value
from (
  (
     sales
    left join  matl
      on (
        (
          upper(cast((
            sales.dstrbtr_matl_num
          ) as text)) = upper(
            ltrim(cast((
              matl.sap_matl_num
            ) as text), cast((
              cast('0' as varchar)
            ) as text))
          )
        )
      )
  )
  left join  cust
    on (
      (
        cast((
          cust.dstrbtr_grp_cd
        ) as text) = cast((
          sales.dstrbtr_grp_cd
        ) as text)
      )
    )
)
union all
select
  cast('Sellin' as varchar) as data_type,
  cast(null as varchar) as order_date,
  sellin.year_jnj as "year",
  cast((
    sellin.year_quarter_jnj
  ) as varchar) as year_quarter,
  cast((
    sellin.year_month_jnj
  ) as varchar) as month_year,
  sellin.month_number_jnj as month_number,
  cast(null as varchar) as year_week_number,
  cast(null as varchar) as month_week_number,
  cast('TH' as varchar) as country_code,
  cast('Thailand' as varchar) as country_name,
  sellin.dstrbtr_grp_cd as distributor_id,
  cast(null as varchar) as whcode,
  cast(null as varchar) as whgroup,
  sellin.item_code as sku_code,
  sellin.item_description as sku_description,
  sellin.franchise,
  sellin.brand,
  sellin.variant,
  sellin.segment,
  sellin.put_up as put_up_description,
  sellin.prod_sub_brand,
  sellin.prod_subsegment,
  sellin.prod_category,
  sellin.prod_subcategory,
  sellin.sap_cust_id,
  sellin.sap_cust_nm,
  sellin.sap_sls_org,
  sellin.sap_cmp_id,
  sellin.sap_cntry_cd,
  sellin.sap_cntry_nm,
  sellin.sap_addr,
  sellin.sap_region,
  sellin.sap_state_cd,
  sellin.sap_city,
  sellin.sap_post_cd,
  sellin.sap_chnl_cd,
  sellin.sap_chnl_desc,
  sellin.sap_sls_office_cd,
  sellin.sap_sls_office_desc,
  sellin.sap_sls_grp_cd,
  sellin.sap_sls_grp_desc,
  sellin.sap_prnt_cust_key,
  sellin.sap_prnt_cust_desc,
  sellin.sap_cust_chnl_key,
  sellin.sap_cust_chnl_desc,
  sellin.sap_cust_sub_chnl_key,
  sellin.sap_sub_chnl_desc,
  sellin.sap_go_to_mdl_key,
  sellin.go_to_model_description as sap_go_to_mdl_desc,
  sellin.sap_bnr_key,
  sellin.banner_description as sap_bnr_desc,
  sellin.sap_bnr_frmt_key,
  sellin.sap_bnr_frmt_desc,
  sellin.retail_env,
  0 as sales_quantity,
  0 as return_quantity,
  0 as gross_trade_sales,
  0 as inventory_quantity,
  0 as amount_before_discount,
  0 as inventory,
  sum(sellin.gross_trade_sales_value) as si_gross_trade_sales_value,
  sum(sellin.tp_value) as si_tp_value,
  sum(sellin.net_trade_sales_value) as si_net_trade_sales_value
from  sellin
group by
  1,
  2,
  sellin.year_jnj,
  cast((
    sellin.year_quarter_jnj
  ) as varchar),
  cast((
    sellin.year_month_jnj
  ) as varchar),
  sellin.month_number_jnj,
  7,
  8,
  9,
  10,
  sellin.dstrbtr_grp_cd,
  12,
  13,
  sellin.item_code,
  sellin.item_description,
  sellin.franchise,
  sellin.brand,
  sellin.variant,
  sellin.segment,
  sellin.put_up,
  sellin.prod_sub_brand,
  sellin.prod_subsegment,
  sellin.prod_category,
  sellin.prod_subcategory,
  sellin.sap_cust_id,
  sellin.sap_cust_nm,
  sellin.sap_sls_org,
  sellin.sap_cmp_id,
  sellin.sap_cntry_cd,
  sellin.sap_cntry_nm,
  sellin.sap_addr,
  sellin.sap_region,
  sellin.sap_state_cd,
  sellin.sap_city,
  sellin.sap_post_cd,
  sellin.sap_chnl_cd,
  sellin.sap_chnl_desc,
  sellin.sap_sls_office_cd,
  sellin.sap_sls_office_desc,
  sellin.sap_sls_grp_cd,
  sellin.sap_sls_grp_desc,
  sellin.sap_prnt_cust_key,
  sellin.sap_prnt_cust_desc,
  sellin.sap_cust_chnl_key,
  sellin.sap_cust_chnl_desc,
  sellin.sap_cust_sub_chnl_key,
  sellin.sap_sub_chnl_desc,
  sellin.sap_go_to_mdl_key,
  sellin.go_to_model_description,
  sellin.sap_bnr_key,
  sellin.banner_description,
  sellin.sap_bnr_frmt_key,
  sellin.sap_bnr_frmt_desc,
  sellin.retail_env
)



select *  from final

