with edw_vw_th_sellout_sales_foc_fact  as (
  select * from {{ ref('thaedw_integration__edw_vw_th_sellout_sales_foc_fact') }}
),
edw_vw_os_time_dim as (
  select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_vw_os_dstrbtr_customer_dim as (
  select * from {{ ref('thaedw_integration__edw_vw_os_dstrbtr_customer_dim') }}
),
edw_vw_os_dstrbtr_material_dim as (
  select * from {{ ref('thaedw_integration__edw_vw_os_dstrbtr_material_dim') }}
),
itg_th_target_distribution as (
  select * from {{ ref('thaitg_integration__itg_th_target_distribution') }}
),
itg_th_productgrouping as (
  select * from {{ ref('thaitg_integration__itg_th_productgrouping') }}
),
itg_th_target_sales as (
  select * from {{ ref('thaitg_integration__itg_th_target_sales') }}
),
edw_vw_os_customer_dim as (
  select * from {{ ref('thaedw_integration__edw_vw_th_customer_dim') }}
),
edw_vw_os_material_dim as (
  select * from {{ ref('thaedw_integration__edw_vw_th_material_dim') }}
),
so_matl as 
(
    select distinct
      edw_vw_os_dstrbtr_material_dim.sap_matl_num,
      edw_vw_os_dstrbtr_material_dim.dstrbtr_bar_cd
    from edw_vw_os_dstrbtr_material_dim
    where
      (
        cast((
          edw_vw_os_dstrbtr_material_dim.cntry_cd
        ) as text) = cast((
          cast('TH' as varchar)
        ) as text)
      )
  ) ,
 si_matl as (
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
sellin_cust as (
        select
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
        from edw_vw_os_customer_dim as sellin_cust
        where
          (
            cast((
              sellin_cust.sap_cntry_cd
            ) as text) = cast((
              cast('TH' as varchar)
            ) as text)
          )
      ),
target_sales as (
          select
            itg_th_target_sales.dstrbtr_id,
            itg_th_target_sales.sls_office,
            itg_th_target_sales.sls_grp,
            itg_th_target_sales.target,
            itg_th_target_sales.period
          from itg_th_target_sales
        ) ,
target_distribution as (
            select
              "target".dstrbtr_id,
              "target".period,
              "target".target,
              prodgroup.prod_cd
            from itg_th_target_distribution as "target", itg_th_productgrouping as prodgroup
            where
              (
                upper(cast((
                  "target".prod_nm
                ) as text)) = upper(cast((
                  prodgroup.prod_grp
                ) as text))
              )
          ) ,
sellout_cust as (
              select distinct
                edw_vw_os_dstrbtr_customer_dim.region_nm,
                edw_vw_os_dstrbtr_customer_dim.prov_nm,
                edw_vw_os_dstrbtr_customer_dim.city_nm,
                edw_vw_os_dstrbtr_customer_dim.cust_nm,
                edw_vw_os_dstrbtr_customer_dim.chnl_cd,
                edw_vw_os_dstrbtr_customer_dim.chnl_desc,
                edw_vw_os_dstrbtr_customer_dim.sls_office_cd,
                edw_vw_os_dstrbtr_customer_dim.sls_grp_cd,
                edw_vw_os_dstrbtr_customer_dim.cust_grp_cd,
                edw_vw_os_dstrbtr_customer_dim.outlet_type_cd,
                edw_vw_os_dstrbtr_customer_dim.outlet_type_desc,
                edw_vw_os_dstrbtr_customer_dim.cust_cd,
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
            ) 
,
"time" as  (
                  select distinct
                    edw_vw_os_time_dim.year,
                    edw_vw_os_time_dim.qrtr,
                    edw_vw_os_time_dim.mnth_id,
                    edw_vw_os_time_dim.mnth_no,
                    edw_vw_os_time_dim.wk,
                    edw_vw_os_time_dim.mnth_wk_no,
                    edw_vw_os_time_dim.cal_date
                  from edw_vw_os_time_dim as edw_vw_os_time_dim
                  where
                    (
                      
                        edw_vw_os_time_dim.year > (date_part(year,current_timestamp()) - 3)
                      
                      or (
                        edw_vw_os_time_dim.year > (date_part(year,current_timestamp()) - 3)
                      )
                    )
                ) ,
sales as (
  (
              select
                sales.cntry_cd,
                sales.cntry_nm,
                "time".year,
                "time".qrtr,
                "time".mnth_id,
                "time".mnth_no,
                "time".wk,
                "time".mnth_wk_no,
                "time".cal_date,
                sales.bill_date,
                sales.order_no,
                sales.iscancel,
                sales.grp_cd,
                sales.prom_cd1,
                sales.prom_cd2,
                sales.prom_cd3,
                sales.dstrbtr_grp_cd,
                sales.dstrbtr_matl_num,
                sales.cust_cd,
                sales.slsmn_nm,
                sales.slsmn_cd,
                sales.cn_reason_cd,
                sales.cn_reason_desc,
                sales.grs_prc,
                sales.grs_trd_sls,
                sales.cn_dmgd_gds,
                sales.crdt_nt_amt,
                sales.trd_discnt_item_lvl,
                sales.trd_discnt_bill_lvl,
                sales.sls_qty,
                sales.ret_qty,
                sales.quantity_dz,
                sales.net_trd_sls,
                sales.tot_bf_discount,
                sales.product_name2
              from (
                (
                  select
                    edw_vw_th_sellout_sales_foc_fact.cntry_cd,
                    edw_vw_th_sellout_sales_foc_fact.cntry_nm,
                    edw_vw_th_sellout_sales_foc_fact.bill_date,
                    edw_vw_th_sellout_sales_foc_fact.order_no,
                    edw_vw_th_sellout_sales_foc_fact.product_name2,
                    edw_vw_th_sellout_sales_foc_fact.iscancel,
                    edw_vw_th_sellout_sales_foc_fact.grp_cd,
                    edw_vw_th_sellout_sales_foc_fact.prom_cd1,
                    edw_vw_th_sellout_sales_foc_fact.prom_cd2,
                    edw_vw_th_sellout_sales_foc_fact.prom_cd3,
                    edw_vw_th_sellout_sales_foc_fact.dstrbtr_grp_cd,
                    edw_vw_th_sellout_sales_foc_fact.dstrbtr_matl_num,
                    edw_vw_th_sellout_sales_foc_fact.cust_cd,
                    edw_vw_th_sellout_sales_foc_fact.slsmn_nm,
                    edw_vw_th_sellout_sales_foc_fact.slsmn_cd,
                    edw_vw_th_sellout_sales_foc_fact.cn_reason_cd,
                    edw_vw_th_sellout_sales_foc_fact.cn_reason_desc,
                    edw_vw_th_sellout_sales_foc_fact.grs_prc,
                    sum(
                      case
                        when (
                          (
                            edw_vw_th_sellout_sales_foc_fact.cn_reason_cd is null
                          )
                          or (
                            left(cast((
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                            ) as text), 1) <> cast((
                              cast('N' as varchar)
                            ) as text)
                          )
                        )
                        then cast((
                          (
                            edw_vw_th_sellout_sales_foc_fact.grs_trd_sls + edw_vw_th_sellout_sales_foc_fact.ret_val
                          )
                        ) as double)
                        else cast(null as double)
                      end
                    ) as grs_trd_sls,
                    sum(
                      case
                        when (
                          (
                            edw_vw_th_sellout_sales_foc_fact.cn_reason_cd is null
                          )
                          or (
                            cast((
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                            ) as text) = cast((
                              cast('' as varchar)
                            ) as text)
                          )
                        )
                        then cast((
                          0.0
                        ) as double)
                        when (
                          
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                            like 'D%' 
                          )
                        
                        THEN cast((
                          0.0
                        ) AS DOUBLE)
                        WHEN (
                         
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                            like 'N%' 
                          )
                        
                        then cast((
                          edw_vw_th_sellout_sales_foc_fact.net_trd_sls
                        ) as double)
                        else cast(null as double)
                      end
                    ) as cn_dmgd_gds,
                    sum(
                      case
                        when (
                          
                            (
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd is null
                            )
                            or (
                              cast((
                                edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                              ) as text) = cast((
                                cast('' as varchar)
                              ) as text)
                            )
                          )
                          and (
                            cast((
                              edw_vw_th_sellout_sales_foc_fact.net_trd_sls
                            ) as double) < cast((
                              0
                            ) as double)
                          )
                        
                        then cast((
                          edw_vw_th_sellout_sales_foc_fact.net_trd_sls
                        ) as double)
                        when
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                            like 'D%' 
                        
                        then cast((
                          edw_vw_th_sellout_sales_foc_fact.net_trd_sls
                        ) as double)
                        when (
                          
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                            like 'N%'
                          )
                        
                        then cast((
                          0.0
                        ) as double)
                        else cast(null as double)
                      end
                    ) as crdt_nt_amt,
                    sum(edw_vw_th_sellout_sales_foc_fact.trd_discnt_item_lvl) as trd_discnt_item_lvl,
                    sum(edw_vw_th_sellout_sales_foc_fact.trd_discnt_bill_lvl) as trd_discnt_bill_lvl,
                    sum(edw_vw_th_sellout_sales_foc_fact.ret_val) as ret_val,
                    sum(edw_vw_th_sellout_sales_foc_fact.sls_qty) as sls_qty,
                    sum(edw_vw_th_sellout_sales_foc_fact.ret_qty) as ret_qty,
                    sum(
                      case
                        when (
                          (
                            edw_vw_th_sellout_sales_foc_fact.cn_reason_cd is null
                          )
                          or (
                            left(cast((
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                            ) as text), 1) <> cast((
                              cast('N' as varchar)
                            ) as text)
                          )
                        )
                        then cast((
                          edw_vw_th_sellout_sales_foc_fact.sls_qty
                        ) as double)
                        else cast(null as double)
                      end
                    ) as quantity_dz,
                    sum(edw_vw_th_sellout_sales_foc_fact.net_trd_sls) as net_trd_sls,
                    sum(
                      case
                        when (
                          (
                            edw_vw_th_sellout_sales_foc_fact.cn_reason_cd is null
                          )
                          or (
                            cast((
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                            ) as text) = cast((
                              cast('' as varchar)
                            ) as text)
                          )
                        )
                        then cast((
                          edw_vw_th_sellout_sales_foc_fact.tot_bf_discount
                        ) as double)
                        when (
                          
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                            like 'D%' 
                          
                        )
                        then cast((
                          edw_vw_th_sellout_sales_foc_fact.tot_bf_discount
                        ) as double)
                        when (
                          
                              edw_vw_th_sellout_sales_foc_fact.cn_reason_cd
                            like 'N%' 
                          )
                        
                        then cast((
                          0.0
                        ) as double)
                        else cast(null as double)
                      end
                    ) as tot_bf_discount
                  from edw_vw_th_sellout_sales_foc_fact
                  where
                    (
                      cast((
                        edw_vw_th_sellout_sales_foc_fact.cntry_cd
                      ) as text) = cast((
                        cast('TH' as varchar)
                      ) as text)
                    )
                  group by
                    edw_vw_th_sellout_sales_foc_fact.cntry_cd,
                    edw_vw_th_sellout_sales_foc_fact.cntry_nm,
                    edw_vw_th_sellout_sales_foc_fact.bill_date,
                    edw_vw_th_sellout_sales_foc_fact.order_no,
                    edw_vw_th_sellout_sales_foc_fact.product_name2,
                    edw_vw_th_sellout_sales_foc_fact.iscancel,
                    edw_vw_th_sellout_sales_foc_fact.grp_cd,
                    edw_vw_th_sellout_sales_foc_fact.prom_cd1,
                    edw_vw_th_sellout_sales_foc_fact.prom_cd2,
                    edw_vw_th_sellout_sales_foc_fact.prom_cd3,
                    edw_vw_th_sellout_sales_foc_fact.dstrbtr_grp_cd,
                    edw_vw_th_sellout_sales_foc_fact.dstrbtr_matl_num,
                    edw_vw_th_sellout_sales_foc_fact.cust_cd,
                    edw_vw_th_sellout_sales_foc_fact.slsmn_nm,
                    edw_vw_th_sellout_sales_foc_fact.slsmn_cd,
                    edw_vw_th_sellout_sales_foc_fact.cn_reason_cd,
                    edw_vw_th_sellout_sales_foc_fact.cn_reason_desc,
                    edw_vw_th_sellout_sales_foc_fact.grs_prc
                ) as sales
                join "time"
                  on (
                    (
                      sales.bill_date = cast((
                        "time".cal_date
                      ) as timestampntz)
                    )
                  )
              )
            ) 
),
final as (
select
  sales.bill_date as order_date,
  sales.order_no,
  sales.iscancel,
  sales.grp_cd,
  sales.prom_cd1,
  sales.prom_cd2,
  sales.prom_cd3,
  sales.year,
  sales.qrtr as year_quarter,
  sales.mnth_id as month_year,
  sales.mnth_no as month_number,
  sales.wk as year_week_number,
  sales.mnth_wk_no as month_week_number,
  sales.cntry_cd as country_code,
  sales.cntry_nm as country_name,
  sales.dstrbtr_grp_cd as distributor_id,
  sellout_cust.region_nm as region_desc,
  sellout_cust.prov_nm as city,
  sellout_cust.city_nm as district,
  sales.cust_cd as ar_code,
  sellout_cust.cust_nm as ar_name,
  sellout_cust.chnl_cd as channel_code,
  sellout_cust.chnl_desc as channel,
  sellout_cust.sls_office_cd as sales_office_code,
  case
    when (
      substring(cast((
        sellout_cust.sls_grp_cd
      ) as text), 2, 1) = cast((
        cast('1' as varchar)
      ) as text)
    )
    then (
      cast((
        sales.dstrbtr_grp_cd
      ) as text) || cast((
        cast(' Van' as varchar)
      ) as text)
    )
    else (
      cast((
        sales.dstrbtr_grp_cd
      ) as text) || cast((
        cast(' Credit' as varchar)
      ) as text)
    )
  end as sales_office_name,
  sellout_cust.sls_grp_cd as sales_group,
  sellout_cust.cust_grp_cd as "cluster",
  sellout_cust.outlet_type_cd as ar_type_code,
  sellout_cust.outlet_type_desc as ar_type_name,
  sellin_cust.sap_cust_nm as distributor_name,
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
  sellin_cust.retail_env,
  sales.dstrbtr_matl_num as sku_code,
  case
    when (
      si_matl.sap_mat_desc is null
    )
    then sales.product_name2
    else si_matl.sap_mat_desc
  end as sku_description,
  so_matl.dstrbtr_bar_cd as bar_code,
  si_matl.gph_prod_frnchse as franchise,
  si_matl.gph_prod_brnd as brand,
  si_matl.gph_prod_vrnt as variant,
  si_matl.gph_prod_sgmnt as segment,
  si_matl.gph_prod_put_up_desc as put_up_description,
  si_matl.prod_sub_brand,
  si_matl.prod_subsegment,
  si_matl.prod_category,
  si_matl.prod_subcategory,
  sales.slsmn_nm as salesman_name,
  sales.slsmn_cd as salesman_code,
  sales.cn_reason_cd as cn_reason_code,
  sales.cn_reason_desc as cn_reason_description,
  sales.grs_prc as price,
  sales.grs_trd_sls as gross_trade_sales,
  sales.cn_dmgd_gds as cn_damaged_goods,
  sales.crdt_nt_amt as credit_note_amount,
  sales.trd_discnt_item_lvl as line_discount,
  sales.trd_discnt_bill_lvl as bottom_line_discount,
  sales.sls_qty as sales_quantity,
  sales.ret_qty as return_quantity,
  sales.quantity_dz,
  sales.net_trd_sls as net_invoice,
  sales.tot_bf_discount,
  target_distribution.target as target_calls,
  target_sales.target as target_sales
from 
            sales
            left join  sellout_cust
              on (
                (
                  (
                    upper(cast((
                      sales.cust_cd
                    ) as text)) = upper(cast((
                      sellout_cust.cust_cd
                    ) as text))
                  )
                  and (
                    upper(cast((
                      sales.dstrbtr_grp_cd
                    ) as text)) = upper(cast((
                      sellout_cust.dstrbtr_grp_cd
                    ) as text))
                  )
                )
              )
          
          left join  target_distribution
            on (
              (
                (
                  (
                    upper(cast((
                      sales.dstrbtr_matl_num
                    ) as text)) = upper(cast((
                      target_distribution.prod_cd
                    ) as text))
                  )
                  and (
                    upper(cast((
                      sales.dstrbtr_grp_cd
                    ) as text)) = upper(cast((
                      target_distribution.dstrbtr_id
                    ) as text))
                  )
                )
                and (
                  (
                    substring(cast((
                      cast((
                        sales.bill_date
                      ) as varchar)
                    ) as text), 1, 4) || substring(cast((
                      cast((
                        sales.bill_date
                      ) as varchar)
                    ) as text), 6, 2)
                  ) = cast((
                    target_distribution.period
                  ) as text)
                )
              )
            )
        
        left join  target_sales
          on (
            (
              (
                (
                  (
                    cast((
                      sales.dstrbtr_grp_cd
                    ) as text) = cast((
                      target_sales.dstrbtr_id
                    ) as text)
                  )
                  and (
                    (
                      substring(cast((
                        cast((
                          sales.bill_date
                        ) as varchar)
                      ) as text), 1, 4) || substring(cast((
                        cast((
                          sales.bill_date
                        ) as varchar)
                      ) as text), 6, 2)
                    ) = cast((
                      target_sales.period
                    ) as text)
                  )
                )
                and (
                  upper(cast((
                    sellout_cust.sls_office_cd
                  ) as text)) = upper(cast((
                    target_sales.sls_office
                  ) as text))
                )
              )
              and (
                upper(cast((
                  sellout_cust.sls_grp_cd
                ) as text)) = upper(cast((
                  target_sales.sls_grp
                ) as text))
              )
            )
          )
      
      left join  sellin_cust
        on (
          (
            upper(cast((
              sellout_cust.sap_soldto_code
            ) as text)) = upper(cast((
              sellin_cust.sap_cust_id
            ) as text))
          )
        )
    
    left join si_matl
      on (
        (
          upper(cast((
            sales.dstrbtr_matl_num
          ) as text)) = upper(
            ltrim(cast((
              si_matl.sap_matl_num
            ) as text), cast((
              cast('0' as varchar)
            ) as text))
          )
        )
      )
  
  left join  so_matl
    on (
      (
        cast((
          si_matl.sap_matl_num
        ) as text) = cast((
          so_matl.sap_matl_num
        ) as text)
      )
    )


    )

select * from final 
