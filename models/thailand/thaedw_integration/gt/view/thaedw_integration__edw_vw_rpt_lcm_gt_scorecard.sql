with itg_la_gt_sales_order_fact as (
  select * from dev_dna_core.snaposeitg_integration.itg_la_gt_sales_order_fact 
),
itg_la_gt_visit as (
  select * from dev_dna_core.snaposeitg_integration.itg_la_gt_visit
),
itg_la_gt_customer as (
  select * from dev_dna_core.snaposeitg_integration.itg_la_gt_customer
),
itg_th_dtscustgroup as (
  select * from dev_dna_core.snaposeitg_integration.itg_th_dtscustgroup
),
itg_th_dstrbtr_material_dim as (
select * from dev_dna_core.snaposeitg_integration.itg_th_dstrbtr_material_dim
),
itg_la_gt_sellout_fact as (
  select * from dev_dna_core.snaposeitg_integration.itg_la_gt_sellout_fact
),
itg_la_gt_schedule as (
select * from dev_dna_core.snaposeitg_integration.itg_la_gt_schedule
),
edw_vw_la_gt_route as (
select * from dev_dna_core.snaposeedw_integration.edw_vw_la_gt_route
),
itg_la_gt_customer_snapshot as (
select * from dev_dna_core.snaposeitg_integration.itg_la_gt_customer_snapshot
),
itg_mds_lcm_distributor_target_sales as (
select * from dev_dna_core.snaposeitg_integration.itg_mds_lcm_distributor_target_sales
),
itg_mds_lcm_distributor_target_sales_re as (
select * from dev_dna_core.snaposeitg_integration.itg_mds_lcm_distributor_target_sales_re
),
itg_cbd_gt_sales_report_fact as (
select * from dev_dna_core.snaposeitg_integration.itg_cbd_gt_sales_report_fact
),
itg_mds_th_cbd_item_master as (
select * from dev_dna_core.snaposeitg_integration.itg_mds_th_cbd_item_master
),
itg_mds_th_lcm_exchange_rate as (
select * from dev_dna_core.snaposeitg_integration.itg_mds_th_lcm_exchange_rate
),
itg_cbd_gt_customer_snapshot as (
select * from dev_dna_core.snaposeitg_integration.itg_cbd_gt_customer_snapshot
),
itg_cbd_gt_customer as (
select * from dev_dna_core.snaposeitg_integration.itg_cbd_gt_customer
),
itg_mym_gt_sales_report_fact as (
select * from dev_dna_core.snaposeitg_integration.itg_mym_gt_sales_report_fact
),
itg_mds_th_mym_product_master as (
select *  from dev_dna_core.snaposeitg_integration.itg_mds_th_mym_product_master
),
itg_mds_th_myanmar_customer_master as (
select * from dev_dna_core.snaposeitg_integration.itg_mds_th_myanmar_customer_master
),
itg_mym_gt_customer_snapshot as (
select * from dev_dna_core.snaposeitg_integration.itg_mym_gt_customer_snapshot
),
itg_mym_gt_customer as (
select * from dev_dna_core.snaposeitg_integration.itg_mym_gt_customer
),
final as (

(
  (
    (
      (
        (
          (
            (
              select
                'ACTUAL' as identifier,
                'LA' as cntry_cd,
                'LAK' as crncy_cd,
                'Laos' as cntry_nm,
                cast((
                  cal.cal_mnth_id
                ) as varchar) as year_month,
                cal.cal_year as "year",
                cal.cal_mnth_no as "month",
                sls.distributorid as distributor_id,
                coalesce(cust_grp.grp_nm, cast('N/A' as varchar)) as retail_env,
                cast((
                  (
                    (
                      cast((
                        coalesce(sls.salegroup, cast('N/A' as varchar))
                      ) as text) || cast((
                        cast('-' as varchar)
                      ) as text)
                    ) || cast((
                      coalesce(cust.salesareaname, cast('N/A' as varchar))
                    ) as text)
                  )
                ) as varchar) as salesarea,
                sum(
                  case
                    when (
                      (
                        (
                          upper(substring(cast((
                            sls.cnreasoncode
                          ) as text), 1, 1)) = cast((
                            cast('D' as varchar)
                          ) as text)
                        )
                        OR (
                          sls.cnreasoncode IS NULL
                        )
                      )
                      OR (
                        cast((
                          sls.cnreasoncode
                        ) as text) = cast((
                          cast('' as varchar)
                        ) as text)
                      )
                    )
                    then sls.total
                    else cast((
                      cast((
                        0
                      ) as decimal)
                    ) as decimal(18, 0))
                  end
                ) as sell_out,
                sum(
                  case
                    when (
                      (
                        sls.cnreasoncode IS NULL
                      )
                      OR (
                        left(cast((
                          sls.cnreasoncode
                        ) as text), 1) <> cast((
                          cast('N' as varchar)
                        ) as text)
                      )
                    )
                    then (
                      sls.qty * matl_dim.sls_prc_credit
                    )
                    else cast((
                      cast((
                        0
                      ) as decimal)
                    ) as decimal(18, 0))
                  end
                ) as gross_sell_out,
                sum(sls.total) as net_sell_out,
                0 as sellout_target,
                '0' as planned_call_count,
                '0' as visited_call_count,
                '0' as effective_call_count,
                '0' as coverage_stores_count,
                '0' as reactivate_stores_count,
                '0' as inactive_stores_count,
                '0' as sales_order_count,
                '0' as on_time_count,
                '0' as in_full_count,
                '0' as otif_count
              from (
                (
                  (
                    (
                      (
                        select
                          st.distributorid,
                          st.orderno,
                          st.orderdate,
                          st.arcode,
                          st.arname,
                          st.city,
                          st.region,
                          st.saledistrict,
                          st.saleoffice,
                          st.salegroup,
                          st.artypecode,
                          st.saleemployee,
                          st.salename,
                          st.productcode,
                          st.productdesc,
                          st.megabrand,
                          st.brand,
                          st.baseproduct,
                          st.variant,
                          st.putup,
                          st.grossprice,
                          st.qty,
                          st.subamt1,
                          st.discount,
                          st.subamt2,
                          st.discountbtline,
                          st.totalbeforevat,
                          st.total,
                          st.linenumber,
                          st.iscancel,
                          st.cndocno,
                          st.cnreasoncode,
                          st.promotionheader1,
                          st.promotionheader2,
                          st.promotionheader3,
                          st.promodesc1,
                          st.promodesc2,
                          st.promodesc3,
                          st.promocode1,
                          st.promocode2,
                          st.promocode3,
                          st.avgdiscount,
                          st.filename,
                          st.run_id,
                          st.crt_dttm,
                          st.updt_dttm,
                          row_number() over (partition by st.distributorid, st.orderno, st.productcode, st.arcode, st.linenumber order by st.orderdate desc) as rn
                        from itg_la_gt_sellout_fact as st
                      ) as sls
                      left join edw_vw_os_time_dim as cal
                        on (
                          (
                            cast((
                              sls.orderdate
                            ) as timestampntz) = cast((
                              cal.cal_date
                            ) as timestampntz)
                          )
                        )
                    )
                    left join itg_th_dtscustgroup as cust_grp
                      on (
                        (
                          upper(cast((
                            cust_grp.ar_typ_cd
                          ) as text)) = upper(cast((
                            sls.artypecode
                          ) as text))
                        )
                      )
                  )
                  left join itg_th_dstrbtr_material_dim as matl_dim
                    on (
                      (
                        Ltrim(
                          cast((
                            sls.productcode
                          ) as text),
                          cast((
                            cast((
                              0
                            ) as varchar)
                          ) as text)
                        ) = Ltrim(
                          cast((
                            matl_dim.item_cd
                          ) as text),
                          cast((
                            cast((
                              0
                            ) as varchar)
                          ) as text)
                        )
                      )
                    )
                )
                left join itg_la_gt_customer as cust
                  on (
                    (
                      (
                        cast((
                          cust.arcode
                        ) as text) = cast((
                          sls.arcode
                        ) as text)
                      )
                      and (
                        cast((
                          cust.distributorid
                        ) as text) = cast((
                          sls.distributorid
                        ) as text)
                      )
                    )
                  )
              )
              where
                (
                  sls.rn = cast((
                    1
                  ) as bigint)
                )
              group by
                cal.cal_mnth_id,
                cal.cal_year,
                cal.cal_mnth_no,
                sls.distributorid,
                cust_grp.grp_nm,
                sls.salegroup,
                cust.salesareaname,
                sls.qty,
                matl_dim.sls_prc_credit
              union all
              select
                'ACTUAL' as identifier,
                'LA' as cntry_cd,
                'LAK' as crncy_cd,
                'Laos' as cntry_nm,
                cast((
                  cal.cal_mnth_id
                ) as varchar) as year_month,
                cal.cal_year as "year",
                cal.cal_mnth_no as "month",
                pc.distributor_id,
                coalesce(pc.retail_env, cast('N/A' as varchar)) as retail_env,
                cast((
                  coalesce(pc.salesarea, cast((
                    cast('N/A' as varchar)
                  ) as text))
                ) as varchar) as salesarea,
                0 as sell_out,
                0 as gross_sell_out,
                0 as net_sell_out,
                '0' as sellout_target,
                count(pc.planned_call) as planned_call_count,
                count(vc.visited_call) as visited_call_count,
                count(ec.effective_call) as effective_call_count,
                '0' as coverage_stores_count,
                '0' as reactivate_stores_count,
                '0' as inactive_stores_count,
                '0' as sales_order_count,
                '0' as on_time_count,
                '0' as in_full_count,
                '0' as otif_count
              from (
                (
                  (
                    (
                      select
                        sch.saleunit as distributor_id,
                        sch.employee_id as sales_rep_id,
                        rd.store_id,
                        cust_dim.grp_nm as retail_env,
                        (
                          (
                            cast((
                              coalesce(cust_dim.salegroup, cast('N/A' as varchar))
                            ) as text) || cast((
                              cast('-' as varchar)
                            ) as text)
                          ) || cast((
                            coalesce(cust_dim.salesareaname, cast('N/A' as varchar))
                          ) as text)
                        ) as salesarea,
                        sch.schedule_date as planned_call
                      from (
                        (
                          itg_la_gt_schedule as sch
                            join (
                              select
                                edw_vw_la_gt_route.distributor_id,
                                edw_vw_la_gt_route.sales_rep_id,
                                edw_vw_la_gt_route.route_id,
                                edw_vw_la_gt_route.store_id,
                                edw_vw_la_gt_route.effective_start_date,
                                edw_vw_la_gt_route.effective_end_date
                              from edw_vw_la_gt_route
                              where
                                (
                                  upper(cast((
                                    edw_vw_la_gt_route.valid_flag
                                  ) as text)) = cast((
                                    cast('Y' as varchar)
                                  ) as text)
                                )
                            ) as rd
                              on (
                                (
                                  (
                                    (
                                      (
                                        (
                                          cast((
                                            sch.route_id
                                          ) as text) = cast((
                                            rd.route_id
                                          ) as text)
                                        )
                                        and (
                                          upper(cast((
                                            sch.saleunit
                                          ) as text)) = upper(cast((
                                            rd.distributor_id
                                          ) as text))
                                        )
                                      )
                                      and (
                                        upper(cast((
                                          sch.employee_id
                                        ) as text)) = upper(cast((
                                          rd.sales_rep_id
                                        ) as text))
                                      )
                                    )
                                    and (
                                      cast((
                                        sch.schedule_date
                                      ) as timestampntz) >= cast((
                                        rd.effective_start_date
                                      ) as timestampntz)
                                    )
                                  )
                                  and (
                                    cast((
                                      sch.schedule_date
                                    ) as timestampntz) <= cast((
                                      rd.effective_end_date
                                    ) as timestampntz)
                                  )
                                )
                              )
                        )
                        left join (
                          select distinct
                            itg_la_gt_customer.distributorid,
                            itg_la_gt_customer.arcode as customer_id,
                            itg_la_gt_customer.artypecode,
                            itg_la_gt_customer.saleemployee as sales_rep_id,
                            cust_grp.grp_nm,
                            itg_la_gt_customer.salegroup,
                            itg_la_gt_customer.salesareaname
                          from (
                            itg_la_gt_customer
                              left join itg_th_dtscustgroup as cust_grp
                                on (
                                  (
                                    upper(cast((
                                      cust_grp.ar_typ_cd
                                    ) as text)) = upper(cast((
                                      itg_la_gt_customer.artypecode
                                    ) as text))
                                  )
                                )
                          )
                        ) as cust_dim
                          on (
                            (
                              (
                                cast((
                                  cust_dim.customer_id
                                ) as text) = cast((
                                  rd.store_id
                                ) as text)
                              )
                              and (
                                cast((
                                  cust_dim.distributorid
                                ) as text) = cast((
                                  sch.saleunit
                                ) as text)
                              )
                            )
                          )
                      )
                      where
                        (
                          (
                            cast((
                              sch.approved
                            ) as text) = cast((
                              cast('Y' as varchar)
                            ) as text)
                          )
                          and (
                            sch.schedule_date <= CONVERT_TIMEZONE(
                              cast((
                                cast('Asia/Bangkok' as varchar)
                              ) as text),
                              cast((
                                cast((
                                  cast(current_timestamp() as varchar)
                                ) as timestampntz)
                              ) as timestampntz)
                            )
                          )
                        )
                    ) as pc
                    left join edw_vw_os_time_dim as cal
                      on (
                        (
                          cast((
                            pc.planned_call
                          ) as timestampntz) = cast((
                            cal.cal_date
                          ) as timestampntz)
                        )
                      )
                  )
                  left join (
                    select distinct
                      itg_la_gt_visit.saleunit as distributor_id,
                      itg_la_gt_visit.id_sale as sales_rep_id,
                      itg_la_gt_visit.id_customer as store_id,
                      itg_la_gt_visit.date_visi as visited_call
                    from itg_la_gt_visit
                    where
                      (
                        (
                          (
                            cast((
                              itg_la_gt_visit.time_plan
                            ) as text) <> cast((
                              cast('88888' as varchar)
                            ) as text)
                          )
                          and (
                            cast((
                              itg_la_gt_visit.time_plan
                            ) as text) <> cast((
                              cast('99999' as varchar)
                            ) as text)
                          )
                        )
                        and (
                          NOT itg_la_gt_visit.date_visi IS NULL
                        )
                      )
                  ) as vc
                    on (
                      (
                        (
                          (
                            (
                              upper(cast((
                                pc.distributor_id
                              ) as text)) = upper(cast((
                                vc.distributor_id
                              ) as text))
                            )
                            and (
                              upper(cast((
                                pc.sales_rep_id
                              ) as text)) = upper(cast((
                                vc.sales_rep_id
                              ) as text))
                            )
                          )
                          and (
                            upper(cast((
                              pc.store_id
                            ) as text)) = upper(cast((
                              vc.store_id
                            ) as text))
                          )
                        )
                        and (
                          upper(cast((
                            cast((
                              pc.planned_call
                            ) as varchar)
                          ) as text)) = upper(cast((
                            cast((
                              vc.visited_call
                            ) as varchar)
                          ) as text))
                        )
                      )
                    )
                )
                left join (
                  select distinct
                    fact.saleunit as distributor_id,
                    fact.id_sale as sales_rep_id,
                    fact.id_customer as store_id,
                    fact.date_visi as effective_call
                  from (
                    itg_la_gt_visit as fact
                      join (
                        select
                          itg_la_gt_sales_order_fact.saleunit,
                          itg_la_gt_sales_order_fact.orderid,
                          itg_la_gt_sales_order_fact.orderdate,
                          itg_la_gt_sales_order_fact.customer_id,
                          itg_la_gt_sales_order_fact.customer_name,
                          itg_la_gt_sales_order_fact.city,
                          itg_la_gt_sales_order_fact.region,
                          itg_la_gt_sales_order_fact.saledistrict,
                          itg_la_gt_sales_order_fact.saleoffice,
                          itg_la_gt_sales_order_fact.salegroup,
                          itg_la_gt_sales_order_fact.customertype,
                          itg_la_gt_sales_order_fact.storetype,
                          itg_la_gt_sales_order_fact.saletype,
                          itg_la_gt_sales_order_fact.salesemployee,
                          itg_la_gt_sales_order_fact.salename,
                          itg_la_gt_sales_order_fact.productid,
                          itg_la_gt_sales_order_fact.productname,
                          itg_la_gt_sales_order_fact.megabrand,
                          itg_la_gt_sales_order_fact.brand,
                          itg_la_gt_sales_order_fact.baseproduct,
                          itg_la_gt_sales_order_fact.variant,
                          itg_la_gt_sales_order_fact.putup,
                          itg_la_gt_sales_order_fact.priceref,
                          itg_la_gt_sales_order_fact.backlog,
                          itg_la_gt_sales_order_fact.qty,
                          itg_la_gt_sales_order_fact.subamt1,
                          itg_la_gt_sales_order_fact.discount,
                          itg_la_gt_sales_order_fact.subamt2,
                          itg_la_gt_sales_order_fact.discountbtline,
                          itg_la_gt_sales_order_fact.totalbeforevat,
                          itg_la_gt_sales_order_fact.total,
                          itg_la_gt_sales_order_fact.sales_order_line_no,
                          itg_la_gt_sales_order_fact.canceled,
                          itg_la_gt_sales_order_fact.documentid,
                          itg_la_gt_sales_order_fact.return_reason,
                          itg_la_gt_sales_order_fact.promotioncode,
                          itg_la_gt_sales_order_fact.promotioncode1,
                          itg_la_gt_sales_order_fact.promotioncode2,
                          itg_la_gt_sales_order_fact.promotioncode3,
                          itg_la_gt_sales_order_fact.promotioncode4,
                          itg_la_gt_sales_order_fact.promotioncode5,
                          itg_la_gt_sales_order_fact.promotion_code,
                          itg_la_gt_sales_order_fact.promotion_code2,
                          itg_la_gt_sales_order_fact.promotion_code3,
                          itg_la_gt_sales_order_fact.avgdiscount,
                          itg_la_gt_sales_order_fact.ordertype,
                          itg_la_gt_sales_order_fact.approverstatus,
                          itg_la_gt_sales_order_fact.pricelevel,
                          itg_la_gt_sales_order_fact.optional3,
                          itg_la_gt_sales_order_fact.deliverydate,
                          itg_la_gt_sales_order_fact.ordertime,
                          itg_la_gt_sales_order_fact.shipto,
                          itg_la_gt_sales_order_fact.billto,
                          itg_la_gt_sales_order_fact.deliveryrouteid,
                          itg_la_gt_sales_order_fact.approved_date,
                          itg_la_gt_sales_order_fact.approved_time,
                          itg_la_gt_sales_order_fact.ref_15,
                          itg_la_gt_sales_order_fact.paymenttype,
                          itg_la_gt_sales_order_fact.load_flag,
                          itg_la_gt_sales_order_fact.filename,
                          itg_la_gt_sales_order_fact.run_id,
                          itg_la_gt_sales_order_fact.crt_dttm,
                          itg_la_gt_sales_order_fact.updt_dttm
                        from itg_la_gt_sales_order_fact
                        where
                          (
                            (
                              NOT trim(cast((
                                itg_la_gt_sales_order_fact.ref_15
                              ) as text)) IS NULL
                            )
                            OR (
                              trim(cast((
                                itg_la_gt_sales_order_fact.ref_15
                              ) as text)) <> cast((
                                cast('' as varchar)
                              ) as text)
                            )
                          )
                      ) as so
                        on (
                          (
                            (
                              (
                                (
                                  upper(cast((
                                    fact.saleunit
                                  ) as text)) = upper(trim(cast((
                                    so.saleunit
                                  ) as text)))
                                )
                                and (
                                  upper(cast((
                                    fact.id_customer
                                  ) as text)) = upper(trim(cast((
                                    so.customer_id
                                  ) as text)))
                                )
                              )
                              and (
                                upper(right(cast((
                                  fact.id_sale
                                ) as text), 5)) = upper(trim(cast((
                                  so.salesemployee
                                ) as text)))
                              )
                            )
                            and (
                              cast((
                                fact.date_visi
                              ) as timestampntz) = cast((
                                so.orderdate
                              ) as timestampntz)
                            )
                          )
                        )
                  )
                  where
                    (
                      (
                        (
                          cast((
                            fact.time_plan
                          ) as text) <> cast((
                            cast('88888' as varchar)
                          ) as text)
                        )
                        and (
                          cast((
                            fact.time_plan
                          ) as text) <> cast((
                            cast('99999' as varchar)
                          ) as text)
                        )
                      )
                      and (
                        NOT fact.date_visi IS NULL
                      )
                    )
                ) as ec
                  on (
                    (
                      (
                        (
                          (
                            upper(cast((
                              pc.distributor_id
                            ) as text)) = upper(cast((
                              ec.distributor_id
                            ) as text))
                          )
                          and (
                            upper(cast((
                              pc.store_id
                            ) as text)) = upper(cast((
                              ec.store_id
                            ) as text))
                          )
                        )
                        and (
                          upper(cast((
                            pc.sales_rep_id
                          ) as text)) = upper(cast((
                            ec.sales_rep_id
                          ) as text))
                        )
                      )
                      and (
                        cast((
                          pc.planned_call
                        ) as timestampntz) = cast((
                          ec.effective_call
                        ) as timestampntz)
                      )
                    )
                  )
              )
              group by
                cal.cal_mnth_id,
                cal.cal_year,
                cal.cal_mnth_no,
                pc.distributor_id,
                pc.retail_env,
                pc.salesarea
            )
            union all
            select
              'ACTUAL' as identifier,
              'LA' as cntry_cd,
              'LAK' as crncy_cd,
              'Laos' as cntry_nm,
              cast((
                cal.cal_mnth_id
              ) as varchar) as year_month,
              cal.cal_year as "year",
              cal.cal_mnth_no as "month",
              cust.distributorid as distributor_id,
              coalesce(cust_grp.grp_nm, cast('N/A' as varchar)) as retail_env,
              cast((
                (
                  (
                    cast((
                      coalesce(cust.salegroup, cast('N/A' as varchar))
                    ) as text) || cast((
                      cast('-' as varchar)
                    ) as text)
                  ) || cast((
                    coalesce(cust.salesareaname, cast('N/A' as varchar))
                  ) as text)
                )
              ) as varchar) as salesarea,
              '0' as sell_out,
              '0' as gross_sell_out,
              '0' as net_sell_out,
              '0' as sellout_target,
              '0' as planned_call_count,
              '0' as visited_call_count,
              '0' as effective_call_count,
              count(*) as coverage_stores_count,
              '0' as reactivate_stores_count,
              '0' as inactive_stores_count,
              '0' as sales_order_count,
              '0' as on_time_count,
              '0' as in_full_count,
              '0' as otif_count
            from (
              (
                itg_la_gt_customer_snapshot as cust
                  left join edw_vw_os_time_dim as cal
                    on (
                      (
                        cust.snapshot_date::date = cal.cal_date::date
                      )
                    )
              )
              left join itg_th_dtscustgroup as cust_grp
                on (
                  (
                    upper(cast((
                      cust_grp.ar_typ_cd
                    ) as text)) = upper(cast((
                      cust.artypecode
                    ) as text))
                  )
                )
            )
            where
              (
                trim(cast((
                  cast((
                    cust.activestatus
                  ) as varchar)
                ) as text)) = cast((
                  cast('1' as varchar)
                ) as text)
              )
            group by
              cal.cal_mnth_id,
              cal.cal_year,
              cal.cal_mnth_no,
              cust.distributorid,
              coalesce(cust_grp.grp_nm, cast('N/A' as varchar)),
              cust.salegroup,
              cust.salesareaname
          )
          union all
          select
            'ACTUAL' as identifier,
            'LA' as cntry_cd,
            'LAK' as crncy_cd,
            'Laos' as cntry_nm,
            cast((
              stores.year_month
            ) as varchar) as year_month,
            cal.cal_year as "year",
            cal.cal_mnth_no as "month",
            stores.distributor_id,
            coalesce(cust_grp.grp_nm, cast('N/A' as varchar)) as retail_env,
            cast((
              (
                (
                  cast((
                    coalesce(cust.salegroup, cast('N/A' as varchar))
                  ) as text) || cast((
                    cast('-' as varchar)
                  ) as text)
                ) || cast((
                  coalesce(cust.salesareaname, cast('N/A' as varchar))
                ) as text)
              )
            ) as varchar) as salesarea,
            0 as sell_out,
            0 as gross_sell_out,
            0 as net_sell_out,
            '0' as sellout_target,
            '0' as planned_call_count,
            '0' as visited_call_count,
            '0' as effective_call_count,
            '0' as coverage_stores_count,
            stores.reactivate_stores as reactivate_stores_count,
            stores.inactive_store as inactive_stores_count,
            '0' as sales_order_count,
            '0' as on_time_count,
            '0' as in_full_count,
            '0' as otif_count
          from (
            (
              (
                (
                  select
                    cust_flag.year_month,
                    cust_flag.distributor_id,
                    cust_flag.cust_cd,
                    cust_flag.curr_actv_status,
                    cust_flag.curr_net_sell_out,
                    cust_flag.prev_actv_status,
                    cust_flag.prev_net_sell_out,
                    case
                      when (
                        (
                          (
                            (
                              cust_flag.curr_net_sell_out > cast((
                                cast((
                                  0
                                ) as decimal)
                              ) as decimal(18, 0))
                            )
                            and (
                              cust_flag.prev_net_sell_out <= cast((
                                cast((
                                  0
                                ) as decimal)
                              ) as decimal(18, 0))
                            )
                          )
                          and (
                            cust_flag.curr_actv_status = 1
                          )
                        )
                        and (
                          cust_flag.prev_actv_status = 1
                        )
                      )
                      then cast('1' as varchar)
                      else cast('0' as varchar)
                    end as reactivate_stores,
                    case
                      when (
                        (
                          cust_flag.prev_net_sell_out <= cast((
                            cast((
                              0
                            ) as decimal)
                          ) as decimal(18, 0))
                        )
                        and (
                          cust_flag.prev_actv_status = 1
                        )
                      )
                      then cast('1' as varchar)
                      else cast('0' as varchar)
                    end as inactive_store
                  from (
                    select
                      curr_mon_stores.year_month,
                      curr_mon_stores.distributor_id,
                      curr_mon_stores.cust_cd,
                      curr_mon_stores.curr_actv_status,
                      coalesce(
                        curr_mon_stores.curr_net_sell_out,
                        cast((
                          cast((
                            0
                          ) as decimal)
                        ) as decimal(18, 0))
                      ) as curr_net_sell_out,
                      prev_mon_stores.prev_actv_status,
                      coalesce(
                        prev_mon_stores.prev_net_sell_out,
                        cast((
                          cast((
                            0
                          ) as decimal)
                        ) as decimal(18, 0))
                      ) as prev_net_sell_out
                    from (
                      (
                        select
                          cust_dim.snap_shot_month as year_month,
                          cust_dim.distributorid as distributor_id,
                          cust_dim.arcode as cust_cd,
                          cust_dim.activestatus as curr_actv_status,
                          coalesce(
                            sales.curr_net_sell_out,
                            cast((
                              cast((
                                0
                              ) as decimal)
                            ) as decimal(18, 0))
                          ) as curr_net_sell_out
                        from (
                          (
                            select distinct
                              to_char(
                                cast(cust.snapshot_date as timestampntz),
                                cast((
                                  cast('YYYYMM' as varchar)
                                ) as text)
                              ) as snap_shot_month,
                              cust.distributorid,
                              cust.arcode,
                              cust.salegroup,
                              cust_grp.grp_nm,
                              cust.saleemployee,
                              cust.activestatus
                            from (
                              itg_la_gt_customer_snapshot as cust
                                left join itg_th_dtscustgroup as cust_grp
                                  on (
                                    (
                                      upper(cast((
                                        cust_grp.ar_typ_cd
                                      ) as text)) = upper(cast((
                                        cust.artypecode
                                      ) as text))
                                    )
                                  )
                            )
                          ) as cust_dim
                          left join (
                            select
                              cal.cal_mnth_id,
                              itg_la_gt_sellout_fact.distributorid,
                              itg_la_gt_sellout_fact.arcode,
                              sum(itg_la_gt_sellout_fact.total) as curr_net_sell_out
                            from (
                              itg_la_gt_sellout_fact
                                left join edw_vw_os_time_dim as cal
                                  on (
                                    (
                                      cast((
                                        itg_la_gt_sellout_fact.orderdate
                                      ) as timestampntz) = cast((
                                        cal.cal_date
                                      ) as timestampntz)
                                    )
                                  )
                            )
                            where
                              (
                                itg_la_gt_sellout_fact.total > cast((
                                  cast((
                                    0
                                  ) as decimal)
                                ) as decimal(19, 6))
                              )
                            group by
                              cal.cal_mnth_id,
                              itg_la_gt_sellout_fact.distributorid,
                              itg_la_gt_sellout_fact.arcode
                          ) as sales
                            on (
                              (
                                (
                                  (
                                    cast((
                                      sales.distributorid
                                    ) as text) = cast((
                                      cust_dim.distributorid
                                    ) as text)
                                  )
                                  and (
                                    cast((
                                      sales.arcode
                                    ) as text) = cast((
                                      cust_dim.arcode
                                    ) as text)
                                  )
                                )
                                and (
                                  cast((
                                    cast((
                                      sales.cal_mnth_id
                                    ) as varchar)
                                  ) as text) = cust_dim.snap_shot_month
                                )
                              )
                            )
                        )
                      ) as curr_mon_stores
                      left join (
                        select
                          cust_status.cal_mnth_id as year_month,
                          cust_status.distributorid as distributor_id,
                          cust_status.arcode as cust_cd,
                          cust_status.actv_status as prev_actv_status,
                          coalesce(
                            sales.prev_net_sell_out,
                            cast((
                              cast((
                                0
                              ) as decimal)
                            ) as decimal(18, 0))
                          ) as prev_net_sell_out
                        from (
                          (
                            select
                              cast((
                                cal.cal_mnth_id
                              ) as varchar) as cal_mnth_id,
                              cal.l1_month,
                              cal.l3_month,
                              cust.distributorid,
                              cust.arcode,
                              MAX(cust.activestatus) as actv_status
                            from (
                              itg_la_gt_customer_snapshot as cust
                                left join (
                                  select distinct
                                    edw_vw_os_time_dim.cal_mnth_id,
                                    to_char(
                                      cast(dateadd(
                                        month,
                                        (
                                          -cast((
                                            1
                                          ) as bigint)
                                        ),
                                        cast(cast((
                                          to_date(
                                            cast((
                                              cast((
                                                edw_vw_os_time_dim.cal_mnth_id
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
                                    ) as l1_month,
                                    to_char(
                                      cast(dateadd(
                                        month,
                                        (
                                          -cast((
                                            3
                                          ) as bigint)
                                        ),
                                        cast(cast((
                                          to_date(
                                            cast((
                                              cast((
                                                edw_vw_os_time_dim.cal_mnth_id
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
                                    ) as l3_month
                                  from edw_vw_os_time_dim
                                ) as cal
                                  on (
                                    (
                                      (
                                        to_char(
                                          cast(cust.snapshot_date as timestampntz),
                                          cast((
                                            cast('YYYYMM' as varchar)
                                          ) as text)
                                        ) >= cal.l3_month
                                      )
                                      and (
                                        to_char(
                                          cast(cust.snapshot_date as timestampntz),
                                          cast((
                                            cast('YYYYMM' as varchar)
                                          ) as text)
                                        ) <= cal.l1_month
                                      )
                                    )
                                  )
                            )
                            group by
                              cast((
                                cal.cal_mnth_id
                              ) as varchar),
                              cal.l1_month,
                              cal.l3_month,
                              cust.distributorid,
                              cust.arcode
                          ) as cust_status
                          left join (
                            select
                              cast((
                                cal.cal_mnth_id
                              ) as varchar) as cal_mnth_id,
                              cal.l1_month,
                              cal.l3_month,
                              sls.distributorid,
                              sls.arcode,
                              sum(sls.total) as prev_net_sell_out
                            from (
                              itg_la_gt_sellout_fact as sls
                                left join (
                                  select distinct
                                    edw_vw_os_time_dim.cal_mnth_id,
                                    to_char(
                                      cast(dateadd(
                                        month,
                                        (
                                          -cast((
                                            1
                                          ) as bigint)
                                        ),
                                        cast(cast((
                                          to_date(
                                            cast((
                                              cast((
                                                edw_vw_os_time_dim.cal_mnth_id
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
                                    ) as l1_month,
                                    to_char(
                                      cast(dateadd(
                                        month,
                                        (
                                          -cast((
                                            3
                                          ) as bigint)
                                        ),
                                        cast(cast((
                                          to_date(
                                            cast((
                                              cast((
                                                edw_vw_os_time_dim.cal_mnth_id
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
                                    ) as l3_month
                                  from edw_vw_os_time_dim
                                ) as cal
                                  on (
                                    (
                                      (
                                        to_char(
                                          cast(cast((
                                            cast((
                                              sls.orderdate
                                            ) as timestampntz)
                                          ) as timestampntz) as timestampntz),
                                          cast((
                                            cast('YYYYMM' as varchar)
                                          ) as text)
                                        ) >= cal.l3_month
                                      )
                                      and (
                                        to_char(
                                          cast(cast((
                                            cast((
                                              sls.orderdate
                                            ) as timestampntz)
                                          ) as timestampntz) as timestampntz),
                                          cast((
                                            cast('YYYYMM' as varchar)
                                          ) as text)
                                        ) <= cal.l1_month
                                      )
                                    )
                                  )
                            )
                            where
                              (
                                sls.total > cast((
                                  cast((
                                    0
                                  ) as decimal)
                                ) as decimal(19, 6))
                              )
                            group by
                              cast((
                                cal.cal_mnth_id
                              ) as varchar),
                              cal.l1_month,
                              cal.l3_month,
                              sls.distributorid,
                              sls.arcode
                          ) as sales
                            on (
                              (
                                (
                                  (
                                    (
                                      (
                                        cast((
                                          cust_status.cal_mnth_id
                                        ) as text) = cast((
                                          sales.cal_mnth_id
                                        ) as text)
                                      )
                                      and (
                                        cust_status.l1_month = sales.l1_month
                                      )
                                    )
                                    and (
                                      cust_status.l3_month = sales.l3_month
                                    )
                                  )
                                  and (
                                    upper(cast((
                                      cust_status.distributorid
                                    ) as text)) = upper(cast((
                                      sales.distributorid
                                    ) as text))
                                  )
                                )
                                and (
                                  upper(cast((
                                    cust_status.arcode
                                  ) as text)) = upper(cast((
                                    sales.arcode
                                  ) as text))
                                )
                              )
                            )
                        )
                      ) as prev_mon_stores
                        on (
                          (
                            (
                              (
                                curr_mon_stores.year_month = cast((
                                  prev_mon_stores.year_month
                                ) as text)
                              )
                              and (
                                upper(cast((
                                  curr_mon_stores.distributor_id
                                ) as text)) = upper(cast((
                                  prev_mon_stores.distributor_id
                                ) as text))
                              )
                            )
                            and (
                              upper(cast((
                                curr_mon_stores.cust_cd
                              ) as text)) = upper(cast((
                                prev_mon_stores.cust_cd
                              ) as text))
                            )
                          )
                        )
                    )
                  ) as cust_flag
                ) as stores
                left join (
                  select distinct
                    edw_vw_os_time_dim.cal_mnth_id,
                    edw_vw_os_time_dim.cal_year,
                    edw_vw_os_time_dim.cal_mnth_no
                  from edw_vw_os_time_dim
                ) as cal
                  on (
                    (
                      stores.year_month = cast((
                        cast((
                          cal.cal_mnth_id
                        ) as varchar)
                      ) as text)
                    )
                  )
              )
              left join itg_la_gt_customer as cust
                on (
                  (
                    (
                      cast((
                        stores.distributor_id
                      ) as text) = cast((
                        cust.distributorid
                      ) as text)
                    )
                    and (
                      cast((
                        stores.cust_cd
                      ) as text) = cast((
                        cust.arcode
                      ) as text)
                    )
                  )
                )
            )
            left join itg_th_dtscustgroup as cust_grp
              on (
                (
                  upper(cast((
                    cust_grp.ar_typ_cd
                  ) as text)) = upper(cast((
                    cust.artypecode
                  ) as text))
                )
              )
          )
        )
        union all
        select
          'TARGET_SA' as identifier,
          'LA' as cntry_cd,
          'LAK' as crncy_cd,
          'Laos' as cntry_nm,
          tgt.period as year_month,
          cast((
            substring(cast((
              tgt.period
            ) as text), 1, 4)
          ) as INT) as "year",
          cast((
            substring(cast((
              tgt.period
            ) as text), 5, 6)
          ) as INT) as "month",
          tgt.distributorid as distributor_id,
          'Target' as retail_env,
          cast((
            (
              (
                cast((
                  coalesce(tgt.saleoffice, cast('N/A' as varchar))
                ) as text) || cast((
                  cast('-' as varchar)
                ) as text)
              ) || cast((
                coalesce(cust.salesareaname, cast('N/A' as varchar))
              ) as text)
            )
          ) as varchar) as salesarea,
          '0' as sell_out,
          '0' as gross_sell_out,
          '0' as net_sell_out,
          sum(tgt.target) as sellout_target,
          '0' as planned_call_count,
          '0' as visited_call_count,
          '0' as effective_call_count,
          '0' as coverage_stores_count,
          '0' as reactivate_stores_count,
          '0' as inactive_stores_count,
          '0' as sales_order_count,
          '0' as on_time_count,
          '0' as in_full_count,
          '0' as otif_count
        from (
          itg_mds_lcm_distributor_target_sales as tgt
            left join (
              select distinct
                itg_la_gt_customer.salegroup,
                itg_la_gt_customer.salesareaname
              from itg_la_gt_customer
              where
                (
                  (
                    NOT itg_la_gt_customer.salesareaname IS NULL
                  )
                  and (
                    itg_la_gt_customer.activestatus = 1
                  )
                )
            ) as cust
              on (
                (
                  cast((
                    cust.salegroup
                  ) as text) = cast((
                    tgt.saleoffice
                  ) as text)
                )
              )
        )
        where
          (
            cast((
              tgt.distributorid
            ) as text) = cast('LAO' as text)
          )
        group by
          tgt.distributorid,
          tgt.saleoffice,
          tgt.period,
          cust.salesareaname
      )
      union all
      select
        'TARGET_RE' as identifier,
        'LA' as cntry_cd,
        'LAK' as crncy_cd,
        'Laos' as cntry_nm,
        tgt.period as year_month,
        cast((
          substring(cast((
            tgt.period
          ) as text), 1, 4)
        ) as INT) as "year",
        cast((
          substring(cast((
            tgt.period
          ) as text), 5, 6)
        ) as INT) as "month",
        tgt.distributorid as distributor_id,
        coalesce(cust_grp.grp_nm, cast('N/A' as varchar)) as retail_env,
        'N/A' as salesarea,
        '0' as sell_out,
        '0' as gross_sell_out,
        '0' as net_sell_out,
        sum(tgt.target) as sellout_target,
        '0' as planned_call_count,
        '0' as visited_call_count,
        '0' as effective_call_count,
        '0' as coverage_stores_count,
        '0' as reactivate_stores_count,
        '0' as inactive_stores_count,
        '0' as sales_order_count,
        '0' as on_time_count,
        '0' as in_full_count,
        '0' as otif_count
      from (
        itg_mds_lcm_distributor_target_sales_re as tgt
          left join itg_th_dtscustgroup as cust_grp
            on (
              (
                upper(cast((
                  cust_grp.ar_typ_cd
                ) as text)) = upper(cast((
                  tgt.re
                ) as text))
              )
            )
      )
      where
        (
          cast((
            tgt.distributorid
          ) as text) = cast('LAO' as text)
        )
      group by
        tgt.distributorid,
        cust_grp.grp_nm,
        tgt.period
    )
    union all
    select
      'ACTUAL' as identifier,
      'LA' as cntry_cd,
      'LAK' as crncy_cd,
      'Laos' as cntry_nm,
      cast((
        cal.cal_mnth_id
      ) as varchar) as year_month,
      cal.cal_year as "year",
      cal.cal_mnth_no as "month",
      otif.distributor_id,
      coalesce(cust_dim.grp_nm, cast('N/A' as varchar)) as retail_env,
      cast((
        (
          (
            cast((
              coalesce(cust_dim.salegroup, cast('N/A' as varchar))
            ) as text) || cast((
              cast('-' as varchar)
            ) as text)
          ) || cast((
            coalesce(cust_dim.salesareaname, cast('N/A' as varchar))
          ) as text)
        )
      ) as varchar) as salesarea,
      '0' as sell_out,
      '0' as gross_sell_out,
      '0' as net_sell_out,
      '0' as sellout_target,
      '0' as planned_call_count,
      '0' as visited_call_count,
      '0' as effective_call_count,
      '0' as coverage_stores_count,
      '0' as reactivate_stores_count,
      '0' as inactive_stores_count,
      count(otif.sales_order_no) as sales_order_count,
      sum(
        case
          when (
            cast((
              otif.on_time
            ) as text) = cast((
              cast('Y' as varchar)
            ) as text)
          )
          then 1
          else 0
        end
      ) as on_time_count,
      sum(
        case
          when (
            cast((
              otif.in_full
            ) as text) = cast((
              cast('Y' as varchar)
            ) as text)
          )
          then 1
          else 0
        end
      ) as in_full_count,
      sum(
        case
          when (
            cast((
              otif.otif
            ) as text) = cast((
              cast('Y' as varchar)
            ) as text)
          )
          then 1
          else 0
        end
      ) as otif_count
    from (
      (
        (
          select
            so.saleunit as distributor_id,
            so.productid as product_code,
            so.orderid as sales_order_no,
            so.ref_15 as invoice_no,
            so.orderdate,
            so.customer_id as store_id,
            ot.on_time,
            inf.in_full,
            case
              when (
                (
                  cast((
                    ot.on_time
                  ) as text) = cast((
                    cast('Y' as varchar)
                  ) as text)
                )
                and (
                  cast((
                    inf.in_full
                  ) as text) = cast((
                    cast('Y' as varchar)
                  ) as text)
                )
              )
              then cast('Y' as varchar)
              when (
                (
                  ot.on_time IS NULL
                ) AND (
                  inf.in_full IS NULL
                )
              )
              then cast('NA' as varchar)
              else cast('N' as varchar)
            end as otif
          from (
            (
              (
                select
                  itg_la_gt_sales_order_fact.saleunit,
                  itg_la_gt_sales_order_fact.orderid,
                  itg_la_gt_sales_order_fact.orderdate,
                  itg_la_gt_sales_order_fact.customer_id,
                  itg_la_gt_sales_order_fact.customer_name,
                  itg_la_gt_sales_order_fact.city,
                  itg_la_gt_sales_order_fact.region,
                  itg_la_gt_sales_order_fact.saledistrict,
                  itg_la_gt_sales_order_fact.saleoffice,
                  itg_la_gt_sales_order_fact.salegroup,
                  itg_la_gt_sales_order_fact.customertype,
                  itg_la_gt_sales_order_fact.storetype,
                  itg_la_gt_sales_order_fact.saletype,
                  itg_la_gt_sales_order_fact.salesemployee,
                  itg_la_gt_sales_order_fact.salename,
                  itg_la_gt_sales_order_fact.productid,
                  itg_la_gt_sales_order_fact.productname,
                  itg_la_gt_sales_order_fact.megabrand,
                  itg_la_gt_sales_order_fact.brand,
                  itg_la_gt_sales_order_fact.baseproduct,
                  itg_la_gt_sales_order_fact.variant,
                  itg_la_gt_sales_order_fact.putup,
                  itg_la_gt_sales_order_fact.priceref,
                  itg_la_gt_sales_order_fact.backlog,
                  itg_la_gt_sales_order_fact.qty,
                  itg_la_gt_sales_order_fact.subamt1,
                  itg_la_gt_sales_order_fact.discount,
                  itg_la_gt_sales_order_fact.subamt2,
                  itg_la_gt_sales_order_fact.discountbtline,
                  itg_la_gt_sales_order_fact.totalbeforevat,
                  itg_la_gt_sales_order_fact.total,
                  itg_la_gt_sales_order_fact.sales_order_line_no,
                  itg_la_gt_sales_order_fact.canceled,
                  itg_la_gt_sales_order_fact.documentid,
                  itg_la_gt_sales_order_fact.return_reason,
                  itg_la_gt_sales_order_fact.promotioncode,
                  itg_la_gt_sales_order_fact.promotioncode1,
                  itg_la_gt_sales_order_fact.promotioncode2,
                  itg_la_gt_sales_order_fact.promotioncode3,
                  itg_la_gt_sales_order_fact.promotioncode4,
                  itg_la_gt_sales_order_fact.promotioncode5,
                  itg_la_gt_sales_order_fact.promotion_code,
                  itg_la_gt_sales_order_fact.promotion_code2,
                  itg_la_gt_sales_order_fact.promotion_code3,
                  itg_la_gt_sales_order_fact.avgdiscount,
                  itg_la_gt_sales_order_fact.ordertype,
                  itg_la_gt_sales_order_fact.approverstatus,
                  itg_la_gt_sales_order_fact.pricelevel,
                  itg_la_gt_sales_order_fact.optional3,
                  itg_la_gt_sales_order_fact.deliverydate,
                  itg_la_gt_sales_order_fact.ordertime,
                  itg_la_gt_sales_order_fact.shipto,
                  itg_la_gt_sales_order_fact.billto,
                  itg_la_gt_sales_order_fact.deliveryrouteid,
                  itg_la_gt_sales_order_fact.approved_date,
                  itg_la_gt_sales_order_fact.approved_time,
                  itg_la_gt_sales_order_fact.ref_15,
                  itg_la_gt_sales_order_fact.paymenttype,
                  itg_la_gt_sales_order_fact.load_flag,
                  itg_la_gt_sales_order_fact.filename,
                  itg_la_gt_sales_order_fact.run_id,
                  itg_la_gt_sales_order_fact.crt_dttm,
                  itg_la_gt_sales_order_fact.updt_dttm
                from itg_la_gt_sales_order_fact
                where
                  (
                    (
                      (
                        upper(cast((
                          itg_la_gt_sales_order_fact.load_flag
                        ) as text)) <> cast((
                          cast('D' as varchar)
                        ) as text)
                      )
                      and (
                        cast((
                          itg_la_gt_sales_order_fact.canceled
                        ) as text) <> cast((
                          cast((
                            1
                          ) as varchar)
                        ) as text)
                      )
                    )
                    and (
                      upper(cast((
                        itg_la_gt_sales_order_fact.approverstatus
                      ) as text)) = cast((
                        cast('Y' as varchar)
                      ) as text)
                    )
                  )
              ) as so
              left join (
                select
                  cast('ON_TIME' as varchar) as kpi,
                  so.saleunit as distributor_id,
                  so.productid as product_code,
                  so.orderid as sales_order_no,
                  so.ref_15 as invoice_no,
                  so.orderdate,
                  so.customer_id as store_id,
                  so.sales_order_line_no,
                  case
                    when (
                      (
                        NOT cast((
                          sls.bill_date
                        ) as timestampntz) IS NULL
                      )
                      and (
                        DATEDIFF(
                          day,
                          cast((
                            so.orderdate
                          ) as timestampntz),
                          cast((
                            sls.bill_date
                          ) as timestampntz)
                        ) <= 3
                      )
                    )
                    then cast('Y' as varchar)
                    else cast('N' as varchar)
                  end as on_time
                from (
                  (
                    select
                      itg_la_gt_sales_order_fact.saleunit,
                      itg_la_gt_sales_order_fact.orderid,
                      itg_la_gt_sales_order_fact.orderdate,
                      itg_la_gt_sales_order_fact.customer_id,
                      itg_la_gt_sales_order_fact.customer_name,
                      itg_la_gt_sales_order_fact.city,
                      itg_la_gt_sales_order_fact.region,
                      itg_la_gt_sales_order_fact.saledistrict,
                      itg_la_gt_sales_order_fact.saleoffice,
                      itg_la_gt_sales_order_fact.salegroup,
                      itg_la_gt_sales_order_fact.customertype,
                      itg_la_gt_sales_order_fact.storetype,
                      itg_la_gt_sales_order_fact.saletype,
                      itg_la_gt_sales_order_fact.salesemployee,
                      itg_la_gt_sales_order_fact.salename,
                      itg_la_gt_sales_order_fact.productid,
                      itg_la_gt_sales_order_fact.productname,
                      itg_la_gt_sales_order_fact.megabrand,
                      itg_la_gt_sales_order_fact.brand,
                      itg_la_gt_sales_order_fact.baseproduct,
                      itg_la_gt_sales_order_fact.variant,
                      itg_la_gt_sales_order_fact.putup,
                      itg_la_gt_sales_order_fact.priceref,
                      itg_la_gt_sales_order_fact.backlog,
                      itg_la_gt_sales_order_fact.qty,
                      itg_la_gt_sales_order_fact.subamt1,
                      itg_la_gt_sales_order_fact.discount,
                      itg_la_gt_sales_order_fact.subamt2,
                      itg_la_gt_sales_order_fact.discountbtline,
                      itg_la_gt_sales_order_fact.totalbeforevat,
                      itg_la_gt_sales_order_fact.total,
                      itg_la_gt_sales_order_fact.sales_order_line_no,
                      itg_la_gt_sales_order_fact.canceled,
                      itg_la_gt_sales_order_fact.documentid,
                      itg_la_gt_sales_order_fact.return_reason,
                      itg_la_gt_sales_order_fact.promotioncode,
                      itg_la_gt_sales_order_fact.promotioncode1,
                      itg_la_gt_sales_order_fact.promotioncode2,
                      itg_la_gt_sales_order_fact.promotioncode3,
                      itg_la_gt_sales_order_fact.promotioncode4,
                      itg_la_gt_sales_order_fact.promotioncode5,
                      itg_la_gt_sales_order_fact.promotion_code,
                      itg_la_gt_sales_order_fact.promotion_code2,
                      itg_la_gt_sales_order_fact.promotion_code3,
                      itg_la_gt_sales_order_fact.avgdiscount,
                      itg_la_gt_sales_order_fact.ordertype,
                      itg_la_gt_sales_order_fact.approverstatus,
                      itg_la_gt_sales_order_fact.pricelevel,
                      itg_la_gt_sales_order_fact.optional3,
                      itg_la_gt_sales_order_fact.deliverydate,
                      itg_la_gt_sales_order_fact.ordertime,
                      itg_la_gt_sales_order_fact.shipto,
                      itg_la_gt_sales_order_fact.billto,
                      itg_la_gt_sales_order_fact.deliveryrouteid,
                      itg_la_gt_sales_order_fact.approved_date,
                      itg_la_gt_sales_order_fact.approved_time,
                      itg_la_gt_sales_order_fact.ref_15,
                      itg_la_gt_sales_order_fact.paymenttype,
                      itg_la_gt_sales_order_fact.load_flag,
                      itg_la_gt_sales_order_fact.filename,
                      itg_la_gt_sales_order_fact.run_id,
                      itg_la_gt_sales_order_fact.crt_dttm,
                      itg_la_gt_sales_order_fact.updt_dttm
                    from itg_la_gt_sales_order_fact
                    where
                      (
                        (
                          (
                            upper(cast((
                              itg_la_gt_sales_order_fact.load_flag
                            ) as text)) <> cast((
                              cast('D' as varchar)
                            ) as text)
                          )
                          and (
                            cast((
                              itg_la_gt_sales_order_fact.canceled
                            ) as text) <> cast((
                              cast((
                                1
                              ) as varchar)
                            ) as text)
                          )
                        )
                        and (
                          upper(cast((
                            itg_la_gt_sales_order_fact.approverstatus
                          ) as text)) = cast((
                            cast('Y' as varchar)
                          ) as text)
                        )
                      )
                  ) as so
                  left join (
                    select
                      itg_la_gt_sellout_fact.distributorid,
                      itg_la_gt_sellout_fact.orderno,
                      itg_la_gt_sellout_fact.arcode as cust_cd,
                      itg_la_gt_sellout_fact.orderdate as bill_date,
                      itg_la_gt_sellout_fact.productcode,
                      itg_la_gt_sellout_fact.linenumber,
                      sum(itg_la_gt_sellout_fact.qty) as sls_qty
                    from itg_la_gt_sellout_fact
                    group by
                      itg_la_gt_sellout_fact.distributorid,
                      itg_la_gt_sellout_fact.orderno,
                      itg_la_gt_sellout_fact.arcode,
                      itg_la_gt_sellout_fact.orderdate,
                      itg_la_gt_sellout_fact.productcode,
                      itg_la_gt_sellout_fact.linenumber
                  ) as sls
                    on (
                      (
                        (
                          (
                            (
                              cast((
                                coalesce(so.ref_15, cast('NA' as varchar))
                              ) as text) = cast((
                                coalesce(sls.orderno, cast('NA' as varchar))
                              ) as text)
                            )
                            and (
                              cast((
                                so.customer_id
                              ) as text) = cast((
                                sls.cust_cd
                              ) as text)
                            )
                          )
                          and (
                            cast((
                              so.saleunit
                            ) as text) = cast((
                              sls.distributorid
                            ) as text)
                          )
                        )
                        and (
                          cast((
                            so.sales_order_line_no
                          ) as text) = cast((
                            cast((
                              sls.linenumber
                            ) as varchar)
                          ) as text)
                        )
                      )
                    )
                )
                where
                  (
                    case
                      when (
                        (
                          NOT sls.bill_date IS NULL
                        ) AND (
                          sls.bill_date >= so.orderdate
                        )
                      )
                      then 1
                      when (
                        (
                          sls.bill_date IS NULL
                        )
                        and (
                          DATEDIFF(
                            day,
                            cast((
                              so.orderdate
                            ) as timestampntz),
                            coalesce(
                              cast((
                                sls.bill_date
                              ) as timestampntz),
                              CONVERT_TIMEZONE(
                                cast((
                                  cast('Asia/Bangkok' as varchar)
                                ) as text),
                                cast((
                                  cast(current_timestamp() as varchar)
                                ) as timestampntz)
                              )
                            )
                          ) > 3
                        )
                      )
                      then 1
                      else 0
                    end = 1
                  )
              ) as ot
                on (
                  (
                    (
                      (
                        (
                          (
                            (
                              cast((
                                ot.distributor_id
                              ) as text) = cast((
                                so.saleunit
                              ) as text)
                            )
                            and (
                              cast((
                                ot.sales_order_line_no
                              ) as text) = cast((
                                so.sales_order_line_no
                              ) as text)
                            )
                          )
                          and (
                            cast((
                              ot.sales_order_no
                            ) as text) = cast((
                              so.orderid
                            ) as text)
                          )
                        )
                        and (
                          cast((
                            ot.invoice_no
                          ) as text) = cast((
                            so.ref_15
                          ) as text)
                        )
                      )
                      and (
                        ot.orderdate = so.orderdate
                      )
                    )
                    and (
                      cast((
                        ot.store_id
                      ) as text) = cast((
                        so.customer_id
                      ) as text)
                    )
                  )
                )
            )
            left join (
              select
                cast('IN_FULL' as varchar) as kpi,
                so.saleunit as distributor_id,
                so.productid as product_code,
                so.orderid as sales_order_no,
                so.ref_15 as invoice_no,
                so.orderdate,
                so.customer_id as store_id,
                so.sales_order_line_no,
                case
                  when (
                    (
                      coalesce(so.qty, cast((
                        cast((
                          0
                        ) as decimal)
                      ) as decimal(18, 0))) > cast((
                        cast((
                          0
                        ) as decimal)
                      ) as decimal(18, 0))
                    )
                    and (
                      ROUND(
                        coalesce(sls.sls_qty, cast((
                          cast((
                            0
                          ) as decimal)
                        ) as decimal(18, 0))),
                        2
                      ) >= ROUND(coalesce(so.qty, cast((
                        cast((
                          0
                        ) as decimal)
                      ) as decimal(18, 0))), 2)
                    )
                  )
                  then cast('Y' as varchar)
                  else cast('N' as varchar)
                end as in_full
              from (
                (
                  select
                    itg_la_gt_sales_order_fact.saleunit,
                    itg_la_gt_sales_order_fact.orderid,
                    itg_la_gt_sales_order_fact.orderdate,
                    itg_la_gt_sales_order_fact.customer_id,
                    itg_la_gt_sales_order_fact.customer_name,
                    itg_la_gt_sales_order_fact.city,
                    itg_la_gt_sales_order_fact.region,
                    itg_la_gt_sales_order_fact.saledistrict,
                    itg_la_gt_sales_order_fact.saleoffice,
                    itg_la_gt_sales_order_fact.salegroup,
                    itg_la_gt_sales_order_fact.customertype,
                    itg_la_gt_sales_order_fact.storetype,
                    itg_la_gt_sales_order_fact.saletype,
                    itg_la_gt_sales_order_fact.salesemployee,
                    itg_la_gt_sales_order_fact.salename,
                    itg_la_gt_sales_order_fact.productid,
                    itg_la_gt_sales_order_fact.productname,
                    itg_la_gt_sales_order_fact.megabrand,
                    itg_la_gt_sales_order_fact.brand,
                    itg_la_gt_sales_order_fact.baseproduct,
                    itg_la_gt_sales_order_fact.variant,
                    itg_la_gt_sales_order_fact.putup,
                    itg_la_gt_sales_order_fact.priceref,
                    itg_la_gt_sales_order_fact.backlog,
                    itg_la_gt_sales_order_fact.qty,
                    itg_la_gt_sales_order_fact.subamt1,
                    itg_la_gt_sales_order_fact.discount,
                    itg_la_gt_sales_order_fact.subamt2,
                    itg_la_gt_sales_order_fact.discountbtline,
                    itg_la_gt_sales_order_fact.totalbeforevat,
                    itg_la_gt_sales_order_fact.total,
                    itg_la_gt_sales_order_fact.sales_order_line_no,
                    itg_la_gt_sales_order_fact.canceled,
                    itg_la_gt_sales_order_fact.documentid,
                    itg_la_gt_sales_order_fact.return_reason,
                    itg_la_gt_sales_order_fact.promotioncode,
                    itg_la_gt_sales_order_fact.promotioncode1,
                    itg_la_gt_sales_order_fact.promotioncode2,
                    itg_la_gt_sales_order_fact.promotioncode3,
                    itg_la_gt_sales_order_fact.promotioncode4,
                    itg_la_gt_sales_order_fact.promotioncode5,
                    itg_la_gt_sales_order_fact.promotion_code,
                    itg_la_gt_sales_order_fact.promotion_code2,
                    itg_la_gt_sales_order_fact.promotion_code3,
                    itg_la_gt_sales_order_fact.avgdiscount,
                    itg_la_gt_sales_order_fact.ordertype,
                    itg_la_gt_sales_order_fact.approverstatus,
                    itg_la_gt_sales_order_fact.pricelevel,
                    itg_la_gt_sales_order_fact.optional3,
                    itg_la_gt_sales_order_fact.deliverydate,
                    itg_la_gt_sales_order_fact.ordertime,
                    itg_la_gt_sales_order_fact.shipto,
                    itg_la_gt_sales_order_fact.billto,
                    itg_la_gt_sales_order_fact.deliveryrouteid,
                    itg_la_gt_sales_order_fact.approved_date,
                    itg_la_gt_sales_order_fact.approved_time,
                    itg_la_gt_sales_order_fact.ref_15,
                    itg_la_gt_sales_order_fact.paymenttype,
                    itg_la_gt_sales_order_fact.load_flag,
                    itg_la_gt_sales_order_fact.filename,
                    itg_la_gt_sales_order_fact.run_id,
                    itg_la_gt_sales_order_fact.crt_dttm,
                    itg_la_gt_sales_order_fact.updt_dttm
                  from itg_la_gt_sales_order_fact
                  where
                    (
                      (
                        (
                          upper(cast((
                            itg_la_gt_sales_order_fact.load_flag
                          ) as text)) <> cast((
                            cast('D' as varchar)
                          ) as text)
                        )
                        and (
                          cast((
                            itg_la_gt_sales_order_fact.canceled
                          ) as text) <> cast((
                            cast((
                              1
                            ) as varchar)
                          ) as text)
                        )
                      )
                      and (
                        upper(cast((
                          itg_la_gt_sales_order_fact.approverstatus
                        ) as text)) = cast((
                          cast('Y' as varchar)
                        ) as text)
                      )
                    )
                ) as so
                left join (
                  select
                    itg_la_gt_sellout_fact.distributorid,
                    itg_la_gt_sellout_fact.orderno,
                    itg_la_gt_sellout_fact.arcode as cust_cd,
                    itg_la_gt_sellout_fact.orderdate as bill_date,
                    itg_la_gt_sellout_fact.productcode,
                    itg_la_gt_sellout_fact.linenumber,
                    sum(itg_la_gt_sellout_fact.qty) as sls_qty
                  from itg_la_gt_sellout_fact
                  group by
                    itg_la_gt_sellout_fact.distributorid,
                    itg_la_gt_sellout_fact.orderno,
                    itg_la_gt_sellout_fact.arcode,
                    itg_la_gt_sellout_fact.orderdate,
                    itg_la_gt_sellout_fact.productcode,
                    itg_la_gt_sellout_fact.linenumber
                ) as sls
                  on (
                    (
                      (
                        (
                          (
                            cast((
                              coalesce(so.ref_15, cast('NA' as varchar))
                            ) as text) = cast((
                              coalesce(sls.orderno, cast('NA' as varchar))
                            ) as text)
                          )
                          and (
                            cast((
                              so.customer_id
                            ) as text) = cast((
                              sls.cust_cd
                            ) as text)
                          )
                        )
                        and (
                          cast((
                            so.saleunit
                          ) as text) = cast((
                            sls.distributorid
                          ) as text)
                        )
                      )
                      and (
                        cast((
                          so.sales_order_line_no
                        ) as text) = cast((
                          cast((
                            sls.linenumber
                          ) as varchar)
                        ) as text)
                      )
                    )
                  )
              )
              where
                (
                  case
                    when (
                      (
                        NOT sls.bill_date IS NULL
                      ) AND (
                        sls.bill_date >= so.orderdate
                      )
                    )
                    then 1
                    when (
                      (
                        sls.bill_date IS NULL
                      )
                      and (
                        DATEDIFF(
                          day,
                          cast((
                            so.orderdate
                          ) as timestampntz),
                          coalesce(
                            cast((
                              sls.bill_date
                            ) as timestampntz),
                            CONVERT_TIMEZONE(
                              cast((
                                cast('Asia/Bangkok' as varchar)
                              ) as text),
                              cast((
                                cast(current_timestamp() as varchar)
                              ) as timestampntz)
                            )
                          )
                        ) > 3
                      )
                    )
                    then 1
                    else 0
                  end = 1
                )
            ) as inf
              on (
                (
                  (
                    (
                      (
                        (
                          (
                            cast((
                              inf.distributor_id
                            ) as text) = cast((
                              so.saleunit
                            ) as text)
                          )
                          and (
                            cast((
                              inf.sales_order_line_no
                            ) as text) = cast((
                              so.sales_order_line_no
                            ) as text)
                          )
                        )
                        and (
                          cast((
                            inf.sales_order_no
                          ) as text) = cast((
                            so.orderid
                          ) as text)
                        )
                      )
                      and (
                        cast((
                          inf.invoice_no
                        ) as text) = cast((
                          so.ref_15
                        ) as text)
                      )
                    )
                    and (
                      inf.orderdate = so.orderdate
                    )
                  )
                  and (
                    cast((
                      inf.store_id
                    ) as text) = cast((
                      so.customer_id
                    ) as text)
                  )
                )
              )
          )
        ) as otif
        left join edw_vw_os_time_dim as cal
          on (
            (
              cast((
                otif.orderdate
              ) as timestampntz) = cast((
                cal.cal_date
              ) as timestampntz)
            )
          )
      )
      left join (
        select distinct
          itg_la_gt_customer.distributorid,
          itg_la_gt_customer.arcode as customer_id,
          cust_grp.grp_nm,
          itg_la_gt_customer.salegroup,
          itg_la_gt_customer.salesareaname
        from (
          itg_la_gt_customer
            left join itg_th_dtscustgroup as cust_grp
              on (
                (
                  upper(cast((
                    cust_grp.ar_typ_cd
                  ) as text)) = upper(cast((
                    itg_la_gt_customer.artypecode
                  ) as text))
                )
              )
        )
        where
          (
            itg_la_gt_customer.activestatus = 1
          )
      ) as cust_dim
        on (
          (
            (
              cast((
                cust_dim.distributorid
              ) as text) = cast((
                otif.distributor_id
              ) as text)
            )
            and (
              cast((
                cust_dim.customer_id
              ) as text) = cast((
                otif.store_id
              ) as text)
            )
          )
        )
    )
    where
      (
        cast((
          otif.otif
        ) as text) <> cast((
          cast('NA' as varchar)
        ) as text)
      )
    group by
      cast((
        cal.cal_mnth_id
      ) as varchar),
      cal.cal_year,
      cal.cal_mnth_no,
      otif.distributor_id,
      coalesce(cust_dim.grp_nm, cast('N/A' as varchar)),
      cust_dim.salegroup,
      cust_dim.salesareaname
  )
  union all
  (
    (
      (
        (
          select
            'ACTUAL' as identifier,
            'CBD' as cntry_cd,
            'THB' as crncy_cd,
            'Cambodia' as cntry_nm,
            cast((
              cal.cal_mnth_id
            ) as varchar) as year_month,
            cal.cal_year as "year",
            cal.cal_mnth_no as "month",
            'CBD' as distributor_id,
            cast((
              coalesce(
                upper(cast((
                  sls.customer_group
                ) as text)),
                cast((
                  cast('N/A' as varchar)
                ) as text)
              )
            ) as varchar) as retail_env,
            cast((
              coalesce(
                upper(cast((
                  sls.province
                ) as text)),
                cast((
                  cast('N/A' as varchar)
                ) as text)
              )
            ) as varchar) as salesarea,
            sum((
              sls.net_sales_usd * exch_rt.exch_rate
            )) as sell_out,
            sum(
              (
                (
                  sls.sales_qty * matl_dim.sls_prc_credit
                ) / cast((
                  matl_dim.unit_per_case
                ) as decimal)
              )
            ) as gross_sell_out,
            sum((
              sls.net_sales_usd * exch_rt.exch_rate
            )) as net_sell_out,
            0 as sellout_target,
            '0' as planned_call_count,
            '0' as visited_call_count,
            '0' as effective_call_count,
            '0' as coverage_stores_count,
            '0' as reactivate_stores_count,
            '0' as inactive_stores_count,
            '0' as sales_order_count,
            '0' as on_time_count,
            '0' as in_full_count,
            '0' as otif_count
          from (
            (
              (
                (
                  select
                    st.bu,
                    st.client,
                    st.sub_client,
                    st.product_code,
                    st.product_name,
                    st.billing_no,
                    st.billing_date,
                    st.batch_no,
                    st.expiry_date,
                    st.customer_code,
                    st.customer_name,
                    st.distribution_channel,
                    st.customer_group,
                    st.province,
                    st.sales_qty,
                    st.foc_qty,
                    st.net_price,
                    st.net_sales_usd,
                    st.sales_rep_no,
                    st.order_no,
                    st.return_reason,
                    st.payment_term,
                    st.load_flag,
                    st.filename,
                    st.run_id,
                    st.crt_dttm,
                    st.updt_dttm,
                    item_master."sap code 1" as sap_product_code
                  from (
                    itg_cbd_gt_sales_report_fact as st
                      left join itg_mds_th_cbd_item_master as item_master
                        on (
                          (
                            Ltrim(
                              cast((
                                st.product_code
                              ) as text),
                              cast((
                                cast((
                                  0
                                ) as varchar)
                              ) as text)
                            ) = Ltrim(
                              cast((
                                cast((
                                  item_master."dksh code 1"
                                ) as varchar)
                              ) as text),
                              cast((
                                cast((
                                  0
                                ) as varchar)
                              ) as text)
                            )
                          )
                        )
                  )
                ) as sls
                left join itg_mds_th_lcm_exchange_rate as exch_rt
                  on (
                    (
                      (
                        (
                          (
                            (
                              trim(upper(cast((
                                exch_rt.cntry_key
                              ) as text))) = cast('CBD' as text)
                            )
                            and (
                              trim(upper(cast((
                                exch_rt.from_ccy
                              ) as text))) = cast('USD' as text)
                            )
                          )
                          and (
                            trim(upper(cast((
                              exch_rt.to_ccy
                            ) as text))) = cast('THB' as text)
                          )
                        )
                        and (
                          sls.billing_date >= cast((
                            exch_rt.valid_from
                          ) as DATE)
                        )
                      )
                      and (
                        sls.billing_date <= cast((
                          exch_rt.valid_to
                        ) as DATE)
                      )
                    )
                  )
              )
              left join edw_vw_os_time_dim as cal
                on (
                  (
                    cast((
                      sls.billing_date
                    ) as timestampntz) = cast((
                      cal.cal_date
                    ) as timestampntz)
                  )
                )
            )
            left join itg_th_dstrbtr_material_dim as matl_dim
              on (
                (
                  Ltrim(
                    cast((
                      cast((
                        sls.sap_product_code
                      ) as varchar)
                    ) as text),
                    cast((
                      cast((
                        0
                      ) as varchar)
                    ) as text)
                  ) = Ltrim(
                    cast((
                      matl_dim.item_cd
                    ) as text),
                    cast((
                      cast((
                        0
                      ) as varchar)
                    ) as text)
                  )
                )
              )
          )
          group by
            cal.cal_mnth_id,
            cal.cal_year,
            cal.cal_mnth_no,
            sls.customer_group,
            sls.province
          union all
          select
            'ACTUAL' as identifier,
            'CBD' as cntry_cd,
            'THB' as crncy_cd,
            'Cambodia' as cntry_nm,
            cast((
              cal.cal_mnth_id
            ) as varchar) as year_month,
            cal.cal_year as "year",
            cal.cal_mnth_no as "month",
            cust.dstrbtr_id as distributor_id,
            cast((
              coalesce(upper(cast((
                cust.grp_nm
              ) as text)), cast((
                cast('N/A' as varchar)
              ) as text))
            ) as varchar) as retail_env,
            cast((
              coalesce(
                upper(cast((
                  cust.sls_grp
                ) as text)),
                cast((
                  cast('N/A' as varchar)
                ) as text)
              )
            ) as varchar) as salesarea,
            '0' as sell_out,
            '0' as gross_sell_out,
            '0' as net_sell_out,
            '0' as sellout_target,
            '0' as planned_call_count,
            '0' as visited_call_count,
            '0' as effective_call_count,
            count(*) as coverage_stores_count,
            '0' as reactivate_stores_count,
            '0' as inactive_stores_count,
            '0' as sales_order_count,
            '0' as on_time_count,
            '0' as in_full_count,
            '0' as otif_count
          from (
            itg_cbd_gt_customer_snapshot as cust
              left join edw_vw_os_time_dim as cal
                on (
                  (
                    cust.snapshot_date::date = cal.cal_date::date
                  )
                )
          )
          where
            (
              trim(cast((
                cast((
                  cust.actv_status
                ) as varchar)
              ) as text)) = cast((
                cast('1' as varchar)
              ) as text)
            )
          group by
            cal.cal_mnth_id,
            cal.cal_year,
            cal.cal_mnth_no,
            cust.dstrbtr_id,
            coalesce(upper(cast((
              cust.grp_nm
            ) as text)), cast((
              cast('N/A' as varchar)
            ) as text)),
            cust.sls_grp
        )
        union all
        select
          'ACTUAL' as identifier,
          'CBD' as cntry_cd,
          'THB' as crncy_cd,
          'Cambodia' as cntry_nm,
          cast((
            stores.year_month
          ) as varchar) as year_month,
          cal.cal_year as "year",
          cal.cal_mnth_no as "month",
          stores.distributor_id,
          cast((
            coalesce(upper(cast((
              cust.re_nm
            ) as text)), cast((
              cast('N/A' as varchar)
            ) as text))
          ) as varchar) as retail_env,
          cast((
            coalesce(
              upper(cast((
                cust.sls_grp
              ) as text)),
              cast((
                cast('N/A' as varchar)
              ) as text)
            )
          ) as varchar) as salesarea,
          0 as sell_out,
          0 as gross_sell_out,
          0 as net_sell_out,
          '0' as sellout_target,
          '0' as planned_call_count,
          '0' as visited_call_count,
          '0' as effective_call_count,
          '0' as coverage_stores_count,
          stores.reactivate_stores as reactivate_stores_count,
          stores.inactive_store as inactive_stores_count,
          '0' as sales_order_count,
          '0' as on_time_count,
          '0' as in_full_count,
          '0' as otif_count
        from (
          (
            (
              select
                cust_flag.year_month,
                cust_flag.distributor_id,
                cust_flag.cust_cd,
                cust_flag.curr_actv_status,
                cust_flag.curr_net_sell_out,
                cust_flag.prev_actv_status,
                cust_flag.prev_net_sell_out,
                case
                  when (
                    (
                      (
                        (
                          cust_flag.curr_net_sell_out > cast((
                            cast((
                              0
                            ) as decimal)
                          ) as decimal(18, 0))
                        )
                        and (
                          cust_flag.prev_net_sell_out <= cast((
                            cast((
                              0
                            ) as decimal)
                          ) as decimal(18, 0))
                        )
                      )
                      and (
                        cust_flag.curr_actv_status = 1
                      )
                    )
                    and (
                      cust_flag.prev_actv_status = 1
                    )
                  )
                  then cast('1' as varchar)
                  else cast('0' as varchar)
                end as reactivate_stores,
                case
                  when (
                    (
                      cust_flag.prev_net_sell_out <= cast((
                        cast((
                          0
                        ) as decimal)
                      ) as decimal(18, 0))
                    )
                    and (
                      cust_flag.prev_actv_status = 1
                    )
                  )
                  then cast('1' as varchar)
                  else cast('0' as varchar)
                end as inactive_store
              from (
                select
                  curr_mon_stores.year_month,
                  curr_mon_stores.distributor_id,
                  curr_mon_stores.cust_cd,
                  curr_mon_stores.curr_actv_status,
                  coalesce(
                    curr_mon_stores.curr_net_sell_out,
                    cast((
                      cast((
                        0
                      ) as decimal)
                    ) as decimal(18, 0))
                  ) as curr_net_sell_out,
                  prev_mon_stores.prev_actv_status,
                  coalesce(
                    prev_mon_stores.prev_net_sell_out,
                    cast((
                      cast((
                        0
                      ) as decimal)
                    ) as decimal(18, 0))
                  ) as prev_net_sell_out
                from (
                  (
                    select
                      cust_dim.snap_shot_month as year_month,
                      cust_dim.distributorid as distributor_id,
                      cust_dim.arcode as cust_cd,
                      cust_dim.actv_status as curr_actv_status,
                      coalesce(
                        sales.curr_net_sell_out,
                        cast((
                          cast((
                            0
                          ) as decimal)
                        ) as decimal(18, 0))
                      ) as curr_net_sell_out
                    from (
                      (
                        select distinct
                          to_char(
                            cast(cust.snapshot_date as timestampntz),
                            cast((
                              cast('YYYYMM' as varchar)
                            ) as text)
                          ) as snap_shot_month,
                          cust.dstrbtr_id as distributorid,
                          cust.ar_cd as arcode,
                          cust.sls_grp,
                          cust.re_nm,
                          cust.sls_emp,
                          cust.sls_office,
                          cust.actv_status
                        from itg_cbd_gt_customer_snapshot as cust
                      ) as cust_dim
                      left join (
                        select
                          cal.cal_mnth_id,
                          cast('CBD' as varchar) as distributorid,
                          st.customer_code,
                          st.customer_group,
                          st.distribution_channel,
                          sum((
                            st.net_sales_usd * exch_rt.exch_rate
                          )) as curr_net_sell_out
                        from (
                          (
                            itg_cbd_gt_sales_report_fact as st
                              left join itg_mds_th_lcm_exchange_rate as exch_rt
                                on (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            trim(upper(cast((
                                              exch_rt.cntry_key
                                            ) as text))) = cast('CBD' as text)
                                          )
                                          and (
                                            trim(upper(cast((
                                              exch_rt.from_ccy
                                            ) as text))) = cast('USD' as text)
                                          )
                                        )
                                        and (
                                          trim(upper(cast((
                                            exch_rt.to_ccy
                                          ) as text))) = cast('THB' as text)
                                        )
                                      )
                                      and (
                                        st.billing_date >= cast((
                                          exch_rt.valid_from
                                        ) as DATE)
                                      )
                                    )
                                    and (
                                      st.billing_date <= cast((
                                        exch_rt.valid_to
                                      ) as DATE)
                                    )
                                  )
                                )
                          )
                          left join edw_vw_os_time_dim as cal
                            on (
                              (
                                cast((
                                  st.billing_date
                                ) as timestampntz) = cast((
                                  cal.cal_date
                                ) as timestampntz)
                              )
                            )
                        )
                        where
                          (
                            st.net_sales_usd > cast((
                              cast((
                                0
                              ) as decimal)
                            ) as decimal(19, 6))
                          )
                        group by
                          cal.cal_mnth_id,
                          st.customer_code,
                          st.customer_group,
                          st.distribution_channel
                      ) as sales
                        on (
                          (
                            (
                              (
                                cast((
                                  sales.distributorid
                                ) as text) = cast((
                                  cust_dim.distributorid
                                ) as text)
                              )
                              and (
                                cast((
                                  sales.customer_code
                                ) as text) = cast((
                                  cust_dim.arcode
                                ) as text)
                              )
                            )
                            and (
                              cast((
                                cast((
                                  sales.cal_mnth_id
                                ) as varchar)
                              ) as text) = cust_dim.snap_shot_month
                            )
                          )
                        )
                    )
                  ) as curr_mon_stores
                  left join (
                    select
                      cust_status.cal_mnth_id as year_month,
                      cust_status.distributorid as distributor_id,
                      cust_status.arcode as cust_cd,
                      cust_status.actv_status as prev_actv_status,
                      coalesce(
                        sales.prev_net_sell_out,
                        cast((
                          cast((
                            0
                          ) as decimal)
                        ) as decimal(18, 0))
                      ) as prev_net_sell_out
                    from (
                      (
                        select
                          cast((
                            cal.cal_mnth_id
                          ) as varchar) as cal_mnth_id,
                          cal.l1_month,
                          cal.l3_month,
                          cust.dstrbtr_id as distributorid,
                          cust.ar_cd as arcode,
                          MAX(cust.actv_status) as actv_status
                        from (
                          itg_cbd_gt_customer_snapshot as cust
                            left join (
                              select distinct
                                edw_vw_os_time_dim.cal_mnth_id,
                                to_char(
                                  cast(dateadd(
                                    month,
                                    (
                                      -cast((
                                        1
                                      ) as bigint)
                                    ),
                                    cast(cast((
                                      to_date(
                                        cast((
                                          cast((
                                            edw_vw_os_time_dim.cal_mnth_id
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
                                ) as l1_month,
                                to_char(
                                  cast(dateadd(
                                    month,
                                    (
                                      -cast((
                                        3
                                      ) as bigint)
                                    ),
                                    cast(cast((
                                      to_date(
                                        cast((
                                          cast((
                                            edw_vw_os_time_dim.cal_mnth_id
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
                                ) as l3_month
                              from edw_vw_os_time_dim
                            ) as cal
                              on (
                                (
                                  (
                                    to_char(
                                      cast(cust.snapshot_date as timestampntz),
                                      cast((
                                        cast('YYYYMM' as varchar)
                                      ) as text)
                                    ) >= cal.l3_month
                                  )
                                  and (
                                    to_char(
                                      cast(cust.snapshot_date as timestampntz),
                                      cast((
                                        cast('YYYYMM' as varchar)
                                      ) as text)
                                    ) <= cal.l1_month
                                  )
                                )
                              )
                        )
                        group by
                          cast((
                            cal.cal_mnth_id
                          ) as varchar),
                          cal.l1_month,
                          cal.l3_month,
                          cust.dstrbtr_id,
                          cust.ar_cd
                      ) as cust_status
                      left join (
                        select
                          cast((
                            cal.cal_mnth_id
                          ) as varchar) as cal_mnth_id,
                          cal.l1_month,
                          cal.l3_month,
                          cast('CBD' as varchar) as distributorid,
                          sls.customer_code,
                          sum((
                            sls.net_sales_usd * exch_rt.exch_rate
                          )) as prev_net_sell_out
                        from (
                          (
                            itg_cbd_gt_sales_report_fact as sls
                              left join itg_mds_th_lcm_exchange_rate as exch_rt
                                on (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            trim(upper(cast((
                                              exch_rt.cntry_key
                                            ) as text))) = cast('CBD' as text)
                                          )
                                          and (
                                            trim(upper(cast((
                                              exch_rt.from_ccy
                                            ) as text))) = cast('USD' as text)
                                          )
                                        )
                                        and (
                                          trim(upper(cast((
                                            exch_rt.to_ccy
                                          ) as text))) = cast('THB' as text)
                                        )
                                      )
                                      and (
                                        sls.billing_date >= cast((
                                          exch_rt.valid_from
                                        ) as DATE)
                                      )
                                    )
                                    and (
                                      sls.billing_date <= cast((
                                        exch_rt.valid_to
                                      ) as DATE)
                                    )
                                  )
                                )
                          )
                          left join (
                            select distinct
                              edw_vw_os_time_dim.cal_mnth_id,
                              to_char(
                                cast(dateadd(
                                  month,
                                  (
                                    -cast((
                                      1
                                    ) as bigint)
                                  ),
                                  cast(cast((
                                    to_date(
                                      cast((
                                        cast((
                                          edw_vw_os_time_dim.cal_mnth_id
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
                              ) as l1_month,
                              to_char(
                                cast(dateadd(
                                  month,
                                  (
                                    -cast((
                                      3
                                    ) as bigint)
                                  ),
                                  cast(cast((
                                    to_date(
                                      cast((
                                        cast((
                                          edw_vw_os_time_dim.cal_mnth_id
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
                              ) as l3_month
                            from edw_vw_os_time_dim
                          ) as cal
                            on (
                              (
                                (
                                  to_char(
                                    cast(cast((
                                      cast((
                                        sls.billing_date
                                      ) as timestampntz)
                                    ) as timestampntz) as timestampntz),
                                    cast((
                                      cast('YYYYMM' as varchar)
                                    ) as text)
                                  ) >= cal.l3_month
                                )
                                and (
                                  to_char(
                                    cast(cast((
                                      cast((
                                        sls.billing_date
                                      ) as timestampntz)
                                    ) as timestampntz) as timestampntz),
                                    cast((
                                      cast('YYYYMM' as varchar)
                                    ) as text)
                                  ) <= cal.l1_month
                                )
                              )
                            )
                        )
                        where
                          (
                            sls.net_sales_usd > cast((
                              cast((
                                0
                              ) as decimal)
                            ) as decimal(19, 6))
                          )
                        group by
                          cast((
                            cal.cal_mnth_id
                          ) as varchar),
                          cal.l1_month,
                          cal.l3_month,
                          sls.customer_code
                      ) as sales
                        on (
                          (
                            (
                              (
                                (
                                  (
                                    cast((
                                      cust_status.cal_mnth_id
                                    ) as text) = cast((
                                      sales.cal_mnth_id
                                    ) as text)
                                  )
                                  and (
                                    cust_status.l1_month = sales.l1_month
                                  )
                                )
                                and (
                                  cust_status.l3_month = sales.l3_month
                                )
                              )
                              and (
                                upper(cast((
                                  cust_status.distributorid
                                ) as text)) = upper(cast((
                                  sales.distributorid
                                ) as text))
                              )
                            )
                            and (
                              upper(cast((
                                cust_status.arcode
                              ) as text)) = upper(cast((
                                sales.customer_code
                              ) as text))
                            )
                          )
                        )
                    )
                  ) as prev_mon_stores
                    on (
                      (
                        (
                          (
                            curr_mon_stores.year_month = cast((
                              prev_mon_stores.year_month
                            ) as text)
                          )
                          and (
                            upper(cast((
                              curr_mon_stores.distributor_id
                            ) as text)) = upper(cast((
                              prev_mon_stores.distributor_id
                            ) as text))
                          )
                        )
                        and (
                          upper(cast((
                            curr_mon_stores.cust_cd
                          ) as text)) = upper(cast((
                            prev_mon_stores.cust_cd
                          ) as text))
                        )
                      )
                    )
                )
              ) as cust_flag
            ) as stores
            left join (
              select distinct
                edw_vw_os_time_dim.cal_mnth_id,
                edw_vw_os_time_dim.cal_year,
                edw_vw_os_time_dim.cal_mnth_no
              from edw_vw_os_time_dim
            ) as cal
              on (
                (
                  stores.year_month = cast((
                    cast((
                      cal.cal_mnth_id
                    ) as varchar)
                  ) as text)
                )
              )
          )
          left join itg_cbd_gt_customer as cust
            on (
              (
                (
                  cast((
                    stores.distributor_id
                  ) as text) = cast((
                    cust.dstrbtr_id
                  ) as text)
                )
                and (
                  cast((
                    stores.cust_cd
                  ) as text) = cast((
                    cust.ar_cd
                  ) as text)
                )
              )
            )
        )
      )
      union all
      select
        'TARGET_SA' as identifier,
        'CBD' as cntry_cd,
        'THB' as crncy_cd,
        'Cambodia' as cntry_nm,
        tgt.period as year_month,
        cast((
          substring(cast((
            tgt.period
          ) as text), 1, 4)
        ) as INT) as "year",
        cast((
          substring(cast((
            tgt.period
          ) as text), 5, 6)
        ) as INT) as "month",
        tgt.distributorid as distributor_id,
        'Target' as retail_env,
        cast((
          coalesce(
            upper(cast((
              tgt.salegroup
            ) as text)),
            cast((
              cast('N/A' as varchar)
            ) as text)
          )
        ) as varchar) as salesarea,
        '0' as sell_out,
        '0' as gross_sell_out,
        '0' as net_sell_out,
        sum(tgt.target) as sellout_target,
        '0' as planned_call_count,
        '0' as visited_call_count,
        '0' as effective_call_count,
        '0' as coverage_stores_count,
        '0' as reactivate_stores_count,
        '0' as inactive_stores_count,
        '0' as sales_order_count,
        '0' as on_time_count,
        '0' as in_full_count,
        '0' as otif_count
      from (
        itg_mds_lcm_distributor_target_sales as tgt
          left join (
            select distinct
              itg_cbd_gt_customer.sls_grp,
              itg_cbd_gt_customer.sls_office
            from itg_cbd_gt_customer
            where
              (
                (
                  NOT itg_cbd_gt_customer.sls_grp IS NULL
                )
                and (
                  itg_cbd_gt_customer.actv_status = 1
                )
              )
          ) as cust
            on (
              (
                (
                  upper(cast((
                    cust.sls_grp
                  ) as text)) = upper(cast((
                    tgt.salegroup
                  ) as text))
                )
                and (
                  upper(cast((
                    cust.sls_office
                  ) as text)) = upper(cast((
                    tgt.saleoffice
                  ) as text))
                )
              )
            )
      )
      where
        (
          cast((
            tgt.distributorid
          ) as text) = cast('CBD' as text)
        )
      group by
        tgt.distributorid,
        upper(cast((
          tgt.salegroup
        ) as text)),
        tgt.period
    )
    union all
    select
      'TARGET_RE' as identifier,
      'CBD' as cntry_cd,
      'THB' as crncy_cd,
      'Cambodia' as cntry_nm,
      tgt.period as year_month,
      cast((
        substring(cast((
          tgt.period
        ) as text), 1, 4)
      ) as INT) as "year",
      cast((
        substring(cast((
          tgt.period
        ) as text), 5, 6)
      ) as INT) as "month",
      tgt.distributorid as distributor_id,
      cast((
        coalesce(upper(cast((
          tgt.re
        ) as text)), cast((
          cast('N/A' as varchar)
        ) as text))
      ) as varchar) as retail_env,
      'N/A' as salesarea,
      '0' as sell_out,
      '0' as gross_sell_out,
      '0' as net_sell_out,
      sum(tgt.target) as sellout_target,
      '0' as planned_call_count,
      '0' as visited_call_count,
      '0' as effective_call_count,
      '0' as coverage_stores_count,
      '0' as reactivate_stores_count,
      '0' as inactive_stores_count,
      '0' as sales_order_count,
      '0' as on_time_count,
      '0' as in_full_count,
      '0' as otif_count
    from itg_mds_lcm_distributor_target_sales_re as tgt
    where
      (
        cast((
          tgt.distributorid
        ) as text) = cast('CBD' as text)
      )
    group by
      tgt.distributorid,
      upper(cast((
        tgt.re
      ) as text)),
      tgt.period
  )
)
union all
(
  (
    (
      (
        select
          'ACTUAL' as identifier,
          'MYM' as cntry_cd,
          'THB' as crncy_cd,
          'Myanmar' as cntry_nm,
          cast((
            cal.cal_mnth_id
          ) as varchar) as year_month,
          cal.cal_year as "year",
          cal.cal_mnth_no as "month",
          'MYM' as distributor_id,
          cast((
            coalesce(
              upper(cast((
                trim(cust_master.customer_re)
              ) as text)),
              cast((
                cast('N/A' as varchar)
              ) as text)
            )
          ) as varchar) as retail_env,
          cast((
            coalesce(
              upper(cast((
                cust_master.sales_group
              ) as text)),
              cast((
                cast('N/A' as varchar)
              ) as text)
            )
          ) as varchar) as salesarea,
          sum((
            sls.total_mmk * exch_rt.exch_rate
          )) as sell_out,
          sum((
            sls.qty_sold * matl_dim.sls_prc_credit
          )) as gross_sell_out,
          sum((
            sls.total_mmk * exch_rt.exch_rate
          )) as net_sell_out,
          0 as sellout_target,
          '0' as planned_call_count,
          '0' as visited_call_count,
          '0' as effective_call_count,
          '0' as coverage_stores_count,
          '0' as reactivate_stores_count,
          '0' as inactive_stores_count,
          '0' as sales_order_count,
          '0' as on_time_count,
          '0' as in_full_count,
          '0' as otif_count
        from (
          (
            (
              (
                (
                  select
                    st.item_no,
                    st.description,
                    st.qty_sold,
                    st.foc_qty,
                    st.total_mmk,
                    st.period,
                    st.customer_group,
                    st.customer_code,
                    st.customer_name,
                    st.filename,
                    st.run_id,
                    st.crt_dttm,
                    st.updt_dttm,
                    item_master.sap_code as sap_product_code
                  from (
                    itg_mym_gt_sales_report_fact as st
                      left join itg_mds_th_mym_product_master as item_master
                        on (
                          (
                            Ltrim(cast((
                              st.item_no
                            ) as text)) = Ltrim(cast((
                              item_master.item_no
                            ) as text))
                          )
                        )
                  )
                ) as sls
                left join itg_mds_th_myanmar_customer_master as cust_master
                  on (
                    (
                      Ltrim(cast((
                        sls.customer_code
                      ) as text)) = Ltrim(cast((
                        cust_master.customer_code
                      ) as text))
                    )
                  )
              )
              left join itg_mds_th_lcm_exchange_rate as exch_rt
                on (
                  (
                    (
                      (
                        (
                          (
                            trim(upper(cast((
                              exch_rt.cntry_key
                            ) as text))) = cast('MYM' as text)
                          )
                          and (
                            trim(upper(cast((
                              exch_rt.from_ccy
                            ) as text))) = cast('MMK' as text)
                          )
                        )
                        and (
                          trim(upper(cast((
                            exch_rt.to_ccy
                          ) as text))) = cast('THB' as text)
                        )
                      )
                      and (
                        sls.period >= cast((
                          exch_rt.valid_from
                        ) as DATE)
                      )
                    )
                    and (
                      sls.period <= cast((
                        exch_rt.valid_to
                      ) as DATE)
                    )
                  )
                )
            )
            left join edw_vw_os_time_dim as cal
              on (
                (
                  cast((
                    sls.period
                  ) as timestampntz) = cast((
                    cal.cal_date
                  ) as timestampntz)
                )
              )
          )
          left join itg_th_dstrbtr_material_dim as matl_dim
            on (
              (
                Ltrim(
                  cast((
                    cast((
                      sls.sap_product_code
                    ) as varchar)
                  ) as text),
                  cast((
                    cast((
                      0
                    ) as varchar)
                  ) as text)
                ) = Ltrim(
                  cast((
                    matl_dim.item_cd
                  ) as text),
                  cast((
                    cast((
                      0
                    ) as varchar)
                  ) as text)
                )
              )
            )
        )
        group by
          cal.cal_mnth_id,
          cal.cal_year,
          cal.cal_mnth_no,
          trim(cust_master.customer_re),
          cust_master.sales_group
        union all
        select
          'ACTUAL' as identifier,
          'MYM' as cntry_cd,
          'THB' as crncy_cd,
          'Myanmar' as cntry_nm,
          cast((
            cal.cal_mnth_id
          ) as varchar) as year_month,
          cal.cal_year as "year",
          cal.cal_mnth_no as "month",
          cust.dstrbtr_id as distributor_id,
          cast((
            coalesce(upper(cast((
              trim(cust.grp_nm)
            ) as text)), cast((
              cast('N/A' as varchar)
            ) as text))
          ) as varchar) as retail_env,
          cast((
            coalesce(
              upper(cast((
                cust.sls_grp
              ) as text)),
              cast((
                cast('N/A' as varchar)
              ) as text)
            )
          ) as varchar) as salesarea,
          '0' as sell_out,
          '0' as gross_sell_out,
          '0' as net_sell_out,
          '0' as sellout_target,
          '0' as planned_call_count,
          '0' as visited_call_count,
          '0' as effective_call_count,
          count(*) as coverage_stores_count,
          '0' as reactivate_stores_count,
          '0' as inactive_stores_count,
          '0' as sales_order_count,
          '0' as on_time_count,
          '0' as in_full_count,
          '0' as otif_count
        from (
          itg_mym_gt_customer_snapshot as cust
            left join edw_vw_os_time_dim as cal
              on (
                (
cust.snapshot_date::date = cal.cal_date::date
                )
              )
        )
        where
          (
            trim(cast((
              cast((
                cust.actv_status
              ) as varchar)
            ) as text)) = cast((
              cast('1' as varchar)
            ) as text)
          )
        group by
          cal.cal_mnth_id,
          cal.cal_year,
          cal.cal_mnth_no,
          cust.dstrbtr_id,
          coalesce(upper(cast((
            trim(cust.grp_nm)
          ) as text)), cast((
            cast('N/A' as varchar)
          ) as text)),
          cust.sls_grp
      )
      union all
      select
        'ACTUAL' as identifier,
        'MYM' as cntry_cd,
        'THB' as crncy_cd,
        'Myanmar' as cntry_nm,
        cast((
          stores.year_month
        ) as varchar) as year_month,
        cal.cal_year as "year",
        cal.cal_mnth_no as "month",
        stores.distributor_id,
        cast((
          coalesce(upper(cast((
            cust.re_nm
          ) as text)), cast((
            cast('N/A' as varchar)
          ) as text))
        ) as varchar) as retail_env,
        cast((
          coalesce(
            upper(cast((
              cust.sls_grp
            ) as text)),
            cast((
              cast('N/A' as varchar)
            ) as text)
          )
        ) as varchar) as salesarea,
        0 as sell_out,
        0 as gross_sell_out,
        0 as net_sell_out,
        '0' as sellout_target,
        '0' as planned_call_count,
        '0' as visited_call_count,
        '0' as effective_call_count,
        '0' as coverage_stores_count,
        stores.reactivate_stores as reactivate_stores_count,
        stores.inactive_store as inactive_stores_count,
        '0' as sales_order_count,
        '0' as on_time_count,
        '0' as in_full_count,
        '0' as otif_count
      from (
        (
          (
            select
              cust_flag.year_month,
              cust_flag.distributor_id,
              cust_flag.cust_cd,
              cust_flag.curr_actv_status,
              cust_flag.curr_net_sell_out,
              cust_flag.prev_actv_status,
              cust_flag.prev_net_sell_out,
              case
                when (
                  (
                    (
                      (
                        cust_flag.curr_net_sell_out > cast((
                          cast((
                            0
                          ) as decimal)
                        ) as decimal(18, 0))
                      )
                      and (
                        cust_flag.prev_net_sell_out <= cast((
                          cast((
                            0
                          ) as decimal)
                        ) as decimal(18, 0))
                      )
                    )
                    and (
                      cust_flag.curr_actv_status = 1
                    )
                  )
                  and (
                    cust_flag.prev_actv_status = 1
                  )
                )
                then cast('1' as varchar)
                else cast('0' as varchar)
              end as reactivate_stores,
              case
                when (
                  (
                    cust_flag.prev_net_sell_out <= cast((
                      cast((
                        0
                      ) as decimal)
                    ) as decimal(18, 0))
                  )
                  and (
                    cust_flag.prev_actv_status = 1
                  )
                )
                then cast('1' as varchar)
                else cast('0' as varchar)
              end as inactive_store
            from (
              select
                curr_mon_stores.year_month,
                curr_mon_stores.distributor_id,
                curr_mon_stores.cust_cd,
                curr_mon_stores.curr_actv_status,
                coalesce(
                  curr_mon_stores.curr_net_sell_out,
                  cast((
                    cast((
                      0
                    ) as decimal)
                  ) as decimal(18, 0))
                ) as curr_net_sell_out,
                prev_mon_stores.prev_actv_status,
                coalesce(
                  prev_mon_stores.prev_net_sell_out,
                  cast((
                    cast((
                      0
                    ) as decimal)
                  ) as decimal(18, 0))
                ) as prev_net_sell_out
              from (
                (
                  select
                    cust_dim.snap_shot_month as year_month,
                    cust_dim.distributorid as distributor_id,
                    cust_dim.arcode as cust_cd,
                    cust_dim.actv_status as curr_actv_status,
                    coalesce(
                      sales.curr_net_sell_out,
                      cast((
                        cast((
                          0
                        ) as decimal)
                      ) as decimal(18, 0))
                    ) as curr_net_sell_out
                  from (
                    (
                      select distinct
                        to_char(
                          cast(cust.snapshot_date as timestampntz),
                          cast((
                            cast('YYYYMM' as varchar)
                          ) as text)
                        ) as snap_shot_month,
                        cust.dstrbtr_id as distributorid,
                        cust.ar_cd as arcode,
                        cust.sls_grp,
                        cust.re_nm,
                        cust.sls_emp,
                        cust.sls_office,
                        cust.actv_status
                      from itg_mym_gt_customer_snapshot as cust
                    ) as cust_dim
                    left join (
                      select
                        cal.cal_mnth_id,
                        cast('MYM' as varchar) as distributorid,
                        st.customer_code,
                        sum((
                          st.total_mmk * exch_rt.exch_rate
                        )) as curr_net_sell_out
                      from (
                        (
                          itg_mym_gt_sales_report_fact as st
                            left join itg_mds_th_lcm_exchange_rate as exch_rt
                              on (
                                (
                                  (
                                    (
                                      (
                                        (
                                          trim(upper(cast((
                                            exch_rt.cntry_key
                                          ) as text))) = cast('MYM' as text)
                                        )
                                        and (
                                          trim(upper(cast((
                                            exch_rt.from_ccy
                                          ) as text))) = cast('MMK' as text)
                                        )
                                      )
                                      and (
                                        trim(upper(cast((
                                          exch_rt.to_ccy
                                        ) as text))) = cast('THB' as text)
                                      )
                                    )
                                    and (
                                      st.period >= cast((
                                        exch_rt.valid_from
                                      ) as DATE)
                                    )
                                  )
                                  and (
                                    st.period <= cast((
                                      exch_rt.valid_to
                                    ) as DATE)
                                  )
                                )
                              )
                        )
                        left join edw_vw_os_time_dim as cal
                          on (
                            (
                              cast((
                                st.period
                              ) as timestampntz) = cast((
                                cal.cal_date
                              ) as timestampntz)
                            )
                          )
                      )
                      where
                        (
                          st.total_mmk > cast((
                            cast((
                              0
                            ) as decimal)
                          ) as decimal(19, 6))
                        )
                      group by
                        cal.cal_mnth_id,
                        st.customer_code
                    ) as sales
                      on (
                        (
                          (
                            (
                              cast((
                                sales.distributorid
                              ) as text) = cast((
                                cust_dim.distributorid
                              ) as text)
                            )
                            and (
                              cast((
                                sales.customer_code
                              ) as text) = cast((
                                cust_dim.arcode
                              ) as text)
                            )
                          )
                          and (
                            cast((
                              cast((
                                sales.cal_mnth_id
                              ) as varchar)
                            ) as text) = cust_dim.snap_shot_month
                          )
                        )
                      )
                  )
                ) as curr_mon_stores
                left join (
                  select
                    cust_status.cal_mnth_id as year_month,
                    cust_status.distributorid as distributor_id,
                    cust_status.arcode as cust_cd,
                    cust_status.actv_status as prev_actv_status,
                    coalesce(
                      sales.prev_net_sell_out,
                      cast((
                        cast((
                          0
                        ) as decimal)
                      ) as decimal(18, 0))
                    ) as prev_net_sell_out
                  from (
                    (
                      select
                        cast((
                          cal.cal_mnth_id
                        ) as varchar) as cal_mnth_id,
                        cal.l1_month,
                        cal.l3_month,
                        cust.dstrbtr_id as distributorid,
                        cust.ar_cd as arcode,
                        MAX(cust.actv_status) as actv_status
                      from (
                        itg_mym_gt_customer_snapshot as cust
                          left join (
                            select distinct
                              edw_vw_os_time_dim.cal_mnth_id,
                              to_char(
                                cast(dateadd(
                                  month,
                                  (
                                    -cast((
                                      1
                                    ) as bigint)
                                  ),
                                  cast(cast((
                                    to_date(
                                      cast((
                                        cast((
                                          edw_vw_os_time_dim.cal_mnth_id
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
                              ) as l1_month,
                              to_char(
                                cast(dateadd(
                                  month,
                                  (
                                    -cast((
                                      3
                                    ) as bigint)
                                  ),
                                  cast(cast((
                                    to_date(
                                      cast((
                                        cast((
                                          edw_vw_os_time_dim.cal_mnth_id
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
                              ) as l3_month
                            from edw_vw_os_time_dim
                          ) as cal
                            on (
                              (
                                (
                                  to_char(
                                    cast(cust.snapshot_date as timestampntz),
                                    cast((
                                      cast('YYYYMM' as varchar)
                                    ) as text)
                                  ) >= cal.l3_month
                                )
                                and (
                                  to_char(
                                    cast(cust.snapshot_date as timestampntz),
                                    cast((
                                      cast('YYYYMM' as varchar)
                                    ) as text)
                                  ) <= cal.l1_month
                                )
                              )
                            )
                      )
                      group by
                        cast((
                          cal.cal_mnth_id
                        ) as varchar),
                        cal.l1_month,
                        cal.l3_month,
                        cust.dstrbtr_id,
                        cust.ar_cd
                    ) as cust_status
                    left join (
                      select
                        cast((
                          cal.cal_mnth_id
                        ) as varchar) as cal_mnth_id,
                        cal.l1_month,
                        cal.l3_month,
                        cast('MYM' as varchar) as distributorid,
                        sls.customer_code,
                        sum((
                          sls.total_mmk * exch_rt.exch_rate
                        )) as prev_net_sell_out
                      from (
                        (
                          itg_mym_gt_sales_report_fact as sls
                            left join itg_mds_th_lcm_exchange_rate as exch_rt
                              on (
                                (
                                  (
                                    (
                                      (
                                        (
                                          trim(upper(cast((
                                            exch_rt.cntry_key
                                          ) as text))) = cast('MYM' as text)
                                        )
                                        and (
                                          trim(upper(cast((
                                            exch_rt.from_ccy
                                          ) as text))) = cast('MMK' as text)
                                        )
                                      )
                                      and (
                                        trim(upper(cast((
                                          exch_rt.to_ccy
                                        ) as text))) = cast('THB' as text)
                                      )
                                    )
                                    and (
                                      sls.period >= cast((
                                        exch_rt.valid_from
                                      ) as DATE)
                                    )
                                  )
                                  and (
                                    sls.period <= cast((
                                      exch_rt.valid_to
                                    ) as DATE)
                                  )
                                )
                              )
                        )
                        left join (
                          select distinct
                            edw_vw_os_time_dim.cal_mnth_id,
                            to_char(
                              cast(dateadd(
                                month,
                                (
                                  -cast((
                                    1
                                  ) as bigint)
                                ),
                                cast(cast((
                                  to_date(
                                    cast((
                                      cast((
                                        edw_vw_os_time_dim.cal_mnth_id
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
                            ) as l1_month,
                            to_char(
                              cast(dateadd(
                                month,
                                (
                                  -cast((
                                    3
                                  ) as bigint)
                                ),
                                cast(cast((
                                  to_date(
                                    cast((
                                      cast((
                                        edw_vw_os_time_dim.cal_mnth_id
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
                            ) as l3_month
                          from edw_vw_os_time_dim
                        ) as cal
                          on (
                            (
                              (
                                to_char(
                                  cast(cast((
                                    cast((
                                      sls.period
                                    ) as timestampntz)
                                  ) as timestampntz) as timestampntz),
                                  cast((
                                    cast('YYYYMM' as varchar)
                                  ) as text)
                                ) >= cal.l3_month
                              )
                              and (
                                to_char(
                                  cast(cast((
                                    cast((
                                      sls.period
                                    ) as timestampntz)
                                  ) as timestampntz) as timestampntz),
                                  cast((
                                    cast('YYYYMM' as varchar)
                                  ) as text)
                                ) <= cal.l1_month
                              )
                            )
                          )
                      )
                      where
                        (
                          sls.total_mmk > cast((
                            cast((
                              0
                            ) as decimal)
                          ) as decimal(19, 6))
                        )
                      group by
                        cast((
                          cal.cal_mnth_id
                        ) as varchar),
                        cal.l1_month,
                        cal.l3_month,
                        sls.customer_code
                    ) as sales
                      on (
                        (
                          (
                            (
                              (
                                (
                                  cast((
                                    cust_status.cal_mnth_id
                                  ) as text) = cast((
                                    sales.cal_mnth_id
                                  ) as text)
                                )
                                and (
                                  cust_status.l1_month = sales.l1_month
                                )
                              )
                              and (
                                cust_status.l3_month = sales.l3_month
                              )
                            )
                            and (
                              upper(cast((
                                cust_status.distributorid
                              ) as text)) = upper(cast((
                                sales.distributorid
                              ) as text))
                            )
                          )
                          and (
                            upper(cast((
                              cust_status.arcode
                            ) as text)) = upper(cast((
                              sales.customer_code
                            ) as text))
                          )
                        )
                      )
                  )
                ) as prev_mon_stores
                  on (
                    (
                      (
                        (
                          curr_mon_stores.year_month = cast((
                            prev_mon_stores.year_month
                          ) as text)
                        )
                        and (
                          upper(cast((
                            curr_mon_stores.distributor_id
                          ) as text)) = upper(cast((
                            prev_mon_stores.distributor_id
                          ) as text))
                        )
                      )
                      and (
                        upper(cast((
                          curr_mon_stores.cust_cd
                        ) as text)) = upper(cast((
                          prev_mon_stores.cust_cd
                        ) as text))
                      )
                    )
                  )
              )
            ) as cust_flag
          ) as stores
          left join (
            select distinct
              edw_vw_os_time_dim.cal_mnth_id,
              edw_vw_os_time_dim.cal_year,
              edw_vw_os_time_dim.cal_mnth_no
            from edw_vw_os_time_dim
          ) as cal
            on (
              (
                stores.year_month = cast((
                  cast((
                    cal.cal_mnth_id
                  ) as varchar)
                ) as text)
              )
            )
        )
        left join itg_mym_gt_customer as cust
          on (
            (
              (
                cast((
                  stores.distributor_id
                ) as text) = cast((
                  cust.dstrbtr_id
                ) as text)
              )
              and (
                cast((
                  stores.cust_cd
                ) as text) = cast((
                  cust.ar_cd
                ) as text)
              )
            )
          )
      )
    )
    union all
    select
      'TARGET_SA' as identifier,
      'MYM' as cntry_cd,
      'THB' as crncy_cd,
      'Myanmar' as cntry_nm,
      tgt.period as year_month,
      cast((
        substring(cast((
          tgt.period
        ) as text), 1, 4)
      ) as INT) as "year",
      cast((
        substring(cast((
          tgt.period
        ) as text), 5, 6)
      ) as INT) as "month",
      tgt.distributorid as distributor_id,
      'Target' as retail_env,
      cast((
        coalesce(
          upper(cast((
            tgt.salegroup
          ) as text)),
          cast((
            cast('N/A' as varchar)
          ) as text)
        )
      ) as varchar) as salesarea,
      '0' as sell_out,
      '0' as gross_sell_out,
      '0' as net_sell_out,
      sum(tgt.target) as sellout_target,
      '0' as planned_call_count,
      '0' as visited_call_count,
      '0' as effective_call_count,
      '0' as coverage_stores_count,
      '0' as reactivate_stores_count,
      '0' as inactive_stores_count,
      '0' as sales_order_count,
      '0' as on_time_count,
      '0' as in_full_count,
      '0' as otif_count
    from (
      itg_mds_lcm_distributor_target_sales as tgt
        left join (
          select distinct
            itg_mym_gt_customer.sls_grp,
            itg_mym_gt_customer.sls_office
          from itg_mym_gt_customer
          where
            (
              (
                NOT itg_mym_gt_customer.sls_grp IS NULL
              )
              and (
                itg_mym_gt_customer.actv_status = 1
              )
            )
        ) as cust
          on (
            (
              (
                upper(cast((
                  cust.sls_grp
                ) as text)) = upper(cast((
                  tgt.salegroup
                ) as text))
              )
              and (
                upper(cast((
                  cust.sls_office
                ) as text)) = upper(cast((
                  tgt.saleoffice
                ) as text))
              )
            )
          )
    )
    where
      (
        trim(upper(cast((
          tgt.distributorid
        ) as text))) = cast('MYM' as text)
      )
    group by
      tgt.distributorid,
      upper(cast((
        tgt.salegroup
      ) as text)),
      tgt.period
  )
  union all
  select
    'TARGET_RE' as identifier,
    'MYM' as cntry_cd,
    'THB' as crncy_cd,
    'Myanmar' as cntry_nm,
    tgt.period as year_month,
    cast((
      substring(cast((
        tgt.period
      ) as text), 1, 4)
    ) as INT) as "year",
    cast((
      substring(cast((
        tgt.period
      ) as text), 5, 6)
    ) as INT) as "month",
    tgt.distributorid as distributor_id,
    cast((
      coalesce(upper(cast((
        tgt.re
      ) as text)), cast((
        cast('N/A' as varchar)
      ) as text))
    ) as varchar) as retail_env,
    'N/A' as salesarea,
    '0' as sell_out,
    '0' as gross_sell_out,
    '0' as net_sell_out,
    sum(tgt.target) as sellout_target,
    '0' as planned_call_count,
    '0' as visited_call_count,
    '0' as effective_call_count,
    '0' as coverage_stores_count,
    '0' as reactivate_stores_count,
    '0' as inactive_stores_count,
    '0' as sales_order_count,
    '0' as on_time_count,
    '0' as in_full_count,
    '0' as otif_count
  from itg_mds_lcm_distributor_target_sales_re as tgt
  where
    (
      trim(upper(cast((
        tgt.distributorid
      ) as text))) = cast('MYM' as text)
    )
  group by
    tgt.distributorid,
    upper(cast((
      tgt.re
    ) as text)),
    tgt.period
)
)
select * from final