with itg_vn_mt_sellout_vinmart as (
select * from {{ ref('vnmitg_integration__itg_vn_mt_sellout_vinmart') }}
),
itg_vn_mt_sellout_aeon as (
select * from {{ ref('vnmitg_integration__itg_vn_mt_sellout_aeon') }}
),
itg_vn_mt_pos_cust_master as (
select * from {{ ref('vnmitg_integration__itg_vn_mt_pos_cust_master') }}
),
itg_vn_mt_sellout_bhx as (
select * from {{ ref('vnmitg_integration__itg_vn_mt_sellout_bhx') }}
),
itg_vn_mt_sellout_mega as (
select * from {{ ref('vnmitg_integration__itg_vn_mt_sellout_mega') }}
),
itg_vn_mt_sellout_coop as (
select * from {{ ref('vnmitg_integration__itg_vn_mt_sellout_coop') }}
),
itg_vn_mt_sellout_lotte as (
select * from {{ ref('vnmitg_integration__itg_vn_mt_sellout_lotte') }}
),
itg_vn_mt_sellout_guardian as (
select * from {{ ref('vnmitg_integration__itg_vn_mt_sellout_guardian') }}
),
itg_vn_mt_sellout_con_cung as (
select * from {{ ref('vnmitg_integration__itg_vn_mt_sellout_con_cung') }}
),
edw_vw_vn_mt_pos_products as (
select * from {{ ref('vnmedw_integration__edw_vw_vn_mt_pos_products') }}
),
edw_vw_vn_mt_dist_products as (
select * from {{ ref('vnmedw_integration__edw_vw_vn_mt_dist_products') }}
),
edw_vw_vn_mt_pos_price_products as (
select * from {{ ref('vnmedw_integration__edw_vw_vn_mt_pos_price_products') }}
),
final as (
select 
  (pos.year):: integer as year, 
  (pos.month):: integer as month, 
  pos."account", 
  pos.customer_cd, 
  pos.store_name, 
  pos.product_cd, 
  pos.barcode, 
  pos.quantity, 
  pos.amount, 
  current_timestamp()::timestamp_ntz(9) as updt_dttm 
from 
  (
    (
      (
        (
          (
            (
              (
                (
                  select 
                    distinct 'Vinmart' :: character varying as "account", 
                    a.year, 
                    a.month, 
                    a.store as customer_cd, 
                    a.store_name, 
                    a.article as product_cd, 
                    NULL :: character varying as barcode, 
                    sum(a.pos_quantity) over(
                      partition by a.store, 
                      a.article, 
                      a.store_name, 
                      concat(
                        (a.year):: text, 
                        (a.month):: text
                      )
                      order by null
                      rows between unbounded preceding 
                      and unbounded following 
                    ) as quantity, 
                    sum(
                      (a.pos_revenue * 0.88)
                    ) over(
                      partition by a.store, 
                      a.article, 
                      a.store_name, 
                      concat(
                        (a.year):: text, 
                        (a.month):: text
                      ) 
                      order by null
                      rows between unbounded preceding 
                      and unbounded following
                    ) as amount 
                  from 
                    itg_vn_mt_sellout_vinmart a 
                  where 
                    (
                      lower(
                        (a.filename):: text
                      ) like (
                        'vinmart_sell_out%' :: character varying
                      ):: text
                    ) 
                  union all 
                  select 
                    distinct 'Vinmart+' :: character varying as "account", 
                    a.year, 
                    a.month, 
                    a.store as customer_cd, 
                    a.store_name, 
                    a.article as product_cd, 
                    NULL :: character varying as barcode, 
                    sum(a.pos_quantity) over(
                      partition by a.store, 
                      a.article, 
                      a.store_name, 
                      concat(
                        (a.year):: text, 
                        (a.month):: text
                      ) 
                      order by null
                      rows between unbounded preceding 
                      and unbounded following
                    ) as quantity, 
                    sum(
                      (a.pos_revenue * 0.88)
                    ) over(
                      partition by a.store, 
                      a.article, 
                      a.store_name, 
                      concat(
                        (a.year):: text, 
                        (a.month):: text
                      ) 
                      order by null
                      rows between unbounded preceding 
                      and unbounded following
                    ) as amount 
                  from 
                    itg_vn_mt_sellout_vinmart a 
                  where 
                    (
                      lower(
                        (a.filename):: text
                      ) like (
                        'vinmart+_sell_out%' :: character varying
                      ):: text
                    )
                ) 
                union all 
                select 
                  distinct 'Aeon' :: character varying as "account", 
                  a.year, 
                  a.month, 
                  (
                    "replace"(
                      (a.store):: text, 
                      ('\'' :: character varying):: text, 
                      ('' :: character varying):: text
                    )
                  ):: character varying as customer_cd, 
                  s.store_name, 
                  a.item as product_cd, 
                  NULL :: character varying as barcode, 
                  sum(a.sales_quantity) over(
                    partition by "replace"(
                      (a.store):: text, 
                      ('\'' :: character varying):: text, 
                      ('' :: character varying):: text
                    ), 
                    a.item, 
                    s.store_name, 
                    concat(
                      (a.year):: text, 
                      (a.month):: text
                    ) 
                    order by null
                    rows between unbounded preceding 
                    and unbounded following
                  ) as quantity, 
                  sum(
                    (a.sales_amount * 0.88)
                  ) over(
                    partition by replace(
                      (a.store):: text, 
                      ('\'' :: character varying):: text, 
                      ('' :: character varying):: text
                    ), 
                    a.item, 
                    s.store_name, 
                    concat(
                      (a.year):: text, 
                      (a.month):: text
                    ) 
                    order by null
                    rows between unbounded preceding 
                    and unbounded following
                  ) as amount 
                from 
                  (
                    itg_vn_mt_sellout_aeon a 
                    LEFT JOIN (
                      select 
                        b.store_code, 
                        b.store_name, 
                        b.customer_name 
                      from 
                        itg_vn_mt_pos_cust_master b 
                      where 
                        (
                          (
                            (b.active):: text = 'Y' :: text
                          ) 
                          AND (
                            upper(
                              (b.customer_name):: text
                            ) = ('AEON' :: character varying):: text
                          )
                        )
                    ) s ON (
                      (
                        (s.store_code):: text = "replace"(
                          (a.store):: text, 
                          ('\'' :: character varying):: text, 
                          ('' :: character varying):: text
                        )
                      )
                    )
                  ) 
                where 
                  (
                    lower(
                      (a.filename):: text
                    ) like (
                      'aeon_sell_out%' :: character varying
                    ):: text
                  )
              ) 
              union all 
              select 
                distinct 'BHX' :: character varying as "account", 
                a.year, 
                a.month, 
                a.cust_code as customer_cd, 
                a.cust_name, 
                a.pro_code as product_cd, 
                NULL :: character varying as barcode, 
                sum(a.quantity) over(
                  partition by a.cust_code, 
                  a.pro_code, 
                  a.cust_name, 
                  concat(
                    (a.year):: text, 
                    (a.month):: text
                  ) 
                  order by null
                  rows between unbounded preceding 
                  and unbounded following
                ) as quantity, 
                sum(
                  (a.amount * 0.88)
                ) over(
                  partition by a.cust_code, 
                  a.pro_code, 
                  a.cust_name, 
                  concat(
                    (a.year):: text, 
                    (a.month):: text
                  ) 
                  order by null
                  rows between unbounded preceding 
                  and unbounded following
                ) as amount 
              from 
                itg_vn_mt_sellout_bhx a 
              where 
                (
                  lower(
                    (a.filename):: text
                  ) like (
                    'bhx_sell_out%' :: character varying
                  ):: text
                )
            ) 
            union all 
            select 
              distinct 'Mega' :: character varying as "account", 
              a.year, 
              a.month, 
              a.site_no as customer_cd, 
              a.site_name as store_name, 
              a.art_no as product_cd, 
              NULL :: character varying as barcode, 
              sum(a.sale_qty) over(
                partition by a.site_no, 
                a.art_no, 
                a.site_name, 
                concat(
                  (a.year):: text, 
                  (a.month):: text
                ) 
                order by null
                rows between unbounded preceding 
                and unbounded following
              ) as quantity, 
              sum(a.cogs_amt) over(
                partition by a.site_no, 
                a.art_no, 
                a.site_name, 
                concat(
                  (a.year):: text, 
                  (a.month):: text
                )
                order by null
                rows between unbounded preceding 
                and unbounded following
              ) as amount 
            from 
              itg_vn_mt_sellout_mega a
          ) 
          union all 
          select 
            distinct coop."account", 
            coop.year, 
            coop.month, 
            coop.customer_cd, 
            coop.store_name, 
            coop.sku as product_cd, 
            coop.barcode, 
            sum(coop.qty) over(
              partition by coop.customer_cd, 
              coop.sku, 
              coop.store_name, 
              concat(
                (coop.year):: text, 
                (coop.month):: text
              ) 
              order by null
              rows between unbounded preceding 
              and unbounded following
            ) as quantity, 
            sum(
              (coop.sales_amount * 0.88)
            ) over(
              partition by coop.customer_cd, 
              coop.sku, 
              coop.store_name, 
              concat(
                (coop.year):: text, 
                (coop.month):: text
              ) 
              order by null
              rows between unbounded preceding 
              and unbounded following
            ) as amount 
          from 
            (
              select 
                main.year, 
                main.month, 
                main.thang, 
                main.desc_a, 
                main.idept, 
                main.isdept, 
                main.iclas, 
                main.isclas, 
                main.sku, 
                main.tenvt, 
                main.brand_spm, 
                main.madv, 
                main.sumoflg, 
                main.sumofttbvnckhd, 
                main.store, 
                main.sales_amount, 
                (
                  main.sales_amount / lp.list_price
                ) as qty, 
                'COOP' :: character varying as "account", 
                main.store as customer_cd, 
                main.store as store_name, 
                NULL :: character varying as barcode 
              from 
                itg_vn_mt_sellout_coop main, 
                (
                  select 
                    distinct itg_vn_mt_sellout_coop.year, 
                    itg_vn_mt_sellout_coop.month, 
                    itg_vn_mt_sellout_coop.thang, 
                    itg_vn_mt_sellout_coop.desc_a, 
                    itg_vn_mt_sellout_coop.sku, 
                    (
                      itg_vn_mt_sellout_coop.sumofttbvnckhd / (
                        (itg_vn_mt_sellout_coop.sumoflg):: numeric
                      ):: numeric(18, 0)
                    ) as list_price 
                  from 
                    itg_vn_mt_sellout_coop
                ) lp 
              where 
                (
                  (
                    (
                      (
                        (
                          (main.year):: text = (lp.year):: text
                        ) 
                        AND (
                          (main.month):: text = (lp.month):: text
                        )
                      ) 
                      AND (
                        (main.thang):: text = (lp.thang):: text
                      )
                    ) 
                    AND (
                      (main.desc_a):: text = (lp.desc_a):: text
                    )
                  ) 
                  AND (
                    (main.sku):: text = (lp.sku):: text
                  )
                )
            ) coop
        ) 
        union all 
        select 
          distinct 'Lotte' :: character varying as "account", 
          a.year, 
          a.month, 
          a.str as customer_cd, 
          a.str_nm as store_name, 
          a.prod_cd as product_cd, 
          NULL :: character varying as barcode, 
          sum(a.sale_qty) over(
            partition by a.str, 
            a.prod_cd, 
            a.str_nm, 
            concat(
              (a.year):: text, 
              (a.month):: text
            ) 
            order by null
            rows between unbounded preceding 
            and unbounded following
          ) as quantity, 
          sum(
            (
              (a.tot_sale_amt * 0.88) / 1.1
            )
          ) over(
            partition by a.str, 
            a.prod_cd, 
            a.str_nm, 
            concat(
              (a.year):: text, 
              (a.month):: text
            )
            order by null
            rows between unbounded preceding 
            and unbounded following
          ) as amount 
        from 
          itg_vn_mt_sellout_lotte a
      ) 
      union all 
      select 
        distinct 'Guardian' :: character varying as "account", 
        a.year, 
        a.month, 
        a.store_code as customer_cd, 
        a.store_name, 
        a.sku as product_cd, 
        a.barcode, 
        sum(a.sales_supplier) over(
          partition by a.store_code, 
          a.sku, 
          a.store_name, 
          concat(
            (a.year):: text, 
            (a.month):: text
          ) 
          order by null
          rows between unbounded preceding 
          and unbounded following
        ) as quantity, 
        sum(
          (a.amount * 0.88)
        ) over(
          partition by a.store_code, 
          a.sku, 
          a.store_name, 
          concat(
            (a.year):: text, 
            (a.month):: text
          )
          order by null
          rows between unbounded preceding 
          and unbounded following
        ) as amount 
      from 
        itg_vn_mt_sellout_guardian a
    ) 
    union all 
    select 
      distinct pos."account", 
      pos.year, 
      pos.month, 
      pos.customer_cd, 
      pos.store_name, 
      pos.product_cd, 
      pos_prod.barcode, 
      sum(pos.quantity) over(
        partition by pos.customer_cd, 
        pos.product_cd, 
        pos.store_name, 
        concat(
          (pos.year):: text, 
          (pos.month):: text
        ) 
        order by null
        rows between unbounded preceding 
        and unbounded following
      ) as quantity, 
      sum(
        (
          (
            (pos.quantity):: numeric
          ):: numeric(18, 0) * pos_prod.price
        )
      ) over(
        partition by pos.customer_cd, 
        pos.product_cd, 
        pos.store_name, 
        concat(
          (pos.year):: text, 
          (pos.month):: text
        )
        order by null
        rows between unbounded preceding 
        and unbounded following
      ) as amount 
    from 
      (
        (
          select 
            'Con Cung' :: character varying as "account", 
            a.year, 
            a.month, 
            a.store as customer_cd, 
            a.store as store_name, 
            a.product_code as product_cd, 
            a.quantity, 
            a.filename, 
            a.crtd_dttm 
          from 
            itg_vn_mt_sellout_con_cung a
        ) pos 
        LEFT JOIN (
          select 
            distinct prods.barcode, 
            prods.customer, 
            prods.customer_sku, 
            pprod.pc_per_case, 
            pprod.price 
          from 
            (
              (
                (
                  select 
                    edw_vw_vn_mt_pos_products.barcode, 
                    edw_vw_vn_mt_pos_products.customer, 
                    edw_vw_vn_mt_pos_products.customer_sku 
                  from 
                    edw_vw_vn_mt_pos_products 
                  where 
                    (
                      (
                        edw_vw_vn_mt_pos_products.customer
                      ):: text = ('Con Cung' :: character varying):: text
                    )
                ) prods 
                LEFT JOIN (
                  select 
                    distinct edw_vw_vn_mt_dist_products.barcode, 
                    edw_vw_vn_mt_dist_products.jnj_sap_code 
                  from 
                    edw_vw_vn_mt_dist_products
                ) dst_prod ON (
                  (
                    (prods.barcode):: text = (dst_prod.barcode):: text
                  )
                )
              ) 
              LEFT JOIN (
                select 
                  distinct edw_vw_vn_mt_pos_price_products.bar_code, 
                  edw_vw_vn_mt_pos_price_products.product_id_concung, 
                  edw_vw_vn_mt_pos_price_products.pc_per_case, 
                  edw_vw_vn_mt_pos_price_products.price 
                from 
                  edw_vw_vn_mt_pos_price_products
              ) pprod ON (
                (
                  (
                    (dst_prod.barcode):: text = (pprod.bar_code):: text
                  ) 
                  AND (
                    (prods.customer_sku):: text = (pprod.product_id_concung):: text
                  )
                )
              )
            )
        ) pos_prod ON (
          (
            (
              "replace"(
                (pos.product_cd):: text, 
                ('\'' :: character varying):: text, 
                ('' :: character varying):: text
              ) = "replace"(
                (pos_prod.customer_sku):: text, 
                ('\'' :: character varying):: text, 
                ('' :: character varying):: text
              )
            ) 
            AND (
              (pos."account"):: text = (pos_prod.customer):: text
            )
          )
        )
      )
  ) pos
)
select * from final