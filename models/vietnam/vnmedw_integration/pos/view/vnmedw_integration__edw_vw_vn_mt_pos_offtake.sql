with edw_vw_vn_mt_pos_customers as (
select * from {{ ref('vnmedw_integration__edw_vw_vn_mt_pos_customers') }}
),
edw_vw_vn_mt_pos_union as (
select * from {{ ref('vnmedw_integration__edw_vw_vn_mt_pos_union') }}
),
edw_vw_os_time_dim as (
select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_crncy_exch_rates as (
select * from {{ ref('aspedw_integration__edw_crncy_exch_rates') }}
),
edw_vw_vn_mt_pos_products as (
select * from {{ ref('vnmedw_integration__edw_vw_vn_mt_pos_products') }}
),
edw_vw_vn_mt_dist_products as (
select * from {{ ref('vnmedw_integration__edw_vw_vn_mt_dist_products') }}
),
final as (
select 
  case when (
    (
      (
        upper(
          (offtake."account"):: text
        ) <> ('COOP' :: character varying):: text
      ) 
      and (
        upper(
          (offtake."account"):: text
        ) <> ('VINMART+' :: character varying):: text
      )
    ) 
    and (
      upper(
        (offtake."account"):: text
      ) <> ('BHX' :: character varying):: text
    )
  ) then (
    upper(
      (offtake."account"):: text
    )
  ):: character varying when (
    upper(
      (offtake."account"):: text
    ) = ('COOP' :: character varying):: text
  ) then 'SAIGON COOP' :: character varying when (
    upper(
      (offtake."account"):: text
    ) = ('VINMART+' :: character varying):: text
  ) then 'VINMART +' :: character varying when (
    upper(
      (offtake."account"):: text
    ) = ('BHX' :: character varying):: text
  ) then 'BACH HOA XANH' :: character varying ELSE NULL :: character varying END as account, 
  offtake.jnj_year as "year", 
  offtake.mnth_no as "month", 
  offtake.mnth_id as month_id, 
  offtake.qrtr as quarter, 
  offtake.store_code as customer_cd, 
  case when (
    upper(
      (offtake."account"):: text
    ) <> ('CON CUNG' :: character varying):: text
  ) then offtake.pos_store_name ELSE offtake.store_name END as store_name, 
  offtake.product_cd, 
  case when (
    upper(
      (offtake."account"):: text
    ) = ('GUARDIAN' :: character varying):: text
  ) then offtake.barcode ELSE offtake.pos_barcode END as barcode, 
  upper(offtake.product_name) as product_name, 
  upper(offtake.franchise) as franchise, 
  upper(offtake.category) as category, 
  upper(offtake.sub_brand) as sub_brand, 
  upper(offtake.sub_category) as sub_category, 
  offtake.quantity, 
  offtake.amount, 
  (offtake.amount * offtake.ex_rt) as amount_usd 
from 
  (
    select 
      pos.year, 
      pos.month, 
      pos."account", 
      pos.product_cd, 
      pos.customer_cd as trans_cust_cd, 
      pos.store_name, 
      pos.barcode, 
      pos.quantity, 
      exch_rate.ex_rt, 
      pos.amount, 
      timedim.jnj_year, 
      timedim.qrtr, 
      timedim.mnth_id, 
      timedim.mnth_no, 
      pos_prod.pos_barcode, 
      pos_prod.customer, 
      pos_prod.product_name, 
      pos_prod.franchise, 
      pos_prod.category, 
      pos_prod.sub_brand, 
      pos_prod.sub_category, 
      pos_cust.customer_name, 
      pos_cust.status, 
      pos_cust.store_code, 
      pos_cust.pos_store_name, 
      pos_cust.zone 
    from 
      (
        (
          (
            (
              edw_vw_vn_mt_pos_union pos 
              left join (
                select 
                  distinct time_dim."year" as jnj_year, 
                  time_dim.qrtr, 
                  time_dim.mnth_id, 
                  time_dim.mnth_no 
                from 
                  edw_vw_os_time_dim time_dim
              ) timedim ON (
                (
                  case when (
                    length(
                      (
                        (pos.month):: character varying
                      ):: text
                    ) = 1
                  ) then concat(
                    (
                      (pos.year):: character varying
                    ):: text, 
                    concat(
                      (
                        (0):: character varying
                      ):: text, 
                      (
                        (pos.month):: character varying
                      ):: text
                    )
                  ) ELSE concat(
                    (
                      (pos.year):: character varying
                    ):: text, 
                    (
                      (pos.month):: character varying
                    ):: text
                  ) END = timedim.mnth_id
                )
              )
            ) 
            left join (
              select 
                (
                  derived_table1."year" || derived_table1.mnth
                ) as mnth_id, 
                derived_table1.ex_rt 
              from 
                (
                  select 
                    edw_crncy_exch_rates.fisc_yr_per, 
                    "substring"(
                      (
                        (
                          edw_crncy_exch_rates.fisc_yr_per
                        ):: character varying
                      ):: text, 
                      1, 
                      4
                    ) as "year", 
                    "substring"(
                      (
                        (
                          edw_crncy_exch_rates.fisc_yr_per
                        ):: character varying
                      ):: text, 
                      6, 
                      2
                    ) as mnth, 
                    edw_crncy_exch_rates.ex_rt 
                  from 
                    edw_crncy_exch_rates 
                  where 
                    (
                      (
                        (
                          edw_crncy_exch_rates.from_crncy
                        ):: text = ('VND' :: character varying):: text
                      ) 
                      and (
                        (edw_crncy_exch_rates.to_crncy):: text = ('USD' :: character varying):: text
                      )
                    )
                ) derived_table1
            ) exch_rate ON (
              (
                timedim.mnth_id = exch_rate.mnth_id
              )
            )
          ) 
          left join (
            select 
              prods.barcode as pos_barcode, 
              prods.customer, 
              prods.customer_sku, 
              prods.product_name, 
              dst_prod.franchise, 
              dst_prod.category, 
              dst_prod.sub_brand, 
              dst_prod.sub_category 
            from 
              (
                (
                  select 
                    distinct edw_vw_vn_mt_pos_products.barcode, 
                    upper(
                      (
                        edw_vw_vn_mt_pos_products.customer
                      ):: text
                    ) as customer, 
                    upper(
                      (edw_vw_vn_mt_pos_products.name):: text
                    ) as product_name, 
                    edw_vw_vn_mt_pos_products.customer_sku 
                  from 
                    edw_vw_vn_mt_pos_products
                ) prods 
                left join (
                  select 
                    distinct edw_vw_vn_mt_dist_products.barcode, 
                    upper(
                      edw_vw_vn_mt_dist_products.franchise
                    ) as franchise, 
                    upper(
                      edw_vw_vn_mt_dist_products.category
                    ) as category, 
                    upper(
                      edw_vw_vn_mt_dist_products.sub_brand
                    ) as sub_brand, 
                    upper(
                      edw_vw_vn_mt_dist_products.sub_category
                    ) as sub_category, 
                    row_number() OVER(
                      partition by edw_vw_vn_mt_dist_products.barcode order by null
                    ) as rn 
                  from 
                    edw_vw_vn_mt_dist_products
                ) dst_prod ON (
                  (
                    trim(prods.barcode):: text = trim(dst_prod.barcode):: text
                  )
                )
              ) 
            where 
              (dst_prod.rn = 1)
          ) pos_prod ON (
            (
              (
                "replace"(
                  trim(pos.product_cd):: text, 
                  ('\'' :: character varying):: text, 
                  ('' :: character varying):: text
                ) = "replace"(
                  trim(pos_prod.customer_sku):: text, 
                  ('\'' :: character varying):: text, 
                  ('' :: character varying):: text
                )
              ) 
              and (
                "replace"(
                  upper(
                    (pos."account"):: text
                  ), 
                  ('+' :: character varying):: text, 
                  ('' :: character varying):: text
                ) = upper(pos_prod.customer)
              )
            )
          )
        ) 
        left join (
          select 
            edw_vw_vn_mt_pos_customers.customer_name, 
            edw_vw_vn_mt_pos_customers.chain, 
            edw_vw_vn_mt_pos_customers.customer_store_code, 
            edw_vw_vn_mt_pos_customers.status, 
            edw_vw_vn_mt_pos_customers.store_code, 
            edw_vw_vn_mt_pos_customers.store_name as pos_store_name, 
            edw_vw_vn_mt_pos_customers.zone 
          from 
            edw_vw_vn_mt_pos_customers
        ) pos_cust ON (
          (
            (
              upper(
                trim(
                  "replace"(
                    "replace"(
                      (pos.customer_cd):: text, 
                      ('_' :: character varying):: text, 
                      ('' :: character varying):: text
                    ), 
                    (' ' :: character varying):: text, 
                    ('' :: character varying):: text
                  )
                )
              ) = upper(
                trim(
                  "replace"(
                    "replace"(
                      (pos_cust.store_code):: text, 
                      ('_' :: character varying):: text, 
                      ('' :: character varying):: text
                    ), 
                    (' ' :: character varying):: text, 
                    ('' :: character varying):: text
                  )
                )
              )
            ) 
            and (
              "replace"(
                upper(
                  (pos_cust.customer_name):: text
                ), 
                ('+' :: character varying):: text, 
                ('' :: character varying):: text
              ) = "replace"(
                upper(
                  (pos."account"):: text
                ), 
                ('+' :: character varying):: text, 
                ('' :: character varying):: text
              )
            )
          )
        )
      )
  ) offtake
)
select * from final