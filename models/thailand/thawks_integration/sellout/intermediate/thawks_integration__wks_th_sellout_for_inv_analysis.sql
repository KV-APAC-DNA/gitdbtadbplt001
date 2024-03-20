with itg_th_pos_sales_inventory_fact as (
  select * from {{ ref('thaitg_integration__itg_th_pos_sales_inventory_fact') }}
), 
itg_th_pos_customer_dim as (
 select * from {{ ref('thaitg_integration__itg_th_pos_customer_dim') }}
), 
edw_th_inventory_analysis_base_6year_filter as (
  select * from {{ ref('thaedw_integration__edw_th_inventory_analysis_base_6year_filter') }}
), 
itg_th_pos_sales_inventory_fact as (
  select * from {{ ref('thaitg_integration__itg_th_pos_sales_inventory_fact') }}
), 
sdl_th_mt_watsons as (
  select * from {{ source('thasdl_raw', 'sdl_th_mt_watsons') }}
), 
sdl_mds_th_customer_product_code as (
  select * from {{ source('thasdl_raw', 'sdl_mds_th_customer_product_code') }}
), 
sdl_mds_th_product_master as (
  select * from {{ source('thasdl_raw', 'sdl_mds_th_product_master') }}
), 
edw_vw_os_customer_dim as (
  select * from {{ ref('thaedw_integration__edw_vw_th_customer_dim') }}
), 
edw_vw_os_material_dim as (
  select * from {{ ref('thaedw_integration__edw_vw_th_material_dim') }}
), 
edw_material_dim as (
  select * from {{ ref('thaedw_integration__edw_vw_th_material_dim') }}
), 
transformed as (
  select 
    derived_table1.sap_prnt_cust_key, 
    derived_table1.dstr_nm, 
    trim(
      nvl (
        nullif(t4.pka_size_desc, ''), 
        'NA'
      )
    ) as pka_size_desc, 
    trim(
      nvl (
        nullif(t4.gph_prod_brnd, ''), 
        'NA'
      )
    ) as brand, 
    trim(
      nvl (
        nullif(t4.gph_prod_vrnt, ''), 
        'NA'
      )
    ) as variant, 
    trim(
      nvl (
        nullif(t4.gph_prod_sgmnt, ''), 
        'NA'
      )
    ) as segment, 
    trim(
      nvl (
        nullif(t4.gph_prod_ctgry, ''), 
        'NA'
      )
    ) as prod_category, 
    trim(
      nvl (
        nullif(t4.pka_product_key, ''), 
        'NA'
      )
    ) as pka_product_key, 
    to_char(
      min(derived_table1.order_date), 
      'YYYY-MM-DD'
    ) as min_date, 
    min(no_of_wks) as no_of_wks 
  from 
    (
      select 
        'POS' :: character varying as data_type, 
        customer as dstr_nm, 
        sap_prnt_cust_key, 
        sap_prnt_cust_desc, 
        material_number as matl_num, 
        trans_dt as order_date, 
        sales_gts :: double precision as sellout_value, 
        null as no_of_wks 
      from 
        itg_th_pos_sales_inventory_fact 
      where 
        (
          foc_product :: text = 'N' :: text 
          or foc_product is null
        ) 
        and not (
          concat(
            customer :: text, branch_code :: text
          ) in (
            select 
              distinct concat(
                itg_th_pos_customer_dim.cust_cd :: text, 
                itg_th_pos_customer_dim.brnch_no :: text
              ) as concat 
            from 
              itg_th_pos_customer_dim 
            where 
              upper(
                itg_th_pos_customer_dim.brnch_typ :: text
              ) = 'DISTRIBUTION CENTER' :: text 
              and itg_th_pos_customer_dim.cust_cd :: text = 'Lotus' :: text
          )
        ) 
        and not (
          concat(
            sold_to_code :: text, bar_code :: text
          ) in (
            select 
              concat(
                itg_th_pos_sales_inventory_fact.sold_to_code :: text, 
                itg_th_pos_sales_inventory_fact.bar_code :: text
              ) as concat 
            from 
              itg_th_pos_sales_inventory_fact 
            where 
              "left"(
                itg_th_pos_sales_inventory_fact.trans_dt :: date :: character varying :: text, 
                4
              ) >= (
                date_part(
                  year, 
                  current_timestamp():: timestamp without time zone
                ) -6
              ):: character varying :: text 
              and itg_th_pos_sales_inventory_fact.customer :: text = 'Lotus' :: text 
            group by 
              concat(
                itg_th_pos_sales_inventory_fact.sold_to_code :: text, 
                itg_th_pos_sales_inventory_fact.bar_code :: text
              ) 
            having 
              sum(
                itg_th_pos_sales_inventory_fact.sales_baht
              ):: double precision = 0 :: double precision
          )
        ) 
        and sales_gts > 0 
      union all 
      select 
        data_type, 
        distributor_id as dstr_nm, 
        sap_prnt_cust_key, 
        sap_prnt_cust_desc, 
        sku_code, 
        to_char(order_date :: date, 'YYYY-MM-DD') as order_date, 
        gross_trade_sales, 
        null as no_of_wks 
      from 
        edw_th_inventory_analysis_base_6year_filter 
      where 
        upper(data_type) in ('SALES') 
        and gross_trade_sales > 0 
      union all 
      select 
        'WATSON SALES' :: character varying as data_type, 
        'AS Watson''s' as dstr_nm, 
        sap_prnt_cust_key, 
        sap_prnt_cust_desc, 
        a.matl_num, 
        null as order_date, 
        null as gross_trade_sales, 
        'NA' as no_of_wks 
      from 
        (
          select 
            cast('108835' as varchar) as sold_to_code, 
            nvl(
              nullif(c.matl_num, ''), 
              'NA'
            ) as matl_num 
          from 
            sdl_th_mt_watsons a 
            left join sdl_mds_th_customer_product_code b on a.item = b.item 
            left join (
              select 
                distinct code as matl_num, 
                barcode as barcd, 
                retailer_unit_conversion, 
                createdate 
              from 
                (
                  select 
                    barcode, 
                    code, 
                    retailer_unit_conversion, 
                    createdate, 
                    row_number() over (
                      partition by barcode 
                      order by 
                        createdate desc nulls last, 
                        code nulls last
                    ) as rnk 
                  from 
                    sdl_mds_th_product_master 
                  where 
                    barcode <> ''
                ) 
              where 
                rnk = 1
            ) c on b.barcode = c.barcd
        ) a 
        left join (
          select 
            sap_cust_id, 
            sap_prnt_cust_key, 
            sap_prnt_cust_desc 
          from 
            edw_vw_os_customer_dim 
          where 
            sap_cntry_cd = 'TH'
        ) c on cast (
          a.sold_to_code as varchar (10)
        ) = cast (
          c.sap_cust_id as varchar (10)
        )
    ) derived_table1, 
    (
      select 
        * 
      from 
        (
          select 
            *, 
            row_number() over (
              partition by sku_cd 
              order by 
                sku_cd desc
            ) as rank 
          from 
            (
              select 
                --emd.greenlight_sku_flag as greenlight_sku_flag,
                emd.pka_product_key as pka_product_key, 
                emd.pka_product_key_description as pka_product_key_description, 
                emd.pka_product_key as product_key, 
                emd.pka_product_key_description as product_key_description, 
                emd.pka_size_desc as pka_size_desc, 
                t4.gph_prod_frnchse, 
                t4.gph_prod_brnd, 
                t4.gph_prod_sub_brnd, 
                t4.gph_prod_vrnt, 
                t4.gph_prod_sgmnt, 
                t4.gph_prod_subsgmnt, 
                t4.gph_prod_ctgry, 
                t4.gph_prod_subctgry, 
                --t4.gph_prod_put_up_desc ,
                ltrim(t4.sap_matl_num) as sku_cd, 
                sap_mat_desc 
              from 
                (
                  select 
                    * 
                  from 
                    edw_vw_os_material_dim 
                  where 
                    cntry_key = 'TH'
                ) t4 
                left join --(select *  from edw_vw_greenlight_skus where sls_org in ('2400','2500'))
                (
                  select 
                    * 
                  from 
                    edw_material_dim
                ) emd on ltrim (t4.sap_matl_num, 0) = ltrim (emd.matl_num, 0)
            )
        ) 
      where 
        rank = 1
    ) t4 
  where 
    ltrim(derived_table1.matl_num, 0) = ltrim(
      t4.sku_cd(+), 
      0
    ) 
  group by 
    derived_table1.sap_prnt_cust_key, 
    derived_table1.dstr_nm, 
    t4.pka_size_desc, 
    t4.gph_prod_brnd, 
    t4.gph_prod_vrnt, 
    t4.gph_prod_sgmnt, 
    t4.gph_prod_ctgry, 
    t4.pka_product_key
) ,
final as (
    select
    sap_prnt_cust_key::varchar(12) as sap_prnt_cust_key,
    dstr_nm::varchar(20) as dstr_nm,
    pka_size_desc::varchar(30) as pka_size_desc,
    brand::varchar(30) as brand,
    variant::varchar(100) as variant,
    segment::varchar(50) as segment,
    prod_category::varchar(50) as prod_category,
    pka_product_key::varchar(68) as pka_product_key,
    min_date::date as date,
    no_of_wks::varchar(2) as no_of_wks
    from transformed
)
select 
  * 
from final
