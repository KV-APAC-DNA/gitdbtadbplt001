with itg_vn_mt_sellout_vinmart as (
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.itg_vn_mt_sellout_vinmart
),
itg_vn_mt_sellout_aeon as (
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.itg_vn_mt_sellout_aeon
),
itg_vn_mt_pos_cust_master as (
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.itg_vn_mt_pos_cust_master
),
itg_vn_mt_sellout_bhx as (
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.itg_vn_mt_sellout_bhx
),
itg_vn_mt_sellout_mega as (
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.itg_vn_mt_sellout_mega
),
itg_vn_mt_sellout_coop as (
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.itg_vn_mt_sellout_coop
),
itg_vn_mt_sellout_lotte as (
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.itg_vn_mt_sellout_lotte
),
itg_vn_mt_sellout_guardian as (
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.itg_vn_mt_sellout_guardian
),
itg_vn_mt_sellout_con_cung as (
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.itg_vn_mt_sellout_con_cung
),
itg_vn_mt_pos_product_master as (
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.itg_vn_mt_pos_product_master
),
itg_vn_mt_dksh_product_master as (
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.itg_vn_mt_dksh_product_master
),
itg_vn_mt_pos_price_products as (
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.itg_vn_mt_pos_price_products
),

final as
(
SELECT CAST(POS.YEAR AS INT) AS YEAR,
       CAST(POS.MONTH AS INT) AS MONTH,
       POS.ACCOUNT,
       POS.Customer_cd,
       POS.store_name,
       POS.product_cd,
       POS.barcode,
       POS.quantity,
       POS.amount,
       convert_timezone('Asia/Singapore',current_timestamp) updt_dttm
FROM (SELECT DISTINCT 'Vinmart' AS account,
             YEAR,
             MONTH,
             a.store AS Customer_cd,
             a.store_name AS store_name,
             a.article AS product_cd,
             CAST(NULL AS VARCHAR) AS BARCODE,
             SUM(pos_quantity) OVER (PARTITION BY store,article,store_name,CONCAT (YEAR,MONTH)) AS quantity,
             SUM(pos_revenue*0.88) OVER (PARTITION BY store,article,store_name,CONCAT (YEAR,MONTH)) AS amount
      FROM itg_vn_mt_sellout_vinmart a where lower(filename) like 'vinmart_sell_out%'

      UNION ALL

      SELECT DISTINCT 'Aeon' AS account,
             YEAR,
             MONTH,
             replace(a.store,'''','')  AS Customer_cd,
             s.store_name AS store_name,
             a.item AS product_cd,
             CAST(NULL AS VARCHAR) AS BARCODE,
            sum(a.sales_quantity) OVER(  PARTITION BY replace(a.store,'''',''), a.item, s.store_name,concat(a.year::text, a.month::text) 
                ORDER BY replace(a.store,'''',''), a.item, s.store_name,concat(a.year::text, a.month::text)
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS quantity,
            sum(a.sales_amount * 0.88) OVER( PARTITION BY replace(a.store,'''',''), a.item, s.store_name, concat(a.year::text, a.month::text)
                ORDER BY replace(a.store,'''',''), a.item, s.store_name, concat(a.year::text, a.month::text)
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS amount
      FROM itg_vn_mt_sellout_aeon a 
      left join (select b.store_code,b.store_name,b.customer_name from itg_vn_mt_pos_cust_master b where upper(b.customer_name)='AEON' and active='Y') as s on s.store_code=replace(a.store,'''','')
      where lower(filename) like 'aeon_sell_out%'

      UNION ALL

      SELECT DISTINCT 'BHX' AS account,
             YEAR,
             MONTH,
             a.cust_code AS Customer_cd,
             a.cust_name AS store_name,
             a.pro_code AS product_cd,
             CAST(NULL AS VARCHAR) AS BARCODE,
             SUM(quantity) OVER( PARTITION BY a.cust_code, a.pro_code, a.cust_name, concat(a.year::text, a.month::text) 
                ORDER BY a.cust_code, a.pro_code, a.cust_name, concat(a.year::text, a.month::text) 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  AS quantity,
             sum(a.amount * 0.88)OVER( PARTITION BY a.cust_code, a.pro_code, a.cust_name, concat(a.year::text, a.month::text)
                ORDER BY  a.cust_code, a.pro_code, a.cust_name, concat(a.year::text, a.month::text)
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS amount
      FROM itg_vn_mt_sellout_bhx a where lower(a.filename) like 'bhx_sell_out%'

      UNION ALL

	  SELECT DISTINCT 'Vinmart+' AS account,
             YEAR,
             MONTH,
             a.store AS Customer_cd,
             a.store_name AS store_name,
             a.article AS product_cd,
             CAST(NULL AS VARCHAR) AS BARCODE,
             SUM(pos_quantity) OVER (PARTITION BY store,article,store_name,CONCAT (YEAR,MONTH)) AS quantity,
             SUM(pos_revenue*0.88) OVER (PARTITION BY store,article,store_name,CONCAT (YEAR,MONTH)) AS amount
      FROM itg_vn_mt_sellout_vinmart a where lower(filename) like 'vinmart+_sell_out%'

      UNION ALL

      SELECT DISTINCT 'Mega' AS account,
             YEAR,
             MONTH,
             a.site_no AS Customer_cd,
             a.site_name AS store_name,
             a.art_no AS product_cd,
             CAST(NULL AS VARCHAR) AS BARCODE,
             SUM(sale_qty) OVER (PARTITION BY site_no,art_no,site_name,concat (YEAR,MONTH)) AS quantity,
             SUM(cogs_amt) OVER (PARTITION BY site_no,art_no,site_name,concat (YEAR,MONTH)) AS amount
      FROM itg_vn_mt_sellout_mega a

      UNION ALL

      SELECT DISTINCT Coop.account,
             Coop.year,
             Coop.month,
             Coop.customer_cd,
             Coop.store_name,
             Coop.sku AS product_cd,
             Coop.barcode,
             SUM(Coop.qty) OVER (PARTITION BY customer_cd,sku,store_name,CONCAT (YEAR,MONTH)) AS quantity,
             SUM(Coop.sales_amount*0.88) OVER (PARTITION BY customer_cd,sku,store_name,CONCAT (YEAR,MONTH)) AS amount
      FROM (SELECT main.year,
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
                   main.sales_amount / lp.list_price AS qty,
                   'COOP' AS account,
                   main.store AS customer_cd,
                   main.store AS store_name,
                   CAST(NULL AS VARCHAR) AS BARCODE
            FROM itg_vn_mt_sellout_coop main,
                 (SELECT DISTINCT YEAR,
                         MONTH,
                         thang,
                         desc_a,
                         sku,
                         (sumofttbvnckhd / sumoflg) AS list_price
                  FROM itg_vn_mt_sellout_coop) LP
            WHERE main.year = lp.year
            AND   main.month = lp.month
            AND   main.thang = lp.thang
            AND   main.desc_a = lp.desc_a
            AND   main.sku = lp.sku
            ) Coop

      UNION ALL

      SELECT DISTINCT 'Lotte' AS account,
             YEAR,
             MONTH,
             a.str AS customer_Cd,
             a.str_nm AS store_name,
             a.prod_cd AS product_cd,
             CAST(NULL AS VARCHAR) AS BARCODE,
             SUM(sale_qty) OVER (PARTITION BY str,prod_cd,store_name,CONCAT (YEAR,MONTH)) AS quantity,
             SUM(tot_sale_amt*0.88/1.1) OVER (PARTITION BY str,prod_cd,store_name,CONCAT (YEAR,MONTH)) AS amount
      FROM itg_vn_mt_sellout_lotte a

      UNION ALL

      SELECT DISTINCT 'Guardian' AS account,
             YEAR,
             MONTH,
             a.store_code AS customer_cd,
             a.store_name AS store_name,
             a.sku AS product_cd,
             a.barcode AS barcode,
             SUM(sales_supplier) OVER (PARTITION BY store_code,sku,store_name,CONCAT (YEAR,MONTH)) AS quantity,
             SUM(amount*0.88) OVER (PARTITION BY store_code,sku,store_name,CONCAT (YEAR,MONTH)) AS amount
      FROM itg_vn_mt_sellout_guardian a

      UNION ALL

      (SELECT DISTINCT POS.ACCOUNT,
             YEAR,
             MONTH,
             POS.Customer_cd,
             POS.store_name,
             POS.product_cd,
             pos_prod.barcode,
             SUM(pos.quantity) OVER (PARTITION BY customer_cd,product_cd,store_name,CONCAT (YEAR,MONTH)) AS quantity,
             SUM((pos.quantity*price)) OVER (PARTITION BY customer_cd,product_cd,store_name,CONCAT (YEAR,MONTH)) AS amount
      FROM (SELECT 'Con Cung' AS account,
                   YEAR,
                   MONTH,
                   a.store AS customer_cd,
                   a.store AS store_name,
                   a.product_code AS product_cd,
                   quantity,
                   filename,
                   crtd_dttm
            FROM itg_vn_mt_sellout_con_cung a) POS
        LEFT JOIN (SELECT distinct prods.barcode,
                          prods.customer,
                          prods.customer_sku,
                          pprod.pc_per_case,
                          pprod.price
                   FROM (SELECT barcode,
                                customer,
                                customer_sku
                         FROM itg_vn_mt_pos_product_master
                         WHERE active = 'Y'
                         AND   customer = 'Con Cung') prods
                     LEFT JOIN (SELECT DISTINCT barcode,
                                       jnj_sap_code
                                FROM itg_vn_mt_dksh_product_master where active = 'Y') dst_prod ON prods.barcode = dst_prod.barcode
                     LEFT JOIN (SELECT DISTINCT bar_code,product_id_concung,
                                       pc_per_case,
                                       price
                                       FROM itg_vn_mt_pos_price_products where active = 'Y') pprod ON dst_prod.barcode = pprod.bar_code AND prods.customer_sku = pprod.product_id_concung) pos_prod
               ON replace(pos.product_cd,'''','') = replace(pos_prod.customer_sku,'''','')
              AND pos.account = pos_prod.customer
              )
        ) POS
)

select * from final