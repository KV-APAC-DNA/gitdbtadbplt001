with itg_pos_cust_prod_cd_ean_map as(
  select * from DEV_DNA_CORE.SNAPNTAITG_INTEGRATION.ITG_POS_CUST_PROD_CD_EAN_MAP
),
itg_pos_invnt as(
  select * from DEV_DNA_CORE.SNAPNTAITG_INTEGRATION.itg_pos_invnt
),
edw_customer_attr_flat_dim as(
  select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.edw_CUSTOMER_ATTR_FLAT_DIM
),
itg_query_parameters as(
  select * from DEV_DNA_CORE.SNAPNTAITG_INTEGRATION.itg_query_parameters
),
edw_material_dim as(
  select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.edw_material_dim
),
itg_pos_prom_prc_map as(
  select * from DEV_DNA_CORE.SNAPNTAITG_INTEGRATION.itg_pos_prom_prc_map
),
edw_product_attr_dim as(
  select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_PRODUCT_ATTR_DIM
),

x as(
  SELECT a.*,
                   qp.sold_to_party,
                   CASE
                     WHEN a.src_sys_cd LIKE '7-11' OR a.src_sys_cd LIKE 'A-Mart%' OR a.src_sys_cd LIKE 'Cosmed%' OR a.src_sys_cd LIKE 'EC' OR a.src_sys_cd LIKE 'Watsons%' THEN CAST(g.barcd AS VARCHAR(100))
                     ELSE CAST(a.ean_num AS VARCHAR(100))
                   END AS barcode,
                   CASE
                     WHEN c.sls_grp IS NULL OR c.sls_grp = '' THEN a.src_sys_cd
                     ELSE c.sls_grp
                   END AS sls_grp
            FROM itg_pos_invnt a
              LEFT JOIN (SELECT DISTINCT sls_grp,
                                cust_store_ref,
                                sold_to_party
                         FROM edw_customer_attr_flat_dim
                         WHERE cust_store_ref IS NOT NULL
                         AND   cust_store_ref <> '') c
                     ON c.cust_store_ref = a.str_cd
                    AND c.sls_grp = a.src_sys_cd
              LEFT JOIN (SELECT cust_prod_cd,
                                MIN(barcd) AS barcd
                         FROM itg_pos_cust_prod_cd_ean_map
                         GROUP BY cust_prod_cd) g ON a.vend_prod_cd = g.cust_prod_cd
left join (select distinct country_code,parameter_name as src_sys_cd,parameter_value as sold_to_party from 
itg_query_parameters where country_code='TW' and parameter_type='sold_to_party') qp on qp.src_sys_cd=a.src_sys_cd
),
transformed as(
SELECT src.invnt_dt,
       src.vend_cd,
       src.vend_nm,
       CASE
         WHEN src.vend_prod_cd IS NULL OR src.vend_prod_cd = '' THEN '#'
         ELSE src.vend_prod_cd
       END AS vend_prod_cd,
       src.vend_prod_nm,
       CASE
         WHEN src.barcode IS NULL OR src.barcode = '' THEN '#'
         ELSE src.barcode
       END AS ean_num,
       CASE
         WHEN src.str_num IS NULL OR src.str_num = '' THEN '#'
         ELSE src.str_num
       END AS str_cd,
       CASE
         WHEN src.str_nm IS NULL OR src.str_nm = '' THEN '#'
         ELSE src.str_nm
       END AS str_nm,
       CASE
         WHEN src.sold_to_party IS NULL OR src.sold_to_party = '' THEN '#'
         ELSE src.sold_to_party
       END AS sold_to_party,
       src.sls_grp,
       src.mysls_brnd_nm,
       src.invnt_qty,
       src.invnt_amt,
       src.unit_prc_amt,
       src.per_box_qty,
       src.cust_invnt_qty,
       src.box_invnt_qty,
       src.wk_hold_sls,
       src.wk_hold,
       src.fst_recv_dt,
       src.dsct_dt,
       src.dc,
       src.stk_cls,
       src.src_sys_cd,
       src.crncy_cd,
       src.ctry_cd,
       src.mysls_catg,
       src.sap_matl_num AS matl_num,
       e.matl_desc,
       src.prom_invnt_amt,
       src.prom_prc_amt,
       'N' AS hist_flg,
       current_timestamp()::timestamp_ntz(9) AS crt_dttm,
       current_timestamp()::timestamp_ntz(9) AS upd_dttm
FROM (SELECT x.*,
             COALESCE(d.prod_hier_l4,'Others') AS mysls_brnd_nm,
             COALESCE(d.prod_hier_l3,'Others') AS mysls_catg,
             d.sap_matl_num,
             COALESCE((x.invnt_qty*e.prom_prc),0) AS prom_invnt_amt,
             COALESCE(e.prom_prc,CAST(0 AS NUMERIC(16,5))) AS prom_prc_amt,
             x.str_cd AS str_num
      FROM x
        LEFT JOIN (SELECT DISTINCT ean,
                          sap_matl_num,
                          prod_hier_l4,
                          prod_hier_l3,
                          cntry
                   FROM edw_product_attr_dim) d
               ON CAST (x.barcode AS VARCHAR (40)) = CAST (d.ean AS VARCHAR (40))
              AND x.ctry_cd = d.cntry
        LEFT JOIN itg_pos_prom_prc_map e
               ON x.invnt_dt BETWEEN e.prom_strt_dt
              AND e.prom_end_dt
              AND x.barcode = e.barcd
      WHERE x.ctry_cd = 'TW'
        AND   x.src_sys_cd in ( 'Poya 寶雅', 'PX 全聯', 'RT-Mart 大潤發', 'ibonMart', 'A-Mart 愛買')
      ) src
  LEFT JOIN (SELECT DISTINCT matl_num,
                    matl_desc
             FROM edw_material_dim) e ON COALESCE (LTRIM (e.matl_num,0),'#') = LTRIM (COALESCE (src.sap_matl_num,'#'),0)
)
select * from transformed