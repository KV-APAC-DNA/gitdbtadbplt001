with edw_vw_vn_mt_dksh_sellin_fact as (
    select * from {{ ref('vnmedw_integration__edw_vw_vn_mt_dksh_sellin_fact') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_vw_vn_mt_dist_products as (
    select * from {{ ref('vnmedw_integration__edw_vw_vn_mt_dist_products') }}
),
edw_vw_vn_mt_dist_customers as (
    select * from {{ ref('vnmedw_integration__edw_vw_vn_mt_dist_customers') }}
),
itg_vn_mt_customer_sales_organization as (
    select * from {{ ref('vnmitg_integration__itg_vn_mt_customer_sales_organization') }}
),
edw_crncy_exch_rates as (
    select * from {{ ref('aspedw_integration__edw_crncy_exch_rates') }}
),
itg_vn_mt_sellin_coop as (
    select * from {{ ref('vnmitg_integration__itg_vn_mt_sellin_coop') }}
),
itg_vn_mt_pos_price_products as (
    select * from {{ ref('vnmitg_integration__itg_vn_mt_pos_price_products') }}
),
edw_vw_vn_mt_sellin_target as (
    select * from {{ ref('vnmedw_integration__edw_vw_vn_mt_sellin_target') }}
),
edw_vw_vn_billing_fact as (
    select * from {{ ref('vnmedw_integration__edw_vw_vn_billing_fact') }}
),


final as (
   SELECT    'DKSH'    AS data_source,
             'SELL-IN' AS data_type,
             dksh.invoice_date,
             timedim.year,
             timedim.qrtr_no,
             (timedim.qrtr)::varchar      AS qrtr,
             (timedim.mnth_id)::varchar   AS mnth_id,
             (timedim.mnth_desc)::varchar AS mnth_desc,
             timedim.mnth_no,
             timedim.mnth_shrt,
             timedim.mnth_long,
             timedim.wk,
             timedim.mnth_wk_no,
             timedim.cal_year,
             timedim.cal_qrtr_no,
             timedim.cal_mnth_id,
             timedim.cal_mnth_no,
             timedim.cal_mnth_nm,
             timedim.cal_date,
             (timedim.cal_date_id)::varchar AS cal_date_id,
             dksh.supplier_code,
             dksh.supplier_name,
             dksh.plant,
             dksh.productid,
             prd.product_name,
             prd.barcode,
             prd.jnj_sap_code,
             (upper(prd.franchise))::varchar    AS franchise,
             (upper(prd.category))::varchar     AS category,
             (upper(prd.sub_category))::varchar AS sub_category,
             (upper(prd.sub_brand))::varchar    AS sub_brand,
             prd.size,
             dksh.channel,
             dksh.custcode,
             cust.NAME,
             (upper((cust.address)::text))::varchar    AS address,
             (upper(cust.sub_channel))::varchar        AS sub_channel,
             (upper(cust.group_account))::varchar      AS group_account,
             (upper(cust.account))::varchar          AS "account",
             (upper(cust.region))::varchar           AS "region",
             (upper(cust.province))::varchar           AS province,
             (upper(cust.retail_environment))::varchar AS retail_environment,
             dksh.zone as "zone",
             (((substring(timedim.mnth_id, 5, 2)
                       || ('.'::varchar)::text)
                       || substring(timedim.mnth_id, 1, 4)))::varchar(20) AS period,
             (upper((mds.sales_supervisor)::text))::varchar                 AS sales_supervisor,
             (upper((mds.kam)::text))::varchar                              AS kam,
             dksh.qty_exclude_foc                                                     AS sales_qty,
             dksh.net_amount_wo_vat                                                   AS sales_amt_lcy,
             (dksh.net_amount_wo_vat * exch_rate.ex_rt)                               AS sales_amt_usd,
             ((NULL::numeric)::numeric(18,0))::numeric(10,3)                          AS gts_amt_lcy,
             ((NULL::numeric)::numeric(18,0))::numeric(10,3)                          AS gts_amt_usd,
             ((NULL::numeric)::numeric(18,0))::numeric(10,3)                          AS target_lcy,
             ((NULL::numeric)::numeric(18,0))::numeric(10,3)                          AS target_usd
   FROM      (((((edw_vw_vn_mt_dksh_sellin_fact dksh
   JOIN      edw_vw_os_time_dim timedim
   ON        (((dksh.invoice_date)::text = timedim.cal_date_id)))
   LEFT JOIN edw_vw_vn_mt_dist_products prd
   ON        (((dksh.productid)::text = (prd.code)::text)))
   LEFT JOIN edw_vw_vn_mt_dist_customers cust
   ON        (((dksh.custcode)::text = (cust.code)::text)))
   LEFT JOIN
             (
                             SELECT DISTINCT (itg_vn_mt_customer_sales_organization.mtd_code)::varchar                        AS customer_cd,
                                             (upper((itg_vn_mt_customer_sales_organization.sales_supervisor)::text))::varchar AS sales_supervisor,
                                             (upper((itg_vn_mt_customer_sales_organization.sales_man)::text))::varchar        AS ss,
                                             (upper((itg_vn_mt_customer_sales_organization.kam)::text))::varchar              AS kam
                             FROM            itg_vn_mt_customer_sales_organization
                             WHERE  ((itg_vn_mt_customer_sales_organization.mti_code IS NULL)
                                    AND   ((itg_vn_mt_customer_sales_organization.active)::text = ('Y'::varchar)::text))
                             UNION ALL
                             SELECT DISTINCT (itg_vn_mt_customer_sales_organization.mti_code)::varchar                        AS customer_cd,
                                             (upper((itg_vn_mt_customer_sales_organization.sales_supervisor)::text))::varchar AS sales_supervisor,
                                             (upper((itg_vn_mt_customer_sales_organization.sales_man)::text))::varchar        AS ss,
                                             (upper((itg_vn_mt_customer_sales_organization.kam)::text))::varchar              AS kam
                             FROM            itg_vn_mt_customer_sales_organization
                             WHERE           ((itg_vn_mt_customer_sales_organization.mtd_code IS NULL)
                                             AND  ((itg_vn_mt_customer_sales_organization.active)::text = ('Y'::varchar)::text))
              ) mds
   ON        (((dksh.custcode)::text = (mds.customer_cd)::text)))
   LEFT JOIN
             (
              SELECT (derived_table1.year|| derived_table1.mnth) AS mnth_id,
                           derived_table1.ex_rt
                    FROM   (
                                  SELECT edw_crncy_exch_rates.fisc_yr_per,
                                         substring(((edw_crncy_exch_rates.fisc_yr_per)::varchar)::text, 1, 4) AS year,
                                         substring(((edw_crncy_exch_rates.fisc_yr_per)::varchar)::text, 6, 2) AS mnth,
                                         edw_crncy_exch_rates.ex_rt
                                  FROM   edw_crncy_exch_rates
                                  WHERE  (((edw_crncy_exch_rates.from_crncy)::text = ('VND'::varchar)::text)
                                         AND    ((edw_crncy_exch_rates.to_crncy)::text = ('USD'::varchar)::text))) derived_table1
              ) exch_rate
   ON        (((timedim.mnth_id)::numeric(18,0) = (exch_rate.mnth_id)::numeric(18,0))))

   UNION ALL
   
   SELECT    'COOP'          AS data_source,
             'SELL-IN'       AS data_type,
             NULL AS invoice_date,
             timedim.year,
             timedim.qrtr_no,
             (timedim.qrtr)::varchar      AS qrtr,
             (timedim.mnth_id)::varchar   AS mnth_id,
             (timedim.mnth_desc)::varchar AS mnth_desc,
             timedim.mnth_no,
             timedim.mnth_shrt,
             timedim.mnth_long,
             NULL::bigint            AS wk,
             NULL::bigint            AS mnth_wk_no,
             NULL::integer           AS cal_year,
             NULL::integer           AS cal_qrtr_no,
             NULL::integer           AS cal_mnth_id,
             NULL::integer           AS cal_mnth_no,
             NULL::varchar AS cal_mnth_nm,
             NULL::date              AS cal_date,
             NULL::varchar AS cal_date_id,
             NULL::varchar AS supplier_code,
             NULL::varchar AS supplier_name,
             NULL::varchar AS plant,
             coop.sku                AS productid,
             prd.product_name,
             prd.barcode,
             prd.jnj_sap_code,
             (upper(prd.franchise))::varchar    AS franchise,
             (upper(prd.category))::varchar     AS category,
             (upper(prd.sub_category))::varchar AS sub_category,
             (upper(prd.sub_brand))::varchar    AS sub_brand,
             prd.size,
             'MT'  AS channel,
             (replace((coop.store)::text, (','::varchar)::text, (''::varchar)::text))::varchar AS custcode,
             cust.NAME,
             (upper((cust.address)::text))::varchar AS address,
             (upper(cust.sub_channel))::varchar     AS sub_channel, 
             (
             CASE WHEN (((upper(cust.group_account) = ('SAIGON COOP'::varchar)::text)
                       OR  (upper(cust.group_account) IS NULL) AND ('SAIGON COOP' IS NULL))) 
                    THEN ('COOP SM'::varchar)::text
                ELSE upper(cust.group_account)
             END)::varchar AS group_account, 
             (
             CASE WHEN ((upper(cust.account) = ('SAIGON COOP'::varchar)::text)
                      OR ((upper(cust.account) IS NULL) AND ('SAIGON COOP' IS NULL))) 
                    THEN ('COOP SM'::varchar)::text
                ELSE upper(cust.account)
             END)::varchar                                                          AS "account",
             (upper(cust.region))::varchar                                        AS "region",
             (upper(cust.province))::varchar                                        AS province,
             (upper(cust.retail_environment))::varchar                              AS retail_environment,
             NULL::varchar                                                          AS "zone",
             NULL::varchar                                                          AS period,
             (upper((mds.sales_supervisor)::text))::varchar                         AS sales_supervisor,
             (upper((mds.kam)::text))::varchar                                      AS kam,
             coop.sales_quantity                                                              AS sales_qty,
             (((coop.sales_quantity)::numeric)::numeric(18,0) * pp.price)                     AS sales_amt_lcy,
             ((((coop.sales_quantity)::numeric)::numeric(18,0) * pp.price) * exch_rate.ex_rt) AS sales_amt_usd,
             ((NULL::numeric)::numeric(18,0))::numeric(10,3)                                  AS gts_amt_lcy,
             ((NULL::numeric)::numeric(18,0))::numeric(10,3)                                  AS gts_amt_usd,
             ((NULL::numeric)::numeric(18,0))::numeric(10,3)                                  AS target_lcy,
             ((NULL::numeric)::numeric(18,0))::numeric(10,3)                                  AS target_usd
   FROM   itg_vn_mt_sellin_coop coop
   JOIN
             (
                             SELECT DISTINCT edw_vw_os_time_dim.year,
                                             edw_vw_os_time_dim.qrtr_no,
                                             edw_vw_os_time_dim.qrtr,
                                             edw_vw_os_time_dim.mnth_id,
                                             edw_vw_os_time_dim.mnth_desc,
                                             edw_vw_os_time_dim.mnth_no,
                                             edw_vw_os_time_dim.mnth_shrt,
                                             edw_vw_os_time_dim.mnth_long,
                                             edw_vw_os_time_dim.cal_mnth_id
                             FROM            edw_vw_os_time_dim) timedim
   ON        ((((((coop.year)::text|| (coop.month)::text))::integer = timedim.cal_mnth_id)
              AND   (date_part(month, (to_date((((coop.year)::text|| (coop.month)::text)|| ('01'::varchar)::text), ('YYYYMMDD'::varchar)::text))::timestamp_ntz) 
              = (timedim.mnth_no)::number(18,0))))
   LEFT JOIN edw_vw_vn_mt_dist_products prd
   ON        (((coop.sku)::text = (prd.code)::text))
   LEFT JOIN
             (
                 SELECT DISTINCT itg_vn_mt_pos_price_products.bar_code,
                           itg_vn_mt_pos_price_products.price
                     FROM  itg_vn_mt_pos_price_products
                     WHERE ((itg_vn_mt_pos_price_products.active)::text = ('Y'::varchar)::text)
              ) pp
   ON        (((prd.barcode)::text = (pp.bar_code)::text))
   LEFT JOIN edw_vw_vn_mt_dist_customers cust
   ON        ((((replace((coop.store)::text, (','::varchar)::text, (''::varchar)::text))::varchar)::text = (cust.code)::text))
   LEFT JOIN
             (
                             SELECT DISTINCT (itg_vn_mt_customer_sales_organization.mtd_code)::varchar                        AS customer_cd,
                                             (upper((itg_vn_mt_customer_sales_organization.sales_supervisor)::text))::varchar AS sales_supervisor,
                                             (upper((itg_vn_mt_customer_sales_organization.sales_man)::text))::varchar        AS ss,
                                             (upper((itg_vn_mt_customer_sales_organization.kam)::text))::varchar              AS kam
                             FROM            itg_vn_mt_customer_sales_organization
                             WHERE           ((itg_vn_mt_customer_sales_organization.mti_code IS NULL)
                                             AND  ((itg_vn_mt_customer_sales_organization.active)::text = ('Y'::varchar)::text))
                             UNION ALL
                             SELECT DISTINCT (itg_vn_mt_customer_sales_organization.mti_code)::varchar                        AS customer_cd,
                                             (upper((itg_vn_mt_customer_sales_organization.sales_supervisor)::text))::varchar AS sales_supervisor,
                                             (upper((itg_vn_mt_customer_sales_organization.sales_man)::text))::varchar        AS ss,
                                             (upper((itg_vn_mt_customer_sales_organization.kam)::text))::varchar              AS kam
                             FROM            itg_vn_mt_customer_sales_organization
                             WHERE           ((itg_vn_mt_customer_sales_organization.mtd_code IS NULL)
                                             AND             ((itg_vn_mt_customer_sales_organization.active)::text = ('Y'::varchar)::text))) mds
   ON        ((((replace((coop.store)::text, (','::varchar)::text, (''::varchar)::text))::varchar)::text = (mds.customer_cd)::text))
   LEFT JOIN
             (
                    SELECT (derived_table1.year
                                  || derived_table1.mnth) AS mnth_id,
                           derived_table1.ex_rt
                    FROM   (
                                  SELECT edw_crncy_exch_rates.fisc_yr_per,
                                         substring(((edw_crncy_exch_rates.fisc_yr_per)::varchar)::text, 1, 4) AS year,
                                         substring(((edw_crncy_exch_rates.fisc_yr_per)::varchar)::text, 6, 2) AS mnth,
                                         edw_crncy_exch_rates.ex_rt
                                  FROM   edw_crncy_exch_rates
                                  WHERE  (((edw_crncy_exch_rates.from_crncy)::text = ('VND'::varchar)::text)
                                         AND    ((edw_crncy_exch_rates.to_crncy)::text = ('USD'::varchar)::text))
                            ) derived_table1
              ) exch_rate
   ON        (((((coop.year)::text|| (coop.month)::text))::numeric(18,0) = (exch_rate.mnth_id)::numeric(18,0)))
   WHERE     (coop.sales_quantity <> 0)

 UNION ALL

 SELECT    'Target' AS data_source,
           tgt.data_type,
           NULL AS invoice_date,
           timedim.year,
           timedim.qrtr_no,
           (timedim.qrtr)::varchar      AS qrtr,
           (timedim.mnth_id)::varchar   AS mnth_id,
           (timedim.mnth_desc)::varchar AS mnth_desc,
           timedim.mnth_no,
           timedim.mnth_shrt,
           timedim.mnth_long,
           NULL::bigint            AS wk,
           NULL::bigint            AS mnth_wk_no,
           NULL::integer           AS cal_year,
           NULL::integer           AS cal_qrtr_no,
           NULL::integer           AS cal_mnth_id,
           NULL::integer           AS cal_mnth_no,
           NULL::varchar AS cal_mnth_nm,
           NULL::date              AS cal_date,
           NULL::varchar AS cal_date_id,
           NULL::varchar AS supplier_code,
           NULL::varchar AS supplier_name,
           NULL::varchar AS plant,
           NULL::varchar AS productid,
           NULL::varchar AS product_name,
           NULL::varchar AS barcode,
           NULL::varchar AS jnj_sap_code,
           NULL::varchar AS franchise,
           NULL::varchar AS category,
           NULL::varchar AS sub_category,
           NULL::varchar AS sub_brand,
           NULL::varchar AS size,
           NULL::varchar AS channel,
           tgt.cust_code           AS custcode,
           cust.NAME,
           cust.address,
           (upper(cust.sub_channel))::varchar             AS sub_channel,
           (upper(cust.group_account))::varchar           AS group_account,
           (upper(cust.account))::varchar               AS "account",
           (upper(cust.region))::varchar                AS "region",
           (upper(cust.province))::varchar                AS province,
           (upper(cust.retail_environment))::varchar      AS retail_environment,
           NULL::varchar                                  AS "zone",
           NULL::varchar                                  AS period,
           (upper((mds.sales_supervisor)::text))::varchar AS sales_supervisor,
           (upper((mds.kam)::text))::varchar              AS kam,
           NULL::integer                                            AS sales_qty,
           ((NULL::numeric)::numeric(18,0))::numeric(20,3)          AS sales_amt_lcy,
           ((NULL::numeric)::numeric(18,0))::numeric(20,3)          AS sales_amt_usd,
           ((NULL::numeric)::numeric(18,0))::numeric(10,3)          AS gts_amt_lcy,
           ((NULL::numeric)::numeric(18,0))::numeric(10,3)          AS gts_amt_usd,
           (tgt.target)::numeric(20,10)                           AS target_lcy,
           (tgt.target * exch_rate.ex_rt)::numeric(20,10)      AS target_usd
 FROM
        edw_vw_vn_mt_sellin_target tgt
 JOIN
           (
                           SELECT DISTINCT edw_vw_os_time_dim.year,
                                           edw_vw_os_time_dim.qrtr_no,
                                           edw_vw_os_time_dim.qrtr,
                                           edw_vw_os_time_dim.mnth_id,
                                           edw_vw_os_time_dim.mnth_desc,
                                           edw_vw_os_time_dim.mnth_no,
                                           edw_vw_os_time_dim.mnth_shrt,
                                           edw_vw_os_time_dim.mnth_long
                           FROM            edw_vw_os_time_dim
       ) timedim
 ON   (((tgt.sellin_cycle = timedim.mnth_no) AND  ((tgt.sellin_year)::text = ((timedim.year)::varchar)::text)))
 LEFT JOIN edw_vw_vn_mt_dist_customers cust
 ON  (((tgt.cust_code)::text = (cust.code)::text))
 LEFT JOIN
           (
                           SELECT DISTINCT (itg_vn_mt_customer_sales_organization.mtd_code)::varchar                        AS customer_cd,
                                           (upper((itg_vn_mt_customer_sales_organization.sales_supervisor)::text))::varchar AS sales_supervisor,
                                           (upper((itg_vn_mt_customer_sales_organization.sales_man)::text))::varchar        AS ss,
                                           (upper((itg_vn_mt_customer_sales_organization.kam)::text))::varchar              AS kam
                           FROM            itg_vn_mt_customer_sales_organization
                           WHERE           ((itg_vn_mt_customer_sales_organization.mti_code IS NULL)
                                           AND ((itg_vn_mt_customer_sales_organization.active)::text = ('Y'::varchar)::text))
                           UNION ALL
                           SELECT DISTINCT (itg_vn_mt_customer_sales_organization.mti_code)::varchar                        AS customer_cd,
                                           (upper((itg_vn_mt_customer_sales_organization.sales_supervisor)::text))::varchar AS sales_supervisor,
                                           (upper((itg_vn_mt_customer_sales_organization.sales_man)::text))::varchar        AS ss,
                                           (upper((itg_vn_mt_customer_sales_organization.kam)::text))::varchar              AS kam
                           FROM            itg_vn_mt_customer_sales_organization
                           WHERE           ((itg_vn_mt_customer_sales_organization.mtd_code IS NULL)
                                           AND  ((itg_vn_mt_customer_sales_organization.active)::text = ('Y'::varchar)::text))
            ) mds
 ON  (tgt.cust_code)::text = (mds.customer_cd)::text
 LEFT JOIN
           (
                  SELECT (derived_table1.year
                                || derived_table1.mnth) AS mnth_id,
                         derived_table1.ex_rt
                  FROM   (
                                SELECT edw_crncy_exch_rates.fisc_yr_per,
                                       substring(((edw_crncy_exch_rates.fisc_yr_per)::varchar)::text, 1, 4) AS year,
                                       substring(((edw_crncy_exch_rates.fisc_yr_per)::varchar)::text, 6, 2) AS mnth,
                                       edw_crncy_exch_rates.ex_rt
                                FROM   edw_crncy_exch_rates
                                WHERE  (((edw_crncy_exch_rates.from_crncy)::text = ('VND'::varchar)::text)
                                       AND    ((edw_crncy_exch_rates.to_crncy)::text = ('USD'::varchar)::text))
                        ) derived_table1
            ) exch_rate
 ON        ((((tgt.sellin_year)::text|| lpad(((tgt.sellin_cycle)::varchar)::text, 2, ((0)::varchar)::text)))::numeric(18,0) = (exch_rate.mnth_id)::numeric(18,0))
 WHERE     (tgt.data_type IS NOT NULL) AND  (tgt.cust_code IS NOT NULL)



UNION ALL

SELECT    'JNJ'                                AS data_source,
          'SELL-IN'                            AS data_type,
          (bill_ft.bill_dt)::varchar AS invoice_date,
          timedim.year,
          timedim.qrtr_no,
          (timedim.qrtr)::varchar      AS qrtr,
          (timedim.mnth_id)::varchar   AS mnth_id,
          (timedim.mnth_desc)::varchar AS mnth_desc,
          timedim.mnth_no,
          timedim.mnth_shrt,
          timedim.mnth_long,
          timedim.wk,
          timedim.mnth_wk_no,
          timedim.cal_year,
          timedim.cal_qrtr_no,
          timedim.cal_mnth_id,
          timedim.cal_mnth_no,
          timedim.cal_mnth_nm,
          timedim.cal_date,
          (timedim.cal_date_id)::varchar AS cal_date_id,
          NULL                          AS supplier_code,
          NULL                          AS supplier_name,
          NULL                          AS plant,
          (bill_ft.matl_num)::varchar    AS productid,
          prd.product_name,
          prd.barcode,
          prd.jnj_sap_code,
          (upper(prd.franchise))::varchar    AS franchise,
          (upper(prd.category))::varchar     AS category,
          (upper(prd.sub_category))::varchar AS sub_category,
          (upper(prd.sub_brand))::varchar    AS sub_brand,
          prd.size,
          'MT' AS channel, 
          (CASE
                    WHEN (((timedim.mnth_id = ('201908'::varchar)::text) 
                            OR (timedim.mnth_id = ('201909'::varchar)::text))
                            OR  (timedim.mnth_id = ('201910'::varchar)::text)) 
                     THEN
                              CASE
                                        WHEN ((bill_ft.ship_to = ('622334'::varchar)::text)
                                                 OR ((bill_ft.ship_to IS NULL)
                                                 AND  ('622334' IS NULL))) 
                                          THEN ('622333'::varchar)::text
                                        WHEN ((bill_ft.ship_to = ('622450'::varchar)::text)
                                                 OR ((bill_ft.ship_to IS NULL)
                                                 AND       ('622450' IS NULL)))
                                          THEN ('622449'::varchar)::text
                                        WHEN ((bill_ft.ship_to = ('622451'::varchar)::text)OR ((bill_ft.ship_to IS NULL)AND ('622451' IS NULL))) 
                                          THEN ('622449'::varchar)::text
                                        WHEN ((bill_ft.ship_to = ('622452'::varchar)::text)OR ((bill_ft.ship_to IS NULL)      AND  ('622452' IS NULL)))
                                          THEN ('622449'::varchar)::text
                                        WHEN ((bill_ft.ship_to = ('622453'::varchar)::text)
                                                 OR ((bill_ft.ship_to IS NULL)
                                                 AND ('622453' IS NULL))) 
                                          THEN ('622449'::varchar)::text
                                        WHEN ((bill_ft.ship_to = ('622454'::varchar)::text)OR ((bill_ft.ship_to IS NULL)AND ('622454' IS NULL))) 
                                          THEN ('622449'::varchar)::text
                                        WHEN ((bill_ft.ship_to = ('622455'::varchar)::text)
                                                 OR ((bill_ft.ship_to IS NULL)
                                                 AND ('622455' IS NULL)))
                                          THEN ('622449'::varchar)::text
                                        ELSE bill_ft.ship_to
                              END
                    ELSE bill_ft.ship_to
          END)::varchar AS custcode,
          cust.NAME,
          cust.address,
          (upper(cust.sub_channel))::varchar             AS sub_channel,
          (upper(cust.group_account))::varchar           AS group_account,
          (upper(cust.account))::varchar               AS "account",
          (upper(cust.region))::varchar                AS "region",
          (upper(cust.province))::varchar                AS province,
          (upper(cust.retail_environment))::varchar      AS retail_environment,
          NULL                                          AS "zone",
          NULL                                          AS period,
          (upper((mds.sales_supervisor)::text))::varchar AS sales_supervisor,
          (upper((mds.kam)::text))::varchar              AS kam,
          bill_ft.bill_qty_pc                                      AS sales_qty,
          (bill_ft.net_val)::numeric(18,0)                         AS sales_amt_lcy,
          ((bill_ft.net_val)::numeric(18,0) * exch_rate.ex_rt)     AS sales_amt_usd,
          bill_ft.grs_trd_sls                                      AS gts_amt_lcy,
          (bill_ft.grs_trd_sls * exch_rate.ex_rt)                  AS gts_amt_usd,
          (NULL::numeric)::numeric(18,0)                           AS target_lcy,
          (NULL::numeric)::numeric(18,0)                           AS target_usd
FROM      edw_vw_vn_billing_fact bill_ft
JOIN      edw_vw_os_time_dim timedim
ON        (bill_ft.bill_dt = timedim.cal_date)
JOIN      edw_vw_vn_mt_dist_customers cust
ON        (
                    CASE
                      WHEN (((timedim.mnth_id = ('201908'::varchar)::text)
                                          OR (timedim.mnth_id = ('201909'::varchar)::text))
                                          OR (timedim.mnth_id = ('201910'::varchar)::text)) 
                                   THEN (
                                        CASE
                                   WHEN (( (bill_ft.ship_to)::numeric(18,0) = ((622334)::numeric)::numeric(18,0))   
                                          OR  (((bill_ft.ship_to)::numeric(18,0) IS NULL)                    
                                          AND ('622334' IS NULL))) THEN ((622333)::numeric)::numeric(18,0)
                                   WHEN (((bill_ft.ship_to)::numeric(18,0) = ((622450)::numeric)::numeric(18,0))          
                                          OR (((bill_ft.ship_to)::numeric(18,0) IS NULL)                    
                                          AND ('622450' IS NULL))) THEN ((622449)::numeric)::numeric(18,0)
                                   WHEN (((bill_ft.ship_to)::numeric(18,0) = ((622451)::numeric)::numeric(18,0))         
                                          OR (((bill_ft.ship_to)::numeric(18,0) IS NULL) AND  ('622451' IS NULL))) 
                                          THEN ((622449)::numeric)::numeric(18,0)
                                   WHEN (((bill_ft.ship_to)::numeric(18,0) = ((622452)::numeric)::numeric(18,0))          
                                          OR  (((bill_ft.ship_to)::numeric(18,0) IS NULL) AND  ('622452' IS NULL))) THEN ((622449)::numeric)::numeric(18,0)
                                   WHEN (((bill_ft.ship_to)::numeric(18,0) = ((622453)::numeric)::numeric(18,0))          
                                          OR (((bill_ft.ship_to)::numeric(18,0) IS NULL)  AND   ('622453' IS NULL))) THEN ((622449)::numeric)::numeric(18,0)
                                   WHEN (((bill_ft.ship_to)::numeric(18,0) = ((622454)::numeric)::numeric(18,0))          
                                          OR (((bill_ft.ship_to)::numeric(18,0) IS NULL)                    
                                          AND ('622454' IS NULL))) THEN ((622449)::numeric)::numeric(18,0)
                                   WHEN (((bill_ft.ship_to)::numeric(18,0) = ((622455)::numeric)::numeric(18,0))          
                                          OR (((bill_ft.ship_to)::numeric(18,0) IS NULL)                    
                                          AND ('622455' IS NULL))) THEN ((622449)::numeric)::numeric(18,0)
                                   ELSE (bill_ft.ship_to)::numeric(18,0)
                                        END = (cust.code)::numeric(18,0))
                              ELSE ((bill_ft.ship_to)::numeric(18,0) = (cust.code)::numeric(18,0))
                    END
            )
LEFT JOIN
          (
                          SELECT DISTINCT edw_vw_vn_mt_dist_products.barcode,
                                          edw_vw_vn_mt_dist_products.jnj_sap_code,
                                          edw_vw_vn_mt_dist_products.code,
                                          edw_vw_vn_mt_dist_products.franchise,
                                          edw_vw_vn_mt_dist_products.category,
                                          edw_vw_vn_mt_dist_products.sub_category,
                                          edw_vw_vn_mt_dist_products.sub_brand,
                                          edw_vw_vn_mt_dist_products.base_bundle,
                                          edw_vw_vn_mt_dist_products.size,
                                          edw_vw_vn_mt_dist_products.product_name
                          FROM            edw_vw_vn_mt_dist_products) prd
ON  ((bill_ft.matl_num = (prd.code)::text))
LEFT JOIN
          (SELECT DISTINCT (itg_vn_mt_customer_sales_organization.mtd_code)::varchar AS customer_cd,
                                          (upper((itg_vn_mt_customer_sales_organization.sales_supervisor)::text))::varchar AS sales_supervisor,
                                          (upper((itg_vn_mt_customer_sales_organization.sales_man)::text))::varchar        AS ss,
                                          (upper((itg_vn_mt_customer_sales_organization.kam)::text))::varchar              AS kam
                          FROM  itg_vn_mt_customer_sales_organization
                          WHERE           ((itg_vn_mt_customer_sales_organization.mti_code IS NULL)
                                          AND             ((itg_vn_mt_customer_sales_organization.active)::text = ('Y'::varchar)::text))
                          UNION ALL
                          SELECT DISTINCT (itg_vn_mt_customer_sales_organization.mti_code)::varchar                        AS customer_cd,
                                          (upper((itg_vn_mt_customer_sales_organization.sales_supervisor)::text))::varchar AS sales_supervisor,
                                          (upper((itg_vn_mt_customer_sales_organization.sales_man)::text))::varchar        AS ss,
                                          (upper((itg_vn_mt_customer_sales_organization.kam)::text))::varchar              AS kam
                          FROM  itg_vn_mt_customer_sales_organization
                          WHERE           ((itg_vn_mt_customer_sales_organization.mtd_code IS NULL)
                                          AND ((itg_vn_mt_customer_sales_organization.active)::text = ('Y'::varchar)::text))) mds
ON ((bill_ft.ship_to = (mds.customer_cd)::text))
LEFT JOIN
          (
                 SELECT (derived_table1.year
                               || derived_table1.mnth) AS mnth_id,
                        derived_table1.ex_rt
                 FROM   (
                               SELECT edw_crncy_exch_rates.fisc_yr_per,
                                      substring(((edw_crncy_exch_rates.fisc_yr_per)::varchar)::text, 1, 4) AS year,
                                      substring(((edw_crncy_exch_rates.fisc_yr_per)::varchar)::text, 6, 2) AS mnth,
                                      edw_crncy_exch_rates.ex_rt
                               FROM   edw_crncy_exch_rates
                               WHERE  ((( edw_crncy_exch_rates.from_crncy)::text = ('VND'::varchar)::text)
                                      AND    ((edw_crncy_exch_rates.to_crncy)::text = ('USD'::varchar)::text))
                            ) derived_table1
          ) exch_rate
ON        (timedim.mnth_id)::numeric(18,0) = (exch_rate.mnth_id)::numeric(18,0)
WHERE     ((bill_ft.bill_qty_pc <> (((0)::numeric)::numeric(18,0))::numeric(38,4))
          AND (((bill_ft.bill_type)::text = ('ZF2V'::varchar)::text)
          OR ((bill_ft.bill_type)::text = ('S1'::varchar)::text)))
          AND ((bill_ft.cntry_key)::text = ('VN'::varchar)::text)
)

select * from final