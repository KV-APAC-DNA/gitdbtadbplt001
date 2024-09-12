with
edw_vw_os_time_dim as 
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_material_sales_dim as 
(
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_kr_otc_inventory as 
(
    select * from {{ ref('ntaedw_integration__edw_kr_otc_inventory') }}
),
edw_billing_fact as 
(
    select * from {{ ref('aspedw_integration__edw_billing_fact') }}
),

BILL_FACT
AS
(SELECT material,
       cust_sls,
       bill_dt,
       bill_qty,
       netval_inv,
       CAST(netval_inv / bill_qty AS NUMERIC(15,4)) InvoicePrice,
       ROW_NUMBER() OVER (PARTITION BY material,cust_sls ORDER BY bill_dt DESC) rn
FROM edw_billing_fact bill
WHERE sls_org IN ('3200','320A','320S','321A')
AND   bill_type = 'ZF2K')
SELECT 'KR' ctry_cd,
       'INV' Data_Type,
       inv.distributor_cd AS dstr_cd,
       inv.matl_num AS prod_cd,
       mnth_id AS fisc_per,
       SUM(inv_qty) inv_qty,
       0 AS inv_value,
       0 AS so_qty,
       0 AS so_value,
       0 AS si_qty,
       0 AS si_value
FROM edw_kr_otc_inventory inv
WHERE mnth_id >= (date_part(year, convert_timezone('UTC', current_timestamp())) -6)
GROUP BY inv.distributor_cd,
         inv.matl_num,
         mnth_id
UNION ALL
SELECT cntry_cd,
       data_type,
       distributor_code,
       product_code,
       mnth_id AS fisc_per,
       0 AS inv_qty,
       0 AS inv_amt,
       0 AS so_qty,
       0 AS so_amt,
       SUM(sell_in_qty) si_qty,
       SUM(NVL (sell_in_value,amount)) si_value
FROM (SELECT 'KR' cntry_cd,
             'SELLIN' AS data_type,
             LTRIM(sold_to,'0') AS distributor_code,
             LTRIM(MATERIAL,'0') AS product_code,
             bill_dt,
             SUM(bill_qty) sell_in_qty,
             SUM(netval_inv) sell_in_value
      FROM edw_billing_fact
      WHERE LEFT (bill_dt,4) >= (DATE_PART(YEAR,current_timestamp) -6)
      AND   sls_org IN ('3200','320A','320S','321A')
      AND   BILL_TYPE IN ('ZF2K','ZG2K')
      AND   LTRIM(sold_to,'0') IN (SELECT DISTINCT distributor_cd
                                   FROM edw_kr_otc_inventory)
      GROUP BY LTRIM(sold_to,'0'),
               LTRIM(MATERIAL,'0'),
               bill_dt) T1
  LEFT JOIN (SELECT DISTINCT t_msd.matl_num,
                                              t_ean.*
                                       FROM (SELECT cust_sls,
                                                    ean_num,
                                                    netval_inv AS amount
                                             FROM (SELECT ivp.*,
                                                          (ROW_NUMBER() OVER (PARTITION BY ivp.cust_sls ORDER BY ivp.netval_inv,ivp.ean_num DESC)) rn
                                                   FROM (SELECT DISTINCT msd.matl_num,
                                                                msd.ean_num,
                                                                bill.*
                                                         FROM edw_material_sales_dim msd,
                                                              (SELECT DISTINCT LTRIM(material,0) material,
                                                                      cust_sls,
                                                                      InvoicePrice AS price,
                                                                      netval_inv
                                                               FROM BILL_FACT
                                                               WHERE rn = 1) bill
                                                         WHERE msd.sls_org IN ('3200','320A','320S','321A')
                                                         AND   LTRIM(msd.matl_num,'0') = LTRIM(bill.material,'0')
                                                         AND   ean_num != '') ivp) t_ivp
                                             WHERE rn = 1) t_ean,
                                             --group by ean_num
                                            edw_material_sales_dim t_msd
                                       WHERE t_msd.ean_num = t_ean.ean_num
                                       AND   t_msd.sls_org IN ('3200','320A','320S','321A')) T6
                                   ON LTRIM (T1.product_code,0) = LTRIM (T6.matl_num,0)
                                  AND LTRIM (T1.distributor_code,0) = LTRIM (T6.cust_sls,0)
                            LEFT JOIN edw_vw_os_time_dim T2 ON to_date(T1.bill_dt) = to_date(T2.cal_date)
                          GROUP BY cntry_cd,
                                   data_type,
                                   distributor_code,
                                   product_code,
                                   mnth_id