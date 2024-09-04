{{
    config(
        materialized='view'
    )
}}
with dm_integration_dly as
(
    select * from {{ ref('jpnedw_integration__dm_integration_dly') }}
),
edi_cstm_m as
(
    select * from {{ ref('jpnedw_integration__edi_cstm_m') }}
),
dw_iv_month_end as
(
    select * from {{ ref('jpnedw_integration__dw_iv_month_end') }}
),
T_BI_POSDATA as
(
    select * from {{ source('JPNEDW_INTEGRATION', 'T_BI_POSDATA') }}
),
M_ACCOUNT_STORE as
(
    select * from {{ source('JPNEDW_INTEGRATION', 'M_ACCOUNT_STORE') }}
),
ITG_MY_SELLOUT_SALES_FACT as
(
    select * from {{ ref('MYSITG_INTEGRATION__ITG_MY_SELLOUT_SALES_FACT') }}
),
ITG_MY_CUSTOMER_DIM as
(
    select * from {{ ref('MYSITG_INTEGRATION__ITG_MY_CUSTOMER_DIM') }}
),
ITG_MY_POS_SALES_FACT as
(
    select * from {{ ref('MYSITG_INTEGRATION__ITG_MY_POS_SALES_FACT') }}
),
EDW_CALENDAR_DIM as
(
    select * from {{ ref('ASPEDW_INTEGRATION__EDW_CALENDAR_DIM') }}
),
ITG_SG_POS_SALES_FACT as
(
    select * from {{ ref('SGPITG_INTEGRATION__ITG_SG_POS_SALES_FACT') }}
),
ITG_SG_ZUELLIG_SELLOUT as
(
    select * from {{ ref('sgpitg_integration__itg_sg_zuellig_sellout') }}
),
EDW_IMS_FACT as
(
    select * from {{ ref('NTAEDW_INTEGRATION__EDW_IMS_FACT') }}
),
EDW_IMS_INVENTORY_FACT as
(
    select * from {{ ref('NTAEDW_INTEGRATION__EDW_IMS_INVENTORY_FACT') }}
),
EDW_POS_FACT as
(
    select * from {{ ref('NTAEDW_INTEGRATION__EDW_POS_FACT') }}
),
EDW_ECOMMERCE_OFFTAKE_NTA as
(
    select * from {{ ref('NTAEDW_INTEGRATION__EDW_ECOMMERCE_OFFTAKE') }}
),
EDW_ECOMMERCE_OFFTAKE_IND as
(
    select * from {{ ref('INDEDW_INTEGRATION__EDW_ECOMMERCE_OFFTAKE') }}
),
EDW_DAY_CLS_STOCK_FACT as
(
    select * from {{ ref('INDEDW_INTEGRATION__EDW_DAY_CLS_STOCK_FACT') }}
),
EDW_CUSTOMER_DIM as
(
    select * from {{ ref('INDEDW_INTEGRATION__EDW_CUSTOMER_DIM') }}
),
EDW_COPA_TRANS_FACT as
(
    select * from {{ ref('ASPEDW_INTEGRATION__EDW_COPA_TRANS_FACT') }}
),
EDW_IVY_INDONESIA_NOO_ANALYSIS as
(
    select * from {{ ref('IDNEDW_INTEGRATION__EDW_IVY_INDONESIA_NOO_ANALYSIS') }}
),
EDW_DAILYSALES_FACT as
(
    select * from {{ ref('INDEDW_INTEGRATION__EDW_DAILYSALES_FACT') }}
),
EDW_RETAILER_CALENDAR_DIM as
(
    select * from {{ ref('INDEDW_INTEGRATION__EDW_RETAILER_CALENDAR_DIM') }}
),
ITG_PH_DMS_SELLOUT_SALES_FACT as
(
    select * from {{ ref('phlitg_integration__itg_ph_dms_sellout_sales_fact') }}
),
itg_ph_customer_dim as
(
    select * from {{ source('PHLITG_INTEGRATION', 'itg_ph_customer_dim') }}
),
EDW_PH_POS_ANALYSIS as
(
    select * from {{ ref('phledw_integration__edw_ph_pos_analysis') }}
),
EDW_TH_POS_ANALYSIS as
(
    select * from DEV_DNA_CORE.OSEEDW_INTEGRATION.EDW_TH_POS_ANALYSIS
),
EDW_TH_SELLOUT_ANALYSIS as
(
    select * from DEV_DNA_CORE.OSEEDW_INTEGRATION.EDW_TH_SELLOUT_ANALYSIS
),
EDW_METCASH_IND_GROCERY_FACT as
(
    select * from DEV_DNA_CORE.PCFEDW_INTEGRATION.EDW_METCASH_IND_GROCERY_FACT
),
EDW_SALES_REPORTING as
(
    select * from DEV_DNA_CORE.PCFEDW_INTEGRATION.EDW_SALES_REPORTING
),
EDW_CUBE_EXPENSE_TP_FACT as
(
    select * from DEV_DNA_CORE.CHNEDW_INTEGRATION.EDW_CUBE_EXPENSE_TP_FACT
),
EDW_CUBE_EXPENSE_TT_FACT as
(
    select * from DEV_DNA_CORE.CHNEDW_INTEGRATION.EDW_CUBE_EXPENSE_TT_FACT
),
EDW_GT_SO_FACT as
(
    select * from DEV_DNA_CORE.CHNEDW_INTEGRATION.EDW_GT_SO_FACT
),
EDW_CUBE_SALES_CUSTOMER_DIM  as
(
    select * from DEV_DNA_CORE.CHNEDW_INTEGRATION.EDW_CUBE_SALES_CUSTOMER_DIM 
),
edw_inventory_fact  as
(
    select * from DEV_DNA_CORE.CHNEDW_INTEGRATION.edw_inventory_fact
),
ITG_MY_SELLOUT_STOCK_FACT  as
(
    select * from DEV_DNA_CORE.MYSITG_INTEGRATION.ITG_MY_SELLOUT_STOCK_FACT
),
ITG_PH_DMS_SELLOUT_STOCK_FACT  as
(
    select * from DEV_DNA_CORE.OSEITG_INTEGRATION.ITG_PH_DMS_SELLOUT_STOCK_FACT
),
ITG_VN_DMS_D_SELLOUT_SALES_FACT  as
(
    select * from DEV_DNA_CORE.OSEITG_INTEGRATION.ITG_VN_DMS_D_SELLOUT_SALES_FACT
),
ITG_VN_DMS_DISTRIBUTOR_DIM  as
(
    select * from DEV_DNA_CORE.OSEITG_INTEGRATION.ITG_VN_DMS_DISTRIBUTOR_DIM
),
ITG_VN_DMS_SALES_STOCK_FACT  as
(
    select * from DEV_DNA_CORE.OSEITG_INTEGRATION.ITG_VN_DMS_SALES_STOCK_FACT
),
ITG_TH_SELLOUT_INVENTORY_FACT  as
(
    select * from DEV_DNA_CORE.OSEITG_INTEGRATION.ITG_TH_SELLOUT_INVENTORY_FACT
),
ITG_TH_DMS_INVENTORY_FACT  as
(
    select * from DEV_DNA_CORE.THAITG_INTEGRATION.ITG_TH_DMS_INVENTORY_FACT
),
VW_PACIFIC_INVENTORY  as
(
    select * from DEV_DNA_CORE.PCFEDW_INTEGRATION.VW_PACIFIC_INVENTORY
),
ITG_CUSTOMER_SELLOUT  as
(
    select * from DEV_DNA_CORE.PCFITG_INTEGRATION.ITG_CUSTOMER_SELLOUT
),
VW_PACIFIC_SELLOUT  as
(
    select * from DEV_DNA_CORE.PCFEDW_INTEGRATION.VW_PACIFIC_SELLOUT
),
VW_IRI_SCAN_SALES_ANALYSIS  as
(
    select * from DEV_DNA_CORE.PCFEDW_INTEGRATION.VW_IRI_SCAN_SALES_ANALYSIS
),
ITG_KR_ECOM_DSTR_INVENTORY  as
(
    select * from DEV_DNA_CORE.NTAITG_INTEGRATION.ITG_KR_ECOM_DSTR_INVENTORY
),
EDW_CUSTOMER_SALES_DIM  as
(
    select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_CUSTOMER_SALES_DIM
),
EDW_CUSTOMER_BASE_DIM  as
(
    select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_CUSTOMER_BASE_DIM
),
ITG_KR_ECOM_DSTR_SELLOUT  as
(
    select * from DEV_DNA_CORE.NTAITG_INTEGRATION.ITG_KR_ECOM_DSTR_SELLOUT
),
ITG_POS_OFFTAKE_FACT  as
(
    select * from DEV_DNA_CORE.INDITG_INTEGRATION.ITG_POS_OFFTAKE_FACT
),
ITG_VN_MT_SELLIN_DKSH  as
(
    select * from DEV_DNA_CORE.OSEITG_INTEGRATION.ITG_VN_MT_SELLIN_DKSH
),
ITG_VN_MT_SELLIN_TARGET  as
(
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.ITG_VN_MT_SELLIN_TARGET 
),
ITG_VN_MT_SELLOUT_CON_CUNG as
(
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.ITG_VN_MT_SELLOUT_CON_CUNG
),
ITG_VN_MT_SELLOUT_COOP as
(
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.ITG_VN_MT_SELLOUT_COOP
),
ITG_VN_MT_SELLOUT_GUARDIAN as
(
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.ITG_VN_MT_SELLOUT_GUARDIAN 
),
ITG_VN_MT_SELLOUT_LOTTE as
(
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.ITG_VN_MT_SELLOUT_LOTTE
),
ITG_VN_MT_SELLOUT_MEGA as
(
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.ITG_VN_MT_SELLOUT_MEGA
),
ITG_VN_MT_SELLOUT_VINMART as
(
    select * from DEV_DNA_CORE.VNMITG_INTEGRATION.ITG_VN_MT_SELLOUT_VINMART
),
ITG_TW_AS_WATSONS_INVENTORY as
(
    select * from DEV_DNA_CORE.NTAITG_INTEGRATION.ITG_TW_AS_WATSONS_INVENTORY 
),
ITG_MY_AS_WATSONS_INVENTORY as
(
    select * from DEV_DNA_CORE.OSEITG_INTEGRATION.ITG_MY_AS_WATSONS_INVENTORY
),
ITG_PH_AS_WATSONS_INVENTORY as
(
    select * from DEV_DNA_CORE.OSEITG_INTEGRATION.ITG_PH_AS_WATSONS_INVENTORY 
),
EDW_KR_OTC_INVENTORY as
(
    select * from DEV_DNA_CORE.NTAEDW_INTEGRATION.EDW_KR_OTC_INVENTORY
),
EDW_VW_OS_TIME_DIM as(
    select * from DEV_DNA_CORE.OSEEDW_INTEGRATION.EDW_VW_OS_TIME_DIM
),
dm_integration_dly_temp1
AS (
       SELECT jcp_data_source,
              jcp_analysis_type,
              jcp_data_category,
              jcp_create_date,
              jcp_load_date,
              SUM(jcp_qty) AS jcp_qty_sum,
              SUM(jcp_amt) AS jcp_amt_sum,
              DATE_TRUNC('day', so_rcv_dt) AS transaction_date,
              'Distributors' AS subject_name,
              jcp_cstm_cd AS account_cd,
              cstm_nm AS account_nm,
              NULL AS file_name,
              'JP' AS country
       FROM DM_INTEGRATION_DLY a
       LEFT JOIN EDI_CSTM_M b ON a.jcp_cstm_cd = b.cstm_cd
       WHERE jcp_plan_type = 'Actual'
              AND jcp_data_source = 'SO'
              AND so_rcv_dt >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY jcp_data_source,
              jcp_analysis_type,
              jcp_data_category,
              jcp_create_date,
              jcp_load_date,
              DATE_TRUNC('day', so_rcv_dt),
              --so_rcv_dt,
              jcp_cstm_cd,
              cstm_nm
       ),
dw_iv_month_end_temp1
AS (
       SELECT 'Inventory' AS jcp_data_source,
              'Inventory' AS jcp_analysis_type,
              'Inventory' AS jcp_data_category,
              DATE_TRUNC('day', Inv.create_dt) AS jcp_create_date,
              DATE_TRUNC('day',Inv.create_dt) AS jcp_load_date,
              SUM(qty) AS jcp_qty_sum,
              0 AS jcp_amt_sum,
              DATE_TRUNC('day', Inv.invt_dt) AS transaction_date,
              'Distributors' AS subject_name,
              Inv.cstm_cd AS account_cd,
              Cus.cstm_nm AS account_nm,
              NULL AS file_name,
              'JP' AS country
       FROM DW_IV_MONTH_END Inv
       LEFT JOIN EDI_CSTM_M Cus ON Inv.cstm_cd = Cus.cstm_cd
       WHERE Inv.Invt_dt >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day', Inv.create_dt),
              DATE_TRUNC('day',Inv.invt_dt),
              Inv.cstm_cd,
              Cus.cstm_nm
       ),
dm_integration_dly_temp2
AS (
       SELECT jcp_data_source,
              jcp_analysis_type,
              jcp_data_category,
              jcp_create_date,
              jcp_load_date,
              SUM(jcp_qty) AS jcp_qty_sum,
              SUM(jcp_amt) AS jcp_amt_sum,
              DATE_TRUNC('day',tp_res_payment_dt) AS transaction_date,
              'TP Accounts' AS subject_name,
              tp_promo_cd AS account_cd,
              tp_promo_nm AS account_nm,
              NULL AS file_name,
              'JP' AS country
       FROM DM_INTEGRATION_DLY
       WHERE jcp_plan_type = 'Actual'
              AND jcp_data_source = 'TP'
              AND tp_res_payment_dt >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY jcp_data_source,
              jcp_analysis_type,
              jcp_data_category,
              jcp_create_date,
              jcp_load_date,
              tp_promo_cd,
              tp_promo_nm,
              DATE_TRUNC('day',tp_res_payment_dt)
       ),
t_bi_posdata_temp1
AS (
       SELECT 'POS' AS jcp_data_source,
              'POS' AS jcp_analysis_type,
              'POS' AS jcp_data_category,
              output_dt AS jcp_create_date,
              entry_dt AS jcp_load_date,
              SUM(sales_qty) AS jcp_qty_sum,
              SUM(sales_amnt) AS jcp_amt_sum,
              DATE_TRUNC('day',sales_dt) AS transaction_date,
              'Retailers' AS subject_name,
              a.account_cd AS account_cd,
              b.account_nm AS account_nm,
              entry_fname AS file_name,
              'JP' AS country
       FROM T_BI_POSDATA a
       LEFT JOIN M_ACCOUNT_STORE b ON b.account_cd = a.account_cd
              AND a.str_cd = b.store_cd
       WHERE sales_dt >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY entry_dt,
              entry_fname,
              a.account_cd,
              b.account_nm,
              output_dt,
              DATE_TRUNC('day',sales_dt)
       ),
itg_my_sellout_sales_fact_temp1
AS (
       SELECT 'Sell-Out' AS jcp_data_source,
              'Sell-Out' AS jcp_analysis_type,
              'Sell-Out' AS jcp_data_category,
              DATE_TRUNC('day',so.crtd_dttm) AS jcp_create_date,
              DATE_TRUNC('day',so.crtd_dttm) AS jcp_load_date,
              SUM(so.qty) AS jcp_qty_sum,
              SUM(so.total_amt_aft_tax) AS jcp_amt_sum,
              DATE_TRUNC('day',so.sls_ord_dt) AS transaction_date,
              'Distributors' AS subject_name,
              so.dstrbtr_id AS account_cd,
              COALESCE(cust.dstrbtr_grp_nm, 'No Name') AS account_nm,
              '' AS file_name,
              'MY' AS country
       FROM ITG_MY_SELLOUT_SALES_FACT so
       LEFT JOIN ITG_MY_CUSTOMER_DIM cust ON so.dstrbtr_id = cust.dstrbtr_grp_cd
       WHERE TYPE = 'SI'
              AND sls_ord_dt >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day',so.crtd_dttm),
              so.dstrbtr_id,
              so.cust_cd,
              DATE_TRUNC('day',so.sls_ord_dt),
              COALESCE(cust.dstrbtr_grp_nm, 'No Name')
       ),
itg_my_pos_sales_fact_temp1
AS (
       SELECT 'POS' AS jcp_data_source,
              'POS' AS jcp_analysis_type,
              'POS' AS jcp_data_category,
              DATE_TRUNC('day',crtd_dttm) AS jcp_create_date,
              DATE_TRUNC('day',crtd_dttm) AS jcp_load_date,
              SUM(qty) AS jcp_qty_sum,
              SUM(so_val) AS jcp_amt_sum,
              DATE_TRUNC('day',MAX(cal_day)) AS transaction_date,
              'Retailers' AS subject_name,
              cust_id AS account_cd,
              cust_nm AS account_nm,
              file_nm AS file_name,
              'MY' AS country
       FROM ITG_MY_POS_SALES_FACT c
       LEFT JOIN EDW_CALENDAR_DIM d ON d.cal_wk = c.jj_yr_week_no
       WHERE cal_day >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day',crtd_dttm),
              cust_id,
              cust_nm,
              cal_day,
              file_nm
       ),
itg_sg_pos_sales_fact_temp1
AS (
       SELECT 'POS' AS jcp_data_source,
              'POS' AS jcp_analysis_type,
              'POS' AS jcp_data_category,
              NULL AS jcp_create_date,
              NULL AS jcp_load_date,
              SUM(sales_qty) AS jcp_qty_sum,
              SUM(net_sales) AS jcp_amt_sum,
              DATE_TRUNC('day',MAX(cal_day)) AS transaction_date,
              'Retailers' AS subject_name,
              cust_id AS account_cd,
              store AS account_nm,
              NULL AS file_name,
              'SG' AS country
       FROM ITG_SG_POS_SALES_FACT c
       LEFT JOIN EDW_CALENDAR_DIM d ON d.cal_wk = c.week
       WHERE cal_day >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY cust_id,
              store
       ),
itg_sg_zuellig_sellout_temp1
AS (
       SELECT 'Sell-Out' AS jcp_data_source,
              'Sell-Out' AS jcp_analysis_type,
              'Sell-Out' AS jcp_data_category,
              DATE_TRUNC('day',crtd_dttm) AS jcp_create_date,
              DATE_TRUNC('day',crtd_dttm) AS jcp_load_date,
              SUM(sales_units) AS jcp_qty_sum,
              SUM(sales_value) AS jcp_amt_sum,
              TO_CHAR(DATE_TRUNC('day', MAX(
                  CASE 
                      WHEN sales_date LIKE '%/%/%' THEN TO_DATE(sales_date, 'MM/DD/YYYY')
                      ELSE TO_DATE(sales_date, 'YYYY-MM-DD')
                  END
              )), 'YYYY-MM-DD') AS transaction_date,
              --DATE_TRUNC('day',MAX(TO_DATE(sales_date, 'yyyy-mm-dd'))) AS transaction_date,
              'Retailers' AS subject_name,
              customer_code AS account_cd,
              customer_name AS account_nm,
              NULL AS file_name,
              'SG' AS country
       FROM ITG_SG_ZUELLIG_SELLOUT
       WHERE TO_DATE(
           CASE 
               WHEN sales_date LIKE '%/%/%' THEN sales_date
               ELSE TO_CHAR(TO_DATE(sales_date, 'YYYY-MM-DD'), 'MM/DD/YYYY')
           END, 'MM/DD/YYYY')  >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day',crtd_dttm),
              customer_code,
              customer_name
       ),
edw_ims_fact_temp1
AS (
       SELECT 'Sell-Out' AS jcp_data_source,
              'Sell-Out' AS jcp_analysis_type,
              'Sell-Out' AS jcp_data_category,
              DATE_TRUNC('day',c.crt_dttm) AS jcp_create_date,
              DATE_TRUNC('day',c.crt_dttm) AS jcp_load_date,
              SUM(sls_qty) AS jcp_qty_sum,
              SUM(sls_amt) AS jcp_amt_sum,
              DATE_TRUNC('day',ims_txn_dt) AS transaction_date,
              'Distributors' AS subject_name,
              dstr_cd AS account_cd,
              dstr_nm AS account_nm,
              NULL AS file_name,
              ctry_cd AS country
       FROM EDW_IMS_FACT c
       WHERE ims_txn_dt >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY dstr_cd,
              dstr_nm,
              c.crt_dttm,
              DATE_TRUNC('day',ims_txn_dt),
              ctry_cd
       ),
edw_ims_inventory_fact_temp1
AS (
       SELECT 'Inventory' AS jcp_data_source,
              'Inventory' AS jcp_analysis_type,
              'Inventory' AS jcp_data_category,
              DATE_TRUNC('day',c.crt_dttm) AS jcp_create_date,
              DATE_TRUNC('day',c.crt_dttm) AS jcp_load_date,
              SUM(invnt_qty) AS jcp_qty_sum,
              SUM(invnt_amt) AS jcp_amt_sum,
              DATE_TRUNC('day',invnt_dt) AS transaction_date,
              'Distributors' AS subject_name,
              dstr_cd AS account_cd,
              dstr_nm AS account_nm,
              NULL AS file_name,
              ctry_cd AS country
       FROM EDW_IMS_INVENTORY_FACT c
       LEFT JOIN EDW_CALENDAR_DIM d ON DATE_PART(mon, invnt_dt) = cal_mo_1
       WHERE invnt_dt >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY dstr_cd,
              dstr_nm,
              ctry_cd,
              c.crt_dttm,
              DATE_TRUNC('day',invnt_dt)
       ),
edw_pos_fact_temp1
AS (
       SELECT 'POS' AS jcp_data_source,
              'POS' AS jcp_analysis_type,
              'POS' AS jcp_data_category,
              DATE_TRUNC('day',crt_dttm) AS jcp_create_date,
              DATE_TRUNC('day',crt_dttm) AS jcp_load_date,
              SUM(sls_qty) AS jcp_qty_sum,
              SUM(sls_amt) AS jcp_amt_sum,
              DATE_TRUNC('day',pos_dt) AS transaction_date,
              'Retailers' AS subject_name,
              sls_grp AS account_cd,
              sls_grp AS account_nm,
              NULL AS file_name,
              ctry_cd AS country
       FROM EDW_POS_FACT
       WHERE pos_dt >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY sls_grp,
              crt_dttm,
              ctry_cd,
              DATE_TRUNC('day',pos_dt)
       ),
edw_ecommerce_offtake_temp1
AS (
       SELECT 'Offtake' AS jcp_data_source,
              'Offtake' AS jcp_analysis_type,
              'Offtake' AS jcp_data_category,
              DATE_TRUNC('day',load_date) AS jcp_create_date,
              DATE_TRUNC('day',load_date) AS jcp_load_date,
              SUM(quantity) AS jcp_qty_sum,
              SUM(sales_value) AS jcp_amt_sum,
              DATE_TRUNC('day',transaction_date) AS transaction_date,
              'Retailers' AS subject_name,
              retailer_code AS account_cd,
              retailer_name AS account_nm,
              source_file_name AS file_name,
              'KR' AS country
       FROM EDW_ECOMMERCE_OFFTAKE_NTA
       WHERE transaction_date >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY retailer_code,
              retailer_name,
              load_date,
              DATE_TRUNC('day',transaction_date),
              source_file_name
       ),
edw_ecommerce_offtake_temp2
AS (
       SELECT 'Offtake' AS jcp_data_source,
              'Offtake' AS jcp_analysis_type,
              'Offtake' AS jcp_data_category,
              NULL AS jcp_create_date,
              NULL AS jcp_load_date,
              SUM(sales_qty) AS jcp_qty_sum,
              SUM(sales_value) AS jcp_amt_sum,
              DATE_TRUNC('day',TO_DATE(transaction_date, 'yyyy-mm-dd')) AS transaction_date,
              'Retailers' AS subject_name,
              account_name AS account_cd,
              account_name AS account_nm,
              NULL AS file_name,
              'IN' AS country
       FROM EDW_ECOMMERCE_OFFTAKE_IND
       WHERE TO_DATE(transaction_date, 'yyyy-mm-dd') >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY account_name,
              transaction_date,
              crt_dttm
       ),
edw_day_cls_stock_fact_temp1
AS (
       SELECT 'Inventory' AS jcp_data_source,
              'Inventory' AS jcp_analysis_type,
              'Inventory' AS jcp_data_category,
              NULL AS jcp_create_date,
              NULL AS jcp_load_date,
              SUM(salclsstock) AS jcp_qty_sum,
              SUM(salclsstockval) AS jcp_amt_sum,
              DATE_TRUNC('day', MAX(stock_date)) AS transaction_date,
              --DATE_TRUNC('day',MAX(TO_DATE(stock_date, 'yyyy-mm-dd'))) AS transaction_date,
              'Distributors' AS subject_name,
              a.customer_code AS account_cd,
              b.customer_name AS account_nm,
              NULL AS file_name,
              'IN' AS country
       FROM EDW_DAY_CLS_STOCK_FACT a
       LEFT JOIN EDW_CUSTOMER_DIM b ON a.customer_code = b.customer_code
       WHERE stock_date >= CURRENT_DATE - INTERVAL '90 days'
       --TO_DATE(stock_date, 'yyyy-mm-dd') >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY a.customer_code,
              b.customer_name
       ),
edw_copa_trans_fact_temp1
AS (
       SELECT 'Sell-In' AS jcp_data_source,
              'Sell-In' AS jcp_analysis_type,
              'Sell-In' AS jcp_data_category,
              crt_dttm AS jcp_create_date,
              crt_dttm AS jcp_load_date,
              SUM(qty) AS jcp_qty_sum,
              SUM(amt_obj_crncy) AS jcp_amt_sum,
              CASE 
                WHEN caln_day = '00000000' THEN '00010101'
                ELSE TO_DATE(caln_day, 'yyyymmdd')
            END AS transaction_date,
              --TO_DATE(caln_day, 'yyyymmdd') AS transaction_date,
              'GL Accounts' AS subject_name,
              acct_num AS account_cd,
              NULL AS account_nm,
              NULL AS file_name,
              ctry_key AS country
       FROM EDW_COPA_TRANS_FACT
        WHERE CASE 
            WHEN caln_day = '00000000' THEN '00010101'
            ELSE TO_DATE(caln_day, 'yyyymmdd')
            END >= CURRENT_DATE - INTERVAL '90 days'
       --TO_DATE(caln_day, 'yyyymmdd') >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY acct_num,
              crt_dttm,
              ctry_key,
              caln_day
       ),
edw_ivy_indonesia_noo_analysis_temp1
AS (
       SELECT 'Sell-Out' AS jcp_data_source,
              'Sell-Out' AS jcp_analysis_type,
              'Sell-Out' AS jcp_data_category,
              NULL AS jcp_create_date,
              NULL AS jcp_load_date,
              SUM(sls_qty) AS jcp_qty_sum,
              SUM(niv) AS jcp_amt_sum,
              bill_dt AS transaction_date,
              'Distributors' AS subject_name,
              jj_sap_dstrbtr_id AS account_cd,
              jj_sap_dstrbtr_nm AS account_nm,
              NULL AS file_name,
              'ID' AS country
       FROM EDW_IVY_INDONESIA_NOO_ANALYSIS
       WHERE bill_dt >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY bill_dt,
              jj_sap_dstrbtr_id,
              jj_sap_dstrbtr_nm
       ),
edw_dailysales_fact_temp1
AS (
       SELECT 'Sell-Out' AS jcp_data_source,
              'Sell-Out' AS jcp_analysis_type,
              'Sell-Out' AS jcp_data_category,
              DATE_TRUNC('day',sf.crt_dttm) AS jcp_create_date,
              DATE_TRUNC('day',sf.crt_dttm) AS jcp_load_date,
              SUM(quantity) AS jcp_qty_sum,
              SUM(gross_amt) AS jcp_amt_sum,
              cd.caldate AS transaction_date,
              'Distributors' AS subject_name,
              sf.customer_code AS account_cd,
              b.customer_name AS account_nm,
              NULL AS file_name,
              'IN' AS country
       FROM EDW_DAILYSALES_FACT sf
       LEFT JOIN EDW_RETAILER_CALENDAR_DIM cd ON sf.invoice_date = cd.day
       LEFT JOIN EDW_CUSTOMER_DIM b ON sf.customer_code = b.customer_code
       WHERE cd.caldate >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY sf.customer_code,
              b.customer_name,
              sf.crt_dttm,
              cd.caldate
       ),
itg_ph_dms_sellout_sales_fact_temp1
AS (
       SELECT 'Sell-Out' AS jcp_data_source,
              'Sell-Out' AS jcp_analysis_type,
              'Sell-Out' AS jcp_data_category,
              NULL AS jcp_create_date,
              NULL AS jcp_load_date,
              SUM(so.Qty) AS jcp_qty_sum,
              SUM(so.nts_val) AS jcp_amt_sum,
              so.invoice_dt AS transaction_date,
              'Distributors' AS subject_name,
              so.dstrbtr_grp_cd AS account_cd,
              cust.dstrbtr_grp_nm AS account_nm,
              NULL AS file_name,
              'PH' AS country
       FROM ITG_PH_DMS_SELLOUT_SALES_FACT so
       LEFT JOIN itg_ph_customer_dim cust ON so.dstrbtr_grp_cd = cust.dstrbtr_grp_cd
       WHERE so.invoice_dt >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY so.dstrbtr_grp_cd,
              so.invoice_dt,
              cust.dstrbtr_grp_nm
       ),
edw_ph_pos_analysis_temp1
AS (
       SELECT 'POS' AS jcp_data_source,
              'POS' AS jcp_analysis_type,
              'POS' AS jcp_data_category,
              NULL AS jcp_create_date,
              NULL AS jcp_load_date,
              SUM(jj_qty_pc) AS jcp_qty_sum,
              SUM(jj_nts) AS jcp_amt_sum,
              TO_DATE(jj_mnth_id || '01', 'yyyymmdd') AS transaction_date,
              'Retailer' AS subject_name,
              cust_cd AS account_cd,
              parent_customer AS account_nm,
              NULL AS file_name,
              'PH' AS country
       FROM EDW_PH_POS_ANALYSIS
       WHERE TO_DATE(jj_mnth_id || '01', 'yyyymmdd') >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY jj_mnth_id,
              cust_cd,
              parent_customer
       ),
edw_th_pos_analysis_temp1
AS (
       SELECT 'POS' AS jcp_data_source,
              'POS' AS jcp_analysis_type,
              'POS' AS jcp_data_category,
              NULL AS jcp_create_date,
              NULL AS jcp_load_date,
              SUM(sale_quantity) AS jcp_qty_sum,
              SUM(salesbaht) AS jcp_amt_sum,
              DATE_TRUNC('day',invoice_date) AS transaction_date,
              'Retailer' AS subject_name,
              customer_code AS account_cd,
              customer_code AS account_nm,
              NULL AS file_name,
              'TH' AS country
       FROM EDW_TH_POS_ANALYSIS
       WHERE invoice_date >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY invoice_date,
              customer_code
       ),
edw_th_sellout_analysis_temp1
AS (
       SELECT 'Sell-Out' AS jcp_data_source,
              'Sell-Out' AS jcp_analysis_type,
              'Sell-Out' AS jcp_data_category,
              NULL AS jcp_create_date,
              NULL AS jcp_load_date,
              SUM(so.quantity_dz) AS jcp_qty_sum,
              SUM(so.net_invoice) AS jcp_amt_sum,
              DATE_TRUNC('day',so.order_date) AS transaction_date,
              'Distributor' AS subject_name,
              distributor_id AS account_cd,
              distributor_id AS account_nm,
              NULL AS file_name,
              'TH' AS country
       FROM EDW_TH_SELLOUT_ANALYSIS so
       WHERE so.order_date >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY so.order_date,
              so.distributor_id
       ),
edw_metcash_ind_grocery_fact_temp1
AS (
       SELECT 'Sell-Out (Metcash)' AS jcp_data_source,
              'Sell-Out (Metcash)' AS jcp_analysis_type,
              'Sell-Out (Metcash)' AS jcp_data_category,
              NULL AS jcp_create_date,
              NULL AS jcp_load_date,
              SUM(gross_sales) AS jcp_qty_sum,
              SUM(gross_units) AS jcp_amt_sum,
              DATE_TRUNC('day',cal_date) AS transaction_date,
              'Banner' AS subject_name,
              banner_id AS account_cd,
              banner AS account_nm,
              NULL AS file_name,
              'AU' AS country
       FROM EDW_METCASH_IND_GROCERY_FACT
       WHERE cal_date >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY cal_date,
              banner_id,
              banner
       ),
edw_sales_reporting_temp1
AS (
       SELECT 'Customer Plan' AS jcp_data_source,
              'Customer Plan' AS jcp_analysis_type,
              'Customer Plan' AS jcp_data_category,
              NULL AS jcp_create_date,
              NULL AS jcp_load_date,
              SUM(goal_qty) AS jcp_qty_sum,
              SUM(goal_nts) AS jcp_amt_sum,
              DATE_TRUNC('day',snap_shot_dt) AS transaction_date,
              'Sales Offices' AS subject_name,
              sales_office_cd AS account_cd,
              sales_office_desc AS account_nm,
              NULL AS file_name,
              ctry_key AS country
       FROM EDW_SALES_REPORTING
       WHERE pac_source_type = 'FILE'
              AND ctry_key = 'AU'
       GROUP BY snap_shot_dt,
              sales_office_cd,
              sales_office_desc,
              ctry_key
       ),
edw_sales_reporting_temp2
AS (
       SELECT 'Promax Forecast' AS jcp_data_source,
              'Promax Forecast' AS jcp_analysis_type,
              'Promax Forecast' AS jcp_data_category,
              NULL AS jcp_create_date,
              NULL AS jcp_load_date,
              SUM(px_qty) AS jcp_qty_sum,
              SUM(px_gts) AS jcp_amt_sum,
              DATE_TRUNC('day',snap_shot_dt) AS transaction_date,
              'Sales Offices' AS subject_name,
              cust_no AS account_cd,
              cust_nm AS account_nm,
              NULL AS file_name,
              ctry_key AS country
       FROM EDW_SALES_REPORTING
       WHERE pac_source_type = 'PROMAX'
              AND pac_subsource_type = 'PX_FORECAST'
              AND ctry_key = 'AU'
       GROUP BY snap_shot_dt,
              cust_no,
              cust_nm,
              ctry_key
       ),
edw_sales_reporting_temp3
AS (
       SELECT 'Promax Terms' AS jcp_data_source,
              'Promax Terms' AS jcp_analysis_type,
              'Promax Terms' AS jcp_data_category,
              NULL AS jcp_create_date,
              NULL AS jcp_load_date,
              SUM(px_qty) AS jcp_qty_sum,
              SUM(px_gts) AS jcp_amt_sum,
              DATE_TRUNC('day',snap_shot_dt) AS transaction_date,
              'Sales Offices' AS subject_name,
              cust_no AS account_cd,
              cust_nm AS account_nm,
              NULL AS file_name,
              ctry_key AS country
       FROM EDW_SALES_REPORTING
       WHERE pac_source_type = 'PROMAX'
              AND pac_subsource_type = 'PX_TERMS'
              AND ctry_key = 'AU'
       GROUP BY snap_shot_dt,
              cust_no,
              cust_nm,
              ctry_key
       ),
edw_cube_expense_tp_fact_temp1
AS (
       SELECT 'TP' AS jcp_data_source,
              'TP' AS jcp_analysis_type,
              'TP' AS jcp_data_category,
              insert_date AS jcp_create_date,
              insert_date AS jcp_load_date,
              NULL AS jcp_qty_sum,
              SUM(act_a) AS jcp_amt_sum,
              TO_DATE(budget_year || budget_month || '01', 'yyyymmdd') AS transaction_date,
              'GL Accounts' AS subject_name,
              gl_account AS account_cd,
              NULL AS account_nm,
              NULL AS file_name,
              'CN' AS country
       FROM EDW_CUBE_EXPENSE_TP_FACT
       GROUP BY insert_date,
              budget_year,
              budget_month,
              gl_account
       ),
edw_cube_expense_tt_fact_temp1
AS (
       SELECT 'TT' AS jcp_data_source,
              'TT' AS jcp_analysis_type,
              'TT' AS jcp_data_category,
              insert_date AS jcp_create_date,
              insert_date AS jcp_load_date,
              NULL AS jcp_qty_sum,
              SUM(act_a) AS jcp_amt_sum,
              TO_DATE(budget_year || budget_month || '01', 'yyyymmdd') AS transaction_date,
              'TT Type' AS subject_name,
              cctp_type AS account_cd,
              NULL AS account_nm,
              NULL AS file_name,
              'CN' AS country
       FROM EDW_CUBE_EXPENSE_TT_FACT
       GROUP BY insert_date,
              budget_year,
              budget_month,
              cctp_type
       ),
edw_gt_so_fact_temp1
AS (
       SELECT 'Sell-Out' AS jcp_data_source,
              'Sell-Out' AS jcp_analysis_type,
              'Sell-Out' AS jcp_data_category,
              NULL AS jcp_create_date,
              NULL AS jcp_load_date,
              SUM(so_qty) AS jcp_qty_sum,
              SUM(so_gts) AS jcp_amt_sum,
              st_date AS transaction_date,
              'Distributors' AS subject_name,
              a.customer_code AS account_cd,
              customer_name AS account_nm,
              NULL AS file_name,
              'CN' AS country
       FROM EDW_GT_SO_FACT a
       LEFT JOIN EDW_CUBE_SALES_CUSTOMER_DIM b ON a.customer_code = b.customer_code
       WHERE st_date >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY st_date,
              a.customer_code,
              b.customer_name
       ),
edw_inventory_fact_temp1
AS (
       SELECT 'Inventory' AS jcp_data_source,
              'Inventory' AS jcp_analysis_type,
              'Inventory' AS jcp_data_category,
              DATE_TRUNC('day',a.insert_date) AS jcp_create_date,
              DATE_TRUNC('day',a.insert_date) AS jcp_load_date,
              SUM(inv_qty) AS jcp_qty_sum,
              SUM(inv_gts) AS jcp_amt_sum,
              TO_DATE(a.yyyymm || '01', 'yyyymmdd') AS transaction_date,
              'Distributors' AS subject_name,
              a.customer_code AS account_cd,
              b.customer_name AS account_nm,
              NULL AS file_name,
              'CN' AS country
       FROM edw_inventory_fact a
       LEFT JOIN EDW_CUBE_SALES_CUSTOMER_DIM b ON a.customer_code = b.customer_code
       GROUP BY a.insert_date,
              a.yyyymm,
              a.customer_code,
              b.customer_name
       ),
itg_my_sellout_stock_fact_temp1
AS (
       SELECT 'Inventory' AS jcp_data_source,
              'Inventory' AS jcp_analysis_type,
              'Inventory' AS jcp_data_category,
              DATE_TRUNC('day',Stk.crtd_dttm) AS jcp_create_date,
              DATE_TRUNC('day',Stk.crtd_dttm) AS jcp_load_date,
              SUM(Stk.qty) AS jcp_qty_sum,
              SUM(Stk.total_val) AS jcp_amt_sum,
              Stk.inv_dt AS transaction_date,
              'Distributors' AS subject_name,
              Stk.cust_id AS account_cd,
              COALESCE(cust.dstrbtr_grp_nm, 'No Name') AS account_nm,
              NULL AS file_name,
              'MY' AS country
       FROM ITG_MY_SELLOUT_STOCK_FACT Stk
       LEFT JOIN ITG_MY_CUSTOMER_DIM cust ON Stk.cust_id = cust.dstrbtr_grp_cd
       WHERE Stk.inv_dt >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day',Stk.crtd_dttm),
              Stk.inv_dt,
              Stk.cust_id,
              cust.dstrbtr_grp_nm
       ),
itg_ph_dms_sellout_stock_fact_temp1
AS (
       SELECT 'Inventory' AS jcp_data_source,
              'Inventory' AS jcp_analysis_type,
              'Inventory' AS jcp_data_category,
              DATE_TRUNC('day',Stk.crtd_dttm) AS jcp_create_date,
              DATE_TRUNC('day',Stk.crtd_dttm) AS jcp_load_date,
              SUM(Stk.qty) AS jcp_qty_sum,
              SUM(Stk.amt) AS jcp_amt_sum,
              Stk.inv_dt AS transaction_date,
              'Distributors' AS subject_name,
              Stk.dstrbtr_grp_cd AS account_cd,
              cust.dstrbtr_grp_nm AS account_nm,
              NULL AS file_name,
              'PH' AS country
       FROM ITG_PH_DMS_SELLOUT_STOCK_FACT Stk
       LEFT JOIN ITG_PH_CUSTOMER_DIM cust ON stk.dstrbtr_grp_cd = cust.dstrbtr_grp_cd
       WHERE Stk.inv_dt >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day',Stk.crtd_dttm),
              Stk.inv_dt,
              Stk.dstrbtr_grp_cd,
              cust.dstrbtr_grp_nm
       ),
itg_vn_dms_d_sellout_sales_fact_temp1
AS (
       SELECT 'Sell-Out' AS jcp_data_source,
              'Sell-Out' AS jcp_analysis_type,
              'Sell-Out' AS jcp_data_category,
              DATE_TRUNC('day',so.crtd_dttm) AS jcp_create_date,
              DATE_TRUNC('day',So.crtd_dttm) AS jcp_load_date,
              SUM(So.quantity) AS jcp_qty_sum,
              SUM(So.total_sellout_afvat_bfdisc) AS jcp_amt_sum,
              So.invoice_date AS transaction_date,
              'Distributors' AS subject_name,
              Cust.territory_dist AS account_cd,
              NULL AS account_nm,
              NULL AS file_name,
              'VN' AS country
       FROM ITG_VN_DMS_D_SELLOUT_SALES_FACT SO
       LEFT JOIN ITG_VN_DMS_DISTRIBUTOR_DIM cust ON so.dstrbtr_id = cust.dstrbtr_id
       WHERE so.invoice_date >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day',So.crtd_dttm),
              So.invoice_date,
              Cust.territory_dist
       ),
itg_vn_dms_sales_stock_fact_temp1
AS (
       SELECT 'Inventory' AS jcp_data_source,
              'Inventory' AS jcp_analysis_type,
              'Inventory' AS jcp_data_category,
              DATE_TRUNC('day',Stk.crtd_dttm) AS jcp_create_date,
              DATE_TRUNC('day',Stk.crtd_dttm) AS jcp_load_date,
              SUM(Stk.quantity) AS jcp_qty_sum,
              SUM(Stk.amount) AS jcp_amt_sum,
              Stk.date AS transaction_date,
              'Distributors' AS subject_name,
              Cust.territory_dist AS account_cd,
              NULL AS account_nm,
              NULL AS file_name,
              'VN' AS country
       FROM ITG_VN_DMS_SALES_STOCK_FACT Stk
       LEFT JOIN ITG_VN_DMS_DISTRIBUTOR_DIM cust ON stk.dstrbtr_id = cust.dstrbtr_id
       WHERE Stk.date >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day',Stk.crtd_dttm),
              Stk.date,
              Cust.territory_dist
       ),
itg_th_sellout_inventory_fact_temp1
AS (
       SELECT 'Inventory' AS jcp_data_source,
              'Inventory' AS jcp_analysis_type,
              'Inventory' AS jcp_data_category,
              DATE_TRUNC('day',Stk.crt_date) AS jcp_create_date,
              DATE_TRUNC('day',Stk.crt_date) AS jcp_load_date,
              SUM(Stk.Qty) AS jcp_qty_sum,
              SUM(Stk.Amt) AS jcp_amt_sum,
              DATE_TRUNC('day',Stk.rec_dt) AS transaction_date,
              'Distributors' AS subject_name,
              Stk.dstrbtr_id AS account_cd,
              Stk.dstrbtr_id AS account_nm,
              NULL AS file_name,
              'TH' AS country
       FROM ITG_TH_SELLOUT_INVENTORY_FACT Stk
       WHERE Stk.rec_dt >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day',Stk.crt_date),
              DATE_TRUNC('day',Stk.rec_dt),
              Stk.dstrbtr_id
       ),
itg_th_dms_inventory_fact_temp1
AS (
       SELECT 'Inventory' AS jcp_data_source,
              'Inventory' AS jcp_analysis_type,
              'Inventory' AS jcp_data_category,
              DATE_TRUNC('day',Stk.curr_date) AS jcp_create_date,
              DATE_TRUNC('day',Stk.curr_date) AS jcp_load_date,
              SUM(Stk.Qty) AS jcp_qty_sum,
              SUM(Stk.Amount) AS jcp_amt_sum,
              DATE_TRUNC('day',Stk.recdate) AS transaction_date,
              'Distributors' AS subject_name,
              Stk.distributorid AS account_cd,
              Stk.distributorid AS account_nm,
              NULL AS file_name,
              'TH' AS country
       FROM ITG_TH_DMS_INVENTORY_FACT Stk
       WHERE Stk.recdate >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day',Stk.curr_date),
              DATE_TRUNC('day',Stk.recdate),
              Stk.distributorid
       ),
vw_pacific_inventory_temp1
AS (
       SELECT 'Inventory' AS jcp_data_source,
              'Inventory' AS jcp_analysis_type,
              'Inventory' AS jcp_data_category,
              DATE_TRUNC('day',inv.crt_dttm) AS jcp_create_date,
              DATE_TRUNC('day',inv.crt_dttm) AS jcp_load_date,
              SUM(inv.inventory_Qty) AS jcp_qty_sum,
              SUM(inv.inventory_amount) AS jcp_amt_sum,
              DATE_TRUNC('day',inv.inv_date) AS transaction_date,
              'Distributors' AS subject_name,
              inv.sap_parent_customer_key AS account_cd,
              inv.sap_parent_customer_desc AS account_nm,
              NULL AS file_name,
              'AU' AS country
       FROM VW_PACIFIC_INVENTORY inv
       WHERE inv_date >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day',crt_dttm),
              DATE_TRUNC('day',inv_date),
              sap_parent_customer_key,
              sap_parent_customer_desc
       ),
itg_customer_sellout_temp1
AS (
       SELECT 'Sell-Out' AS jcp_data_source,
              'Sell-Out' AS jcp_analysis_type,
              'Sell-Out' AS jcp_data_category,
              DATE_TRUNC('day',so.crt_dttm) AS jcp_create_date,
              DATE_TRUNC('day',so.crt_dttm) AS jcp_load_date,
              SUM(so.so_qty) AS jcp_qty_sum,
              SUM(so.so_qty * std_cost) AS jcp_amt_sum,
              DATE_TRUNC('day',so.so_date) AS transaction_date,
              'Distributors' AS subject_name,
              so.sap_parent_customer_key AS account_cd,
              so.sap_parent_customer_desc AS account_nm,
              NULL AS file_name,
              'AU' AS country
       FROM ITG_CUSTOMER_SELLOUT so
       WHERE so_date >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day',crt_dttm),
              DATE_TRUNC('day',so_date),
              sap_parent_customer_key,
              sap_parent_customer_desc
       ),
vw_pacific_sellout_temp1
AS (
       SELECT 'Sell-Out' AS jcp_data_source,
              'Sell-Out' AS jcp_analysis_type,
              'Sell-Out' AS jcp_data_category,
              DATE_TRUNC('day',current_timestamp()) AS jcp_create_date,
              to_date(so.month, 'YYYYMM') AS jcp_load_date,
              SUM(so.so_qty) AS jcp_qty_sum,
              SUM(so.so_value) AS jcp_amt_sum,
              to_date(so.month, 'YYYYMM') AS transaction_date,
              'Distributors' AS subject_name,
              so.sap_prnt_cust_key AS account_cd,
              so.sap_prnt_cust_desc AS account_nm,
              NULL AS file_name,
              'AU' AS country
       FROM VW_PACIFIC_SELLOUT so
       WHERE to_date(month, 'YYYYMM') >= CURRENT_DATE - INTERVAL '90 days'
              AND sap_prnt_cust_desc = 'METCASH'
       GROUP BY DATE_TRUNC('day',current_timestamp()),
              to_date(month, 'YYYYMM'),
              sap_prnt_cust_key,
              sap_prnt_cust_desc
       ),
vw_iri_scan_sales_analysis_temp1
AS (
       SELECT 'POS' AS jcp_data_source,
              'POS' AS jcp_analysis_type,
              'POS' AS jcp_data_category,
              NULL AS jcp_create_date,
              NULL AS jcp_load_date,
              SUM(scan_units) AS jcp_qty_sum,
              SUM(scan_sales) AS jcp_amt_sum,
              DATE_TRUNC('day',wk_end_dt) AS transaction_date,
              'Customers' AS subject_name,
              representative_cust_cd AS account_cd,
              representative_cust_nm AS account_nm,
              NULL AS file_name,
              'AU' AS country
       FROM VW_IRI_SCAN_SALES_ANALYSIS
       WHERE wk_end_dt >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day',wk_end_dt),
              representative_cust_cd,
              representative_cust_nm
       ),
itg_kr_ecom_dstr_inventory_temp1
AS (
       SELECT 'Inventory' AS jcp_data_source,
              'Inventory' AS jcp_analysis_type,
              'Inventory' AS jcp_data_category,
              DATE_TRUNC('day',inv.crt_dttm) AS jcp_create_date,
              DATE_TRUNC('day',inv.crt_dttm) AS jcp_load_date,
              SUM(inv.inventory_Qty) AS jcp_qty_sum,
              SUM(inv.inventory_Qty) AS jcp_amt_sum,
              DATE_TRUNC('day',inv.inv_date) AS transaction_date,
              'Distributors' AS subject_name,
              inv.dstr_cd AS account_cd,
              cust.sap_cust_nm AS account_nm,
              NULL AS file_name,
              'KR' AS country
       FROM ITG_KR_ECOM_DSTR_INVENTORY inv,
              (
                     SELECT DISTINCT ltrim(ECBD.CUST_NUM, 0) AS SAP_CUST_ID,
                            ECBD.CUST_NM AS SAP_CUST_NM
                     FROM EDW_CUSTOMER_SALES_DIM ECSD,
                            EDW_CUSTOMER_BASE_DIM ECBD
                     WHERE ECSD.CUST_NUM = ECBD.CUST_NUM
                            AND ECSD.SLS_ORG IN ('3200', '320A', '320S', '321A')
                     ) cust
       WHERE inv_date >= CURRENT_DATE - INTERVAL '90 days'
              AND inv.dstr_cd = cust.sap_cust_id(+)
       GROUP BY DATE_TRUNC('day',crt_dttm),
              DATE_TRUNC('day',inv_date),
              dstr_cd,
              SAP_CUST_NM
       ),
itg_kr_ecom_dstr_sellout_temp1
AS (
       SELECT 'Sell-Out' AS jcp_data_source,
              'Sell-Out' AS jcp_analysis_type,
              'Sell-Out' AS jcp_data_category,
              DATE_TRUNC('day',so.crt_dttm) AS jcp_create_date,
              DATE_TRUNC('day',so.crt_dttm) AS jcp_load_date,
              SUM(so.so_qty) AS jcp_qty_sum,
              SUM(so.so_qty) AS jcp_amt_sum,
              DATE_TRUNC('day',so.so_date) AS transaction_date,
              'Distributors' AS subject_name,
              so.dstr_cd AS account_cd,
              cust.sap_cust_nm AS account_nm,
              NULL AS file_name,
              'KR' AS country
       FROM ITG_KR_ECOM_DSTR_SELLOUT so,
              (
                     SELECT DISTINCT ltrim(ECBD.CUST_NUM, 0) AS SAP_CUST_ID,
                            ECBD.CUST_NM AS SAP_CUST_NM
                     FROM EDW_CUSTOMER_SALES_DIM ECSD,
                            EDW_CUSTOMER_BASE_DIM ECBD
                     WHERE ECSD.CUST_NUM = ECBD.CUST_NUM
                            AND ECSD.SLS_ORG IN ('3200', '320A', '320S', '321A')
                     ) cust
       WHERE so_date >= CURRENT_DATE - INTERVAL '90 days'
              AND so.dstr_cd = cust.sap_cust_id(+)
       GROUP BY DATE_TRUNC('day',crt_dttm),
              DATE_TRUNC('day',so_date),
              dstr_cd,
              sap_cust_nm
       ),
itg_pos_offtake_fact_temp1
AS (
       SELECT 'POS' AS jcp_data_source,
              'POS' AS jcp_analysis_type,
              'POS' AS jcp_data_category,
              DATE_TRUNC('day',ot.crt_dttm) AS jcp_create_date,
              DATE_TRUNC('day',ot.file_upload_date) AS jcp_load_date,
              SUM(ot.sls_qty) AS jcp_qty_sum,
              SUM(ot.sls_val_lcy) AS jcp_amt_sum,
              DATE_TRUNC('day',ot.pos_dt) AS transaction_date,
              'Retailers' AS subject_name,
              ot.key_account_name AS account_cd,
              key_account_name AS account_nm,
              source_file_name AS file_name,
              'IN' AS country
       FROM ITG_POS_OFFTAKE_FACT ot
       WHERE pos_dt >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day',crt_dttm),
              DATE_TRUNC('day',pos_dt),
              DATE_TRUNC('day',ot.file_upload_date),
              source_file_name,
              key_account_name
       ),
itg_vn_mt_sellin_dksh_temp1
AS (
       SELECT 'Sell-Out' AS jcp_data_source,
              'Sell-Out' AS jcp_analysis_type,
              'Sell-Out' AS jcp_data_category,
              DATE_TRUNC('day',so.crtd_dttm) AS jcp_create_date,
              DATE_TRUNC('day',so.crtd_dttm) AS jcp_load_date,
              SUM(so.qty_exclude_foc) AS jcp_qty_sum,
              SUM(so.net_amount_wo_vat) AS jcp_amt_sum,
              to_date(so.invoice_date, 'YYYYMMDD') AS transaction_date,
              'Distributors' AS subject_name,
              'DKSH' AS account_cd,
              'DKSH' AS account_nm,
              filename AS file_name,
              'VN' AS country
       FROM ITG_VN_MT_SELLIN_DKSH so
       WHERE to_date(so.invoice_date, 'YYYYMMDD') >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day',crtd_dttm),
              to_date(so.invoice_date, 'YYYYMMDD'),
              filename
       ),
itg_vn_mt_sellin_target_temp1
AS (
       SELECT 'Sell-Out' AS jcp_data_source,
              'Sell-Out' AS jcp_analysis_type,
              'Sell-Out' AS jcp_data_category,
              DATE_TRUNC('day',so.crtd_dttm) AS jcp_create_date,
              DATE_TRUNC('day',so.crtd_dttm) AS jcp_load_date,
              SUM(so.target) AS jcp_qty_sum,
              SUM(so.target) AS jcp_amt_sum,
              to_date(so.sellin_year || lpad(so.sellin_cycle, 2, '0'), 'YYYYMM') AS transaction_date,
              'Distributors' AS subject_name,
              'TARGET' AS account_cd,
              'TARGET' AS account_nm,
              filename AS file_name,
              'VN' AS country
       FROM ITG_VN_MT_SELLIN_TARGET so
       WHERE to_date(so.sellin_year || lpad(so.sellin_cycle, 2, '0'), 'YYYYMM') >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day',crtd_dttm),
              to_date(so.sellin_year || lpad(so.sellin_cycle, 2, '0'), 'YYYYMM'),
              filename
       ),
itg_vn_mt_sellout_con_cung_temp1
AS (
       SELECT 'POS' AS jcp_data_source,
              'POS' AS jcp_analysis_type,
              'POS' AS jcp_data_category,
              DATE_TRUNC('day',ot.crtd_dttm) AS jcp_create_date,
              DATE_TRUNC('day',ot.crtd_dttm) AS jcp_load_date,
              SUM(ot.quantity) AS jcp_qty_sum,
              SUM(ot.quantity) AS jcp_amt_sum,
              to_date(ot.year || ot.month, 'YYYYMM') AS transaction_date,
              'Retailers' AS subject_name,
              'CON_CUNG' AS account_cd,
              'CON_CUNG' AS account_nm,
              filename AS file_name,
              'VN' AS country
       FROM ITG_VN_MT_SELLOUT_CON_CUNG ot
       WHERE to_date(ot.year || ot.month, 'YYYYMM') >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day',crtd_dttm),
              to_date(ot.year || ot.month, 'YYYYMM'),
              filename
       ),
itg_vn_mt_sellout_coop_temp1
AS (
       SELECT 'POS' AS jcp_data_source,
              'POS' AS jcp_analysis_type,
              'POS' AS jcp_data_category,
              DATE_TRUNC('day',ot.crtd_dttm) AS jcp_create_date,
              DATE_TRUNC('day',ot.crtd_dttm) AS jcp_load_date,
              SUM(ot.sales_amount) AS jcp_qty_sum,
              SUM(ot.sales_amount) AS jcp_amt_sum,
              to_date(ot.year || ot.month, 'YYYYMM') AS transaction_date,
              'Retailers' AS subject_name,
              'COOP' AS account_cd,
              'COOP' AS account_nm,
              filename AS file_name,
              'VN' AS country
       FROM ITG_VN_MT_SELLOUT_COOP ot
       WHERE to_date(ot.year || ot.month, 'YYYYMM') >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day',crtd_dttm),
              to_date(ot.year || ot.month, 'YYYYMM'),
              filename
       ),
itg_vn_mt_sellout_guardian_temp1
AS (
       SELECT 'POS' AS jcp_data_source,
              'POS' AS jcp_analysis_type,
              'POS' AS jcp_data_category,
              DATE_TRUNC('day',ot.crtd_dttm) AS jcp_create_date,
              DATE_TRUNC('day',ot.crtd_dttm) AS jcp_load_date,
              SUM(ot.sales_supplier) AS jcp_qty_sum,
              SUM(ot.amount) AS jcp_amt_sum,
              to_date(ot.year || ot.month, 'YYYYMM') AS transaction_date,
              'Retailers' AS subject_name,
              'GUARDIAN' AS account_cd,
              'GUARDIAN' AS account_nm,
              filename AS file_name,
              'VN' AS country
       FROM ITG_VN_MT_SELLOUT_GUARDIAN ot
       WHERE to_date(ot.year || ot.month, 'YYYYMM') >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day',crtd_dttm),
              to_date(ot.year || ot.month, 'YYYYMM'),
              filename
       ),
itg_vn_mt_sellout_lotte_temp1
AS (
       SELECT 'POS' AS jcp_data_source,
              'POS' AS jcp_analysis_type,
              'POS' AS jcp_data_category,
              DATE_TRUNC('day',ot.crtd_dttm) AS jcp_create_date,
              DATE_TRUNC('day',ot.crtd_dttm) AS jcp_load_date,
              SUM(ot.sale_qty) AS jcp_qty_sum,
              SUM(ot.tot_sale_amt) AS jcp_amt_sum,
              to_date(ot.year || ot.month, 'YYYYMM') AS transaction_date,
              'Retailers' AS subject_name,
              'LOTTE' AS account_cd,
              'LOTTE' AS account_nm,
              filename AS file_name,
              'VN' AS country
       FROM ITG_VN_MT_SELLOUT_LOTTE ot
       WHERE to_date(ot.year || ot.month, 'YYYYMM') >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day',crtd_dttm),
              to_date(ot.year || ot.month, 'YYYYMM'),
              filename
       ),
itg_vn_mt_sellout_mega_temp1
AS (
       SELECT 'POS' AS jcp_data_source,
              'POS' AS jcp_analysis_type,
              'POS' AS jcp_data_category,
              DATE_TRUNC('day',ot.crtd_dttm) AS jcp_create_date,
              DATE_TRUNC('day',ot.crtd_dttm) AS jcp_load_date,
              SUM(ot.sale_qty) AS jcp_qty_sum,
              SUM(ot.cogs_amt) AS jcp_amt_sum,
              to_date(ot.year || ot.month, 'YYYYMM') AS transaction_date,
              'Retailers' AS subject_name,
              'MEGA' AS account_cd,
              'MEGA' AS account_nm,
              filename AS file_name,
              'VN' AS country
       FROM ITG_VN_MT_SELLOUT_MEGA ot
       WHERE to_date(ot.year || ot.month, 'YYYYMM') >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day',crtd_dttm),
              to_date(ot.year || ot.month, 'YYYYMM'),
              filename
       ),
itg_vn_mt_sellout_vinmart_temp1
AS (
       SELECT 'POS' AS jcp_data_source,
              'POS' AS jcp_analysis_type,
              'POS' AS jcp_data_category,
              DATE_TRUNC('day',ot.crtd_dttm) AS jcp_create_date,
              DATE_TRUNC('day',ot.crtd_dttm) AS jcp_load_date,
              SUM(ot.pos_quantity) AS jcp_qty_sum,
              SUM(ot.pos_revenue) AS jcp_amt_sum,
              to_date(ot.year || ot.month, 'YYYYMM') AS transaction_date,
              'Retailers' AS subject_name,
              'VINMART' AS account_cd,
              'VINMART' AS account_nm,
              filename AS file_name,
              'VN' AS country
       FROM ITG_VN_MT_SELLOUT_VINMART ot
       WHERE to_date(ot.year || ot.month, 'YYYYMM') >= CURRENT_DATE - INTERVAL '90 days'
       GROUP BY DATE_TRUNC('day',crtd_dttm),
              to_date(ot.year || ot.month, 'YYYYMM'),
              filename
       ),
itg_tw_as_watsons_inventory_temp1
AS (
       SELECT 'Inventory' AS jcp_data_source,
              'Inventory' AS jcp_analysis_type,
              'Inventory' AS jcp_data_category,
              DATE_TRUNC('day',Inv.crtd_dttm) AS jcp_create_date,
              DATE_TRUNC('day',Inv.crtd_dttm) AS jcp_load_date,
              SUM(total_stock_qty) AS jcp_qty_sum,
              SUM(total_stock_value) AS jcp_amt_sum,
              TO_DATE(inv.year || '-' || SUBSTRING(TD.MNTH_ID, 5, 2) || '-15', 'YYYY-MM-DD') AS transaction_date,
              --CAST(inv.year + '-' + SUBSTRING(TD.MNTH_ID, 5, 2) + '-' + '15' AS DATE) AS transaction_date,
              'Distributors' AS subject_name,
              '107323' AS account_cd,
              'AS WATSONS' AS account_nm,
              filename AS file_name,
              'TW' AS country
       FROM ITG_TW_AS_WATSONS_INVENTORY Inv,
              (
                     SELECT YEAR,
                            MNTH_ID,
                            WK
                     FROM EDW_VW_OS_TIME_DIM
                     GROUP BY YEAR,
                            MNTH_ID,
                            WK
                     ) AS TD
       WHERE LTRIM(INV.WEEK_NO, '0') = LTRIM(TD.WK, '0')
              AND INV.YEAR = TD.YEAR
              AND TD.MNTH_ID >= TO_NUMBER(TO_CHAR(DATEADD(MONTH, -3, CURRENT_DATE), 'YYYYMM')) 
              --TD.MNTH_ID >= CAST(TO_CHAR(ADD_MONTHS(DATE_TRUNC('day',current_timestamp()), - 3), 'YYYYMM') AS INTEGER)
       GROUP BY Inv.crtd_dttm,
               TO_DATE(inv.year || '-' || SUBSTRING(TD.MNTH_ID, 5, 2) || '-15', 'YYYY-MM-DD'),
              --CAST(inv.year + '-' + SUBSTRING(TD.MNTH_ID, 5, 2) + '-' + '15' AS DATE),
              filename
       ),
itg_my_as_watsons_inventory_temp1
AS (
       SELECT 'Inventory' AS jcp_data_source,
              'Inventory' AS jcp_analysis_type,
              'Inventory' AS jcp_data_category,
              DATE_TRUNC('day',Inv.crtd_dttm) AS jcp_create_date,
              DATE_TRUNC('day',Inv.crtd_dttm) AS jcp_load_date,
              SUM(inv_qty_pc) AS jcp_qty_sum,
              SUM(inv_value) AS jcp_amt_sum,
              TO_DATE(YEAR || '-' || SUBSTRING(mnth_id, 5, 2) || '-15', 'YYYY-MM-DD') AS transaction_date,
              --CAST(YEAR + '-' + SUBSTRING(mnth_id, 5, 2) + '-' + '15' AS DATE) AS transaction_date,
              'Distributors' AS subject_name,
              cust_cd AS account_cd,
              'AS WATSONS' AS account_nm,
              filename AS file_name,
              'MY' AS country
       FROM ITG_MY_AS_WATSONS_INVENTORY Inv
       WHERE mnth_id >= TO_NUMBER(TO_CHAR(DATEADD(MONTH, -3, CURRENT_DATE), 'YYYYMM'))
       --mnth_id >= CAST(TO_CHAR(ADD_MONTHS(DATE_TRUNC('day',current_timestamp()), - 3), 'YYYYMM') AS INTEGER)
       GROUP BY Inv.crtd_dttm,
              TO_DATE(YEAR || '-' || SUBSTRING(mnth_id, 5, 2) || '-15', 'YYYY-MM-DD'),
              --CAST(YEAR + '-' + SUBSTRING(mnth_id, 5, 2) + '-' + '15' AS DATE),
              cust_cd,
              filename
       ),
itg_ph_as_watsons_inventory_temp1
AS (
       SELECT 'Inventory' AS jcp_data_source,
              'Inventory' AS jcp_analysis_type,
              'Inventory' AS jcp_data_category,
              DATE_TRUNC('day',Inv.crtd_dttm) AS jcp_create_date,
              DATE_TRUNC('day',Inv.crtd_dttm) AS jcp_load_date,
              SUM(total_units) AS jcp_qty_sum,
              SUM(total_cost) AS jcp_amt_sum,
              TO_DATE(YEAR || '-' || mnth_no || '-15', 'YYYY-MM-DD') AS transaction_date,
              --CAST(YEAR + '-' + mnth_no + '-' + '15' AS DATE) AS transaction_date,
              'Distributors' AS subject_name,
              '110680' AS account_cd,
              'AS WATSONS' AS account_nm,
              filename AS file_name,
              'PH' AS country
       FROM ITG_PH_AS_WATSONS_INVENTORY Inv
       WHERE TO_NUMBER(YEAR || mnth_no) >= TO_NUMBER(TO_CHAR(DATEADD(MONTH, -3, CURRENT_DATE), 'YYYYMM'))
       --YEAR + mnth_no >= CAST(TO_CHAR(ADD_MONTHS(DATE_TRUNC('day',current_timestamp()), - 3), 'YYYYMM') AS INTEGER)
       GROUP BY Inv.crtd_dttm,
            TO_DATE(YEAR || '-' || mnth_no || '-15', 'YYYY-MM-DD'),
              --CAST(YEAR + '-' + mnth_no + '-' + '15' AS DATE),
              filename
       ),
edw_kr_otc_inventory_temp1
AS (
       SELECT 'Inventory' AS jcp_data_source,
              'Inventory' AS jcp_analysis_type,
              'Inventory' AS jcp_data_category,
              DATE_TRUNC('day',Inv.crtd_dttm) AS jcp_create_date,
              DATE_TRUNC('day',Inv.crtd_dttm) AS jcp_load_date,
              SUM(inv_qty) AS jcp_qty_sum,
              SUM(inv_amt) AS jcp_amt_sum,
              TO_DATE(SUBSTRING(mnth_id, 1, 4) || '-' || SUBSTRING(mnth_id, 5, 2) || '-15', 'YYYY-MM-DD') AS transaction_date,
              --CAST(SUBSTRING(mnth_id, 0, 5) + '-' + SUBSTRING(mnth_id, 5, 2) + '-' + '15' AS DATE) AS transaction_date,
              'Distributors' AS subject_name,
              distributor_cd AS account_cd,
              NULL AS account_nm,
              filename AS file_name,
              'KR' AS country
       FROM EDW_KR_OTC_INVENTORY Inv
       WHERE TO_NUMBER(mnth_id) >= TO_NUMBER(TO_CHAR(DATEADD(MONTH, -3, CURRENT_DATE), 'YYYYMM'))
       --mnth_id >= CAST(TO_CHAR(ADD_MONTHS(DATE_TRUNC('day',current_timestamp()), - 3), 'YYYYMM') AS INTEGER)
       GROUP BY Inv.crtd_dttm,
              TO_DATE(SUBSTRING(mnth_id, 1, 4) || '-' || SUBSTRING(mnth_id, 5, 2) || '-15', 'YYYY-MM-DD'),
              --CAST(SUBSTRING(mnth_id, 0, 5) + '-' + SUBSTRING(mnth_id, 5, 2) + '-' + '15' AS DATE),
              distributor_cd,
              filename
       ),
final
AS (
       SELECT cal_day,
              jcp_data_source,
              jcp_analysis_type,
              jcp_data_category,
              jcp_create_date,
              jcp_load_date,
              jcp_qty_sum,
              jcp_amt_sum,
              transaction_date,
              subject_name,
              account_cd,
              account_nm,
              file_name,
              country
       FROM (
              SELECT *
              FROM dm_integration_dly_temp1
              
              UNION ALL
              
              SELECT *
              FROM dw_iv_month_end_temp1
              
              UNION ALL
              
              SELECT *
              FROM dm_integration_dly_temp2
              
              UNION ALL
              
              SELECT *
              FROM t_bi_posdata_temp1
              
              UNION ALL
              
              SELECT *
              FROM itg_my_sellout_sales_fact_temp1
              
              UNION ALL
              
              SELECT *
              FROM itg_my_pos_sales_fact_temp1
              
              UNION ALL
              
              SELECT *
              FROM itg_sg_pos_sales_fact_temp1
              
              UNION ALL
              
              SELECT *
              FROM itg_sg_zuellig_sellout_temp1
              
              UNION ALL
              
              SELECT *
              FROM edw_ims_fact_temp1
              
              UNION ALL
              
              SELECT *
              FROM edw_ims_inventory_fact_temp1
              
              UNION ALL
              
              SELECT *
              FROM edw_pos_fact_temp1
              
              UNION ALL
              
              SELECT *
              FROM edw_ecommerce_offtake_temp1
              
              UNION ALL
              
              SELECT *
              FROM edw_ecommerce_offtake_temp2
              
              UNION ALL
              
              SELECT *
              FROM edw_day_cls_stock_fact_temp1
              
              UNION ALL
              
              SELECT *
              FROM edw_copa_trans_fact_temp1
              
              UNION ALL
              
              SELECT *
              FROM edw_ivy_indonesia_noo_analysis_temp1
              
              UNION ALL
              
              SELECT *
              FROM edw_dailysales_fact_temp1
              
              UNION ALL
              
              SELECT *
              FROM itg_ph_dms_sellout_sales_fact_temp1
              
              UNION ALL
              
              SELECT *
              FROM edw_ph_pos_analysis_temp1
              
              UNION ALL
              
              SELECT *
              FROM edw_th_pos_analysis_temp1
              
              UNION ALL
              
              SELECT *
              FROM edw_th_sellout_analysis_temp1
              
              UNION ALL
              
              SELECT *
              FROM edw_metcash_ind_grocery_fact_temp1
              
              UNION ALL
              
              SELECT *
              FROM edw_sales_reporting_temp1
              
              UNION ALL
              
              SELECT *
              FROM edw_sales_reporting_temp2
              
              UNION ALL
              
              SELECT *
              FROM edw_sales_reporting_temp3
              
              UNION ALL
              
              SELECT *
              FROM edw_cube_expense_tp_fact_temp1
              
              UNION ALL
              
              SELECT *
              FROM edw_cube_expense_tt_fact_temp1
              
              UNION ALL
              
              SELECT *
              FROM edw_gt_so_fact_temp1
              
              UNION ALL
              
              SELECT *
              FROM edw_inventory_fact_temp1
              --Added NEW 14th May 2020
              
              UNION ALL
              
              SELECT *
              FROM itg_my_sellout_stock_fact_temp1
              
              UNION ALL
              
              SELECT *
              FROM itg_ph_dms_sellout_stock_fact_temp1
              
              UNION ALL
              
              SELECT *
              FROM itg_vn_dms_d_sellout_sales_fact_temp1
              
              UNION ALL
              
              SELECT *
              FROM itg_vn_dms_sales_stock_fact_temp1
              
              UNION ALL
              
              --via CD SQL Server
              SELECT *
              FROM itg_th_sellout_inventory_fact_temp1
              
              UNION ALL
              
              --via SalesTool
              SELECT *
              FROM itg_th_dms_inventory_fact_temp1
              
              UNION ALL
              
              --AU Pharmacy & IG & Grocery 
              SELECT *
              FROM vw_pacific_inventory_temp1
              
              UNION ALL
              
              --AU Pharmacy
              SELECT *
              FROM itg_customer_sellout_temp1
              
              UNION ALL
              
              --AU IG Sellout
              SELECT *
              FROM vw_pacific_sellout_temp1
              
              UNION ALL
              
              --AU IRI Scan
              SELECT *
              FROM vw_iri_scan_sales_analysis_temp1
              
              UNION ALL
              
              --KR Ecomm Inv 
              SELECT *
              FROM itg_kr_ecom_dstr_inventory_temp1
              
              UNION ALL
              
              --KR Ecomm sell out        
              SELECT *
              FROM itg_kr_ecom_dstr_sellout_temp1
              
              UNION ALL
              
              --India offtake 
              SELECT *
              FROM itg_pos_offtake_fact_temp1
              
              UNION ALL
              
              --VN MT
              SELECT *
              FROM itg_vn_mt_sellin_dksh_temp1
              
              UNION ALL
              
              SELECT *
              FROM itg_vn_mt_sellin_target_temp1
              
              UNION ALL
              
              SELECT *
              FROM itg_vn_mt_sellout_con_cung_temp1
              
              UNION ALL
              
              SELECT *
              FROM itg_vn_mt_sellout_coop_temp1
              
              UNION ALL
              
              SELECT *
              FROM itg_vn_mt_sellout_guardian_temp1
              
              UNION ALL
              
              SELECT *
              FROM itg_vn_mt_sellout_lotte_temp1
              
              UNION ALL
              
              SELECT *
              FROM itg_vn_mt_sellout_mega_temp1
              
              UNION ALL
              
              SELECT *
              FROM itg_vn_mt_sellout_vinmart_temp1
              
              UNION ALL
              
              SELECT *
              FROM itg_tw_as_watsons_inventory_temp1
              
              UNION ALL
              
              SELECT *
              FROM itg_my_as_watsons_inventory_temp1
              
              UNION ALL
              
              SELECT *
              FROM itg_ph_as_watsons_inventory_temp1
              
              UNION ALL
              
              SELECT *
              FROM edw_kr_otc_inventory_temp1
              ) a
       JOIN edw_calendar_dim b ON b.cal_day = a.transaction_date
       )
SELECT * FROM final