with v_ecomm_sales_inv_acc_ch_grp as(
    select * from DEV_DNA_CORE.INDedw_INTEGRATION.v_ecomm_sales_inv_acc_ch_grp
),
wks_mt_paramet_accounts as(
    select * from DEV_DNA_CORE.sm05_workspace.INDWKS_INTEGRATION__wks_mt_paramet_accounts
),
itg_query_parameters as(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),

union1 as(
    SELECT ds.data_source
      ,ds.account_name
      ,ds.fisc_yr
      ,ds.mth_mm
      ,ds.franchise_name
      ,ds.brand_name
      ,ds.variant_name
      ,ds.product_category_name
      ,ds.mothersku_name
      ,ds.channel_name_inv AS channel_name
      ,mds.account_name_as_per_offtake_data_code AS account_name_offtake
      ,SUM(ds.invoice_quantity) AS invoice_quantity
      ,SUM(ds.invoice_value) AS invoice_value
    FROM v_ecomm_sales_inv_acc_ch_grp ds
    INNER JOIN (SELECT account_name
                    ,account_name_as_per_offtake_data_code
                FROM wks_mt_paramet_accounts
                GROUP BY 1,2) mds
            ON UPPER(ds.account_name) = UPPER(mds.account_name)
    WHERE data_source = 'INVOICE_CUBE'
    AND channel_name_inv IN ('KEY ACCOUNT','E-COMMERCE')
    AND fisc_yr >= EXTRACT(YEAR FROM current_timestamp()::timestamp_ntz(9)) - 2      -- to be parameterized
    GROUP BY ds.data_source
        ,ds.account_name
        ,ds.fisc_yr
        ,ds.mth_mm
        ,ds.franchise_name
        ,ds.brand_name
        ,ds.variant_name
        ,ds.product_category_name
        ,ds.mothersku_name
        ,ds.channel_name_inv
        ,mds.account_name_as_per_offtake_data_code
),
union2 as(
    SELECT ids.data_source
      ,ids.udc_keyaccountname AS account_name
      ,ids.fisc_yr
      ,ids.mth_mm
      ,ids.franchise_name
      ,ids.brand_name
      ,ids.variant_name
      ,ids.product_category_name
      ,ids.mothersku_name
      ,CASE WHEN ids.channel_name_sales = 'NATIONAL KEY ACCOUNTS' THEN 'KEY ACCOUNT' ELSE ids.channel_name_sales END AS channel_name
      ,mds.account_name_as_per_offtake_data_code AS account_name_offtake
      ,SUM(ids.invoice_quantity) AS invoice_quantity
      ,SUM(ids.invoice_value) AS invoice_value
    FROM v_ecomm_sales_inv_acc_ch_grp ids
    LEFT JOIN (SELECT parameter_name AS offtake_account_name,
                    parameter_value AS udc_account_name
            FROM itg_query_parameters
            WHERE country_code = 'IN'
                AND parameter_type = 'IN_MT_UDC_OFFTAKE_ACCOUNT_SYNC'
            GROUP BY 1,2) par 
        ON UPPER(ids.udc_keyaccountname) = UPPER(par.udc_account_name)
    INNER JOIN (SELECT account_name
                    ,account_name_as_per_offtake_data_code
                FROM wks_mt_paramet_accounts
                GROUP BY 1,2) mds
            ON UPPER(par.offtake_account_name) = UPPER(mds.account_name_as_per_offtake_data_code)              -- need right mapping for INDIRECT Sales udc_account_names
    WHERE ids.data_source = 'SALES_CUBE'
    AND ids.channel_name_sales IN ('NATIONAL KEY ACCOUNTS','E-COMMERCE')
    AND ids.fisc_yr >= EXTRACT(YEAR FROM current_timestamp()::timestamp_ntz(9)) - 2      -- to be parameterized
    GROUP BY ids.data_source
        ,ids.udc_keyaccountname
        ,ids.fisc_yr
        ,ids.mth_mm
        ,ids.franchise_name
        ,ids.brand_name
        ,ids.variant_name
        ,ids.product_category_name
        ,ids.mothersku_name
        ,CASE WHEN channel_name_sales = 'NATIONAL KEY ACCOUNTS' THEN 'KEY ACCOUNT' ELSE channel_name_sales END
        ,mds.account_name_as_per_offtake_data_code
),
transformed as(
    select * from union1
    union all
    select * from union2
)
select * from transformed