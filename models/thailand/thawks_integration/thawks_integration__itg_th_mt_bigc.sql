
WITH th_wks_bigc as 
(SELECT * from DEV_DNA_CORE.SNAPOSEWKS_INTEGRATION.TH_WKS_BIGC
),
sdl_mds_th_product_master as (
--   select * from DEV_DNA_LOAD.THASDL_RAW.SDL_MDS_TH_PRODUCT_MASTER
select * from {{ source('thasdl_raw', 'sdl_mds_th_product_master') }}
),
edw_list_price as (
--   select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_LIST_PRICE
  select * from {{ ref('aspedw_integration__edw_list_price') }}
),

bigc AS (
  SELECT
    store_format,
    store,
    trans_date,
    barcode,
    sellout_quantity AS sls_qty_raw,
    (
      sellout_quantity * COALESCE(retailer_unit_conversion, 1)
    ) AS sellout_quantity,
    inventory_quantity AS inv_qty_raw,
    foc_product,
    sales_baht,
    stock_baht,
    (
      inventory_quantity * COALESCE(retailer_unit_conversion, 1)
    ) AS inventory_quantity,
    retailer_unit_conversion,
    b.matl_num
  FROM (
    SELECT
      store_format,
      store,
      TO_DATE(b.trans_date, 'DD-MON-YYYY') AS trans_date,
      barcode,
      sellout_quantity,
      inventory_quantity,
      foc_product,
      sales_baht,
      stock_baht
    FROM (
      SELECT
        business_format AS store_format,
        store,
        transaction_date,
        barcode,
        sale_qty_ty AS Sellout_quantity,
        stock_qty_ty AS Inventory_quantity,
        CASE
          WHEN (
            sale_amt_ty_baht = 0
          ) AND (
            stock_ty_baht = 0
          )
          THEN 'Y'
          ELSE 'N'
        END AS foc_product,
        sale_amt_ty_baht AS sales_baht,
        stock_ty_baht AS stock_baht
      FROM th_wks_bigc
      WHERE
        transaction_date <> ''
    ) AS a, (
      SELECT
        transaction_date,
        len,
        CASE
          WHEN len = 9
          THEN SUBSTRING(transaction_date, 1, 7) || CONCAT(
            COALESCE(CAST(20 AS TEXT), ''),
            COALESCE(CAST(SUBSTRING(transaction_date, 8, 2) AS TEXT), '')
          )
          WHEN len = 8
          THEN SUBSTRING(LPAD(transaction_date, 9, 0), 1, 7) || CONCAT(
            COALESCE(CAST(20 AS TEXT), ''),
            COALESCE(CAST(SUBSTRING(transaction_date, 7, 2) AS TEXT), '')
          )
          ELSE transaction_date
        END AS trans_date
      FROM (
        SELECT DISTINCT
          transaction_date,
          file_name,
          LENGTH(transaction_Date) AS len
        FROM th_wks_bigc
      )
    ) AS b
    WHERE
      a.transaction_date = b.transaction_date
  ) AS a
  LEFT JOIN (
    SELECT DISTINCT
      code AS matl_num,
      barcode AS barcd,
      retailer_unit_conversion,
      createdate
    FROM (
      SELECT
        barcode,
        code,
        retailer_unit_conversion,
        createdate,
        ROW_NUMBER() OVER (PARTITION BY barcode ORDER BY createdate DESC NULLS LAST, code) AS rnk
      FROM sdl_mds_th_product_master
      WHERE
        barcode <> ''
    )
    WHERE
      rnk = 1
  ) AS b
    ON LTRIM(a.barcode, 0) = b.barcd
)
SELECT
  '108830' AS Sold_to_code,
  store_format,
  store,
  trans_date,
  SUBSTRING(TO_CHAR(CAST(trans_date AS TIMESTAMPNTZ), 'yyyymmDD'), 1, 6) AS month,
  barcode,
  matl_num,
  sls_qty_raw,
  sellout_quantity,
  inv_qty_raw,
  inventory_quantity,
  retailer_unit_conversion,
  list_price,
  (
    sellout_quantity * list_price
  ) AS Sellout_value,
  (
    inventory_quantity * list_price
  ) AS inventory_value,
  foc_product,
  sales_baht,
  stock_baht
FROM bigc
LEFT JOIN (
  SELECT
    *
  FROM (
    SELECT
      LTRIM(CAST(edw_list_price.material AS TEXT), CAST(CAST(0 AS VARCHAR) AS TEXT)) AS material,
      edw_list_price.amount AS list_price,
      ROW_NUMBER() OVER (PARTITION BY LTRIM(CAST(edw_list_price.material AS TEXT), CAST(CAST(0 AS VARCHAR) AS TEXT)) ORDER BY TO_DATE(CAST(edw_list_price.valid_to AS TEXT), CAST(CAST('YYYYMMDD' AS VARCHAR) AS TEXT)) DESC, TO_DATE(CAST(edw_list_price.dt_from AS TEXT), CAST(CAST('YYYYMMDD' AS VARCHAR) AS TEXT)) DESC) AS rn
    FROM edw_list_price
    WHERE
      CAST(edw_list_price.sls_org AS TEXT) IN ('2400')
  )
  WHERE
    rn = 1
) AS b
  ON bigc.matl_num = b.material
