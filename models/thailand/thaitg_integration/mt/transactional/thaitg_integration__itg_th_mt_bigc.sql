with th_wks_bigc as 
(select * from {{ ref('thawks_integration__th_wks_bigc') }}
),
sdl_mds_th_product_master as (
select * from {{ source('thasdl_raw', 'sdl_mds_th_product_master') }}
),
edw_list_price as (
  select * from {{ ref('aspedw_integration__edw_list_price') }}
),

bigc as (
  select
    store_format,
    store,
    trans_date,
    barcode,
    sellout_quantity as sls_qty_raw,
    (
      sellout_quantity * coalesce(retailer_unit_conversion, 1)
    ) as sellout_quantity,
    inventory_quantity as inv_qty_raw,
    foc_product,
    sales_baht,
    stock_baht,
    (
      inventory_quantity * coalesce(retailer_unit_conversion, 1)
    ) as inventory_quantity,
    retailer_unit_conversion,
    b.matl_num
  from (
    select
      store_format,
      store,
      to_date(b.trans_date, 'DD-MON-YYYY') as trans_date,
      barcode,
      sellout_quantity,
      inventory_quantity,
      foc_product,
      sales_baht,
      stock_baht
    from (
      select
        business_format as store_format,
        store,
        transaction_date,
        barcode,
        sale_qty_ty as sellout_quantity,
        stock_qty_ty as inventory_quantity,
        case
          when (
            sale_amt_ty_baht = 0
          ) and (
            stock_ty_baht = 0
          )
          then 'Y'
          else 'N'
        end as foc_product,
        sale_amt_ty_baht as sales_baht,
        stock_ty_baht as stock_baht
      from th_wks_bigc
      where
        transaction_date <> ''
    ) as a, (
      select
        transaction_date,
        len,
        case
          when len = 9
          then substring(transaction_date, 1, 7) || concat(
            coalesce(cast(20 as text), ''),
            coalesce(cast(substring(transaction_date, 8, 2) as text), '')
          )
          when len = 8
          then substring(lpad(transaction_date, 9, 0), 1, 7) || concat(
            coalesce(cast(20 as text), ''),
            coalesce(cast(substring(transaction_date, 7, 2) as text), '')
          )
          else transaction_date
        end as trans_date
      from (
        select distinct
          transaction_date,
          file_name,
          length(transaction_date) as len
        from th_wks_bigc
      )
    ) as b
    where
      a.transaction_date = b.transaction_date
  ) as a
  left join (
    select distinct
      code as matl_num,
      barcode as barcd,
      retailer_unit_conversion,
      createdate
    from (
      select
        barcode,
        code,
        retailer_unit_conversion,
        createdate,
        row_number() over (partition by barcode order by createdate desc nulls last, code) as rnk
      from sdl_mds_th_product_master
      where
        barcode <> ''
    )
    where
      rnk = 1
  ) as b
    on ltrim(a.barcode, 0) = b.barcd
),

final as (
select
  '108830'::varchar(6) as sold_to_code,
  store_format::varchar(500) as store_format,
  store::varchar(500) as store,
  trans_date::date as trans_date,
  substring(to_char(cast(trans_date as timestampntz), 'yyyymmDD'), 1, 6)::varchar(16) AS month,
  barcode::varchar(500) as barcode,
  matl_num::varchar(1500) as matl_num,
  sls_qty_raw::number(16,4) as sls_qty_raw,
  sellout_quantity:: number(38,4) as sellout_quantity,
  inv_qty_raw::number(16,4) as inv_qty_raw,
  inventory_quantity::number(38,4) as inventory_quantity,
  retailer_unit_conversion::number(31,0) as retailer_unit_conversion,
  list_price,
  (
    sellout_quantity * list_price
  ) AS Sellout_value,
  (
    inventory_quantity * list_price
  )::number(20,4) AS inventory_value,
  foc_product::varchar(1) as foc_product,
  sales_baht::number(16,4) as sales_baht,
  stock_baht::number(16,4) as stock_baht
from bigc
left join (
  select
    *
  from (
    select
      ltrim(cast(edw_list_price.material as text), cast(cast(0 as varchar) as text)) as material,
      edw_list_price.amount as list_price,
      row_number() over (partition by ltrim(cast(edw_list_price.material as text), cast(cast(0 as varchar) as text)) order by to_date(cast(edw_list_price.valid_to AS TEXT), cast(cast('YYYYMMDD' AS VARCHAR) AS TEXT)) DESC, TO_DATE(cast(edw_list_price.dt_from AS TEXT), cast(cast('YYYYMMDD' AS VARCHAR) AS TEXT)) DESC) AS rn
    from edw_list_price
    where
      cast(edw_list_price.sls_org as text) in ('2400')
  )
  where
    rn = 1
) as b
  on bigc.matl_num = b.material
)

select * from final
