with wks_th_makro_test as (
  select * from DEV_DNA_CORE.SNAPOSEWKS_INTEGRATION.WKS_TH_MAKRO
),
edw_vw_os_time_dim as (
  select * from DEV_DNA_CORE.SNENAV01_WORKSPACE.EDW_VW_OS_TIME_DIM
),
makro_sellout as (
  select
    transaction_date as month,
    'day_1' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_1 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_2' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_2 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_3' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_3 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_4' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_4 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_5' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_5 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_6' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_6 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_7' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_7 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_8' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_8 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_9' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_9 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_10' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_10 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_11' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_11 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_12' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_12 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_13' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_13 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_14' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_14 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_15' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_15 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_16' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_16 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_17' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_17 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_18' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_18 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_19' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_19 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_20' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_20 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_21' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_21 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_22' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_22 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_23' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_23 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_24' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_24 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_25' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_25 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_26' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_26 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_27' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_27 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_28' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_28 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_29' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_29 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_30' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_30 as sellout_qty
  from wks_th_makro_test
  union all
  select
    transaction_date as month,
    'day_31' as day,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    day_31 as sellout_qty
  from wks_th_makro_test
), time_dim as (
  select
    cal_date,
    cal_mnth_id,
    CONCAT(COALESCE(cast('day_' as TEXT), ''), COALESCE(cast(day as TEXT), '')) as day_mnth
  from (
    select
      cal_date,
      cal_mnth_id,
      ROW_NUMBER() OVER (PARTITION BY cal_year, cal_mnth_id ORDER BY cal_date asC) as day
    from edw_vw_os_time_dim
  )
), makro_inv as (
  select
    transaction_date as month,
    location_number,
    location_name,
    barcode,
    item_number,
    item_desc,
    SUM(COALESCE(eoh_qty, 0) + COALESCE(order_in_transit_qty, 0)) as inventory_qty
  from wks_th_makro_test
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6
)
select
  cal_date as trans_date,
  cal_mnth_id,
  location_number,
  location_name,
  barcode,
  item_number,
  item_desc,
  0 as inventory_qty,
  sellout_qty
from time_dim
INNER JOIN makro_sellout
  ON cast(month as INT) = cal_mnth_id AND UPPER(day_mnth) = UPPER(day)
union all
select
  time_dim.trans_dt,
  cal_mnth_id,
  location_number,
  location_name,
  barcode,
  item_number,
  item_desc,
  inventory_qty,
  0 as sellout_qty
from makro_inv
LEFT JOIN (
  select
    cal_mnth_id,
    MAX(cal_date) as trans_dt
  from time_dim
  GROUP BY
    cal_mnth_id
) as time_dim
  ON cal_mnth_id = cast(month as INT)
