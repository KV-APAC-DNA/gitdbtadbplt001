with sdl_mds_th_myanmar_customer_master as (
  select * from {{ source('thasdl_raw', 'sdl_mds_th_myanmar_customer_master') }}
  ),
  final as (
  select
    "customer code"::varchar(100) as customer_code,
    "customer name"::varchar(200) as customer_name,
    "customer re"::varchar(200) as customer_re,
    "distributor name"::varchar(100) as distributor_name,
    "sales group"::varchar(200) as sales_group,
    "sales office"::varchar(200) as sales_office,
    type::varchar(200) as type,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm,
  from sdl_mds_th_myanmar_customer_master
  )
select * from final
  