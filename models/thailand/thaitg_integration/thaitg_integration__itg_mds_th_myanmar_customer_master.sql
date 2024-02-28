with sdl_mds_th_myanmar_customer_master as (
--   select * from dev_dna_load.snaposesdl_raw.sdl_mds_th_myanmar_customer_master
  select * from {{ source('thasdl_raw', 'sdl_mds_th_myanmar_customer_master') }}
  ),
  transformed as (
  select
    "customer code" as customer_code,
    "customer name" as customer_name,
    "customer re" as customer_re,
    "distributor name" as distributor_name,
    "sales group" as sales_group,
    "sales office" as sales_office,
    type as type,
    convert_timezone('Asia/Singapore', current_timestamp()) as crt_dttm,
    current_timestamp() as updt_dttm,
  from sdl_mds_th_myanmar_customer_master
  ),
final as (
    select
    customer_code::varchar(100) as customer_code,
    customer_name::varchar(200) as customer_name,
    customer_re::varchar(200) as customer_re,
    distributor_name::varchar(100) as distributor_name,
    sales_group::varchar(200) as sales_group,
    sales_office::varchar(200) as sales_office,
    type::varchar(200) as type,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
    from transformed
)
  select * from final
  