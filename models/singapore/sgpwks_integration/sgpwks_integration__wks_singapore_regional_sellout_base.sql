with source as (
    select * from {{ source('SNAPOSEITG_INTEGRATION', 'itg_sg_pos_sales_fact') }}
),
itg_mds_ap_customer360_config as (
    -- select * from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }}
    select * from DEV_DNA_CORE.APAHIL01_WORKSPACE.ITG_MDS_AP_CUSTOMER360_CONFIG
),
final as (
select
  base.data_src::varchar(3) as data_src,
  base.cntry_cd::varchar(2) as cntry_cd,
  base.cntry_nm::varchar(9) as cntry_nm,
  base.year::numeric(18,0) as year,
  base.mnth_id::numeric(18,0) as mnth_id,
  base.week_id::numeric(18,0) as week_id,
  base.day::date as day,
  base.soldto_code::varchar(200) as soldto_code,
  base.distributor_code::varchar(200) as distributor_code,
  base.distributor_name::varchar(10) as distributor_name,
  base.store_cd::varchar(300) as store_cd,
  base.store_name::varchar(300) as store_name,
  base.store_type::varchar(200) as store_type,
  base.ean::varchar(255) as ean,
  base.matl_num::varchar(255) as matl_num,
  base.pka_product_key::varchar(300) as pka_product_key,
  base.pka_product_key_description::varchar(500) as pka_product_key_description,
  base.so_sls_qty::numeric(9) as so_sls_qty,
  base.so_sls_value::numeric(9) as so_sls_value
from (
  /* pos */
  select
    'POS' as data_src,
    'SG' as cntry_cd,
    'Singapore' as cntry_nm,
    cast(year as int) as year,
    cast(mnth_id as int) as mnth_id,
    cast(week as int) as week_id,
    bill_date /* case when nvl(cast(right(week,2) as varchar(2)),'na')<>'na' */ /* then trunc(to_date(right(week,2)::integer||' '||year::integer,'ww yyyy')) */ /* -(date_part(dow,trunc(to_date(right(week,2)::integer||' --'||year::integer,'ww yyyy'))))::integer+1 */ /* else to_date(mnth_id|| '01','yyyymmdd') end */ as day,
    sold_to_code as soldto_code,
    sold_to_code as distributor_code,
    cust_id as distributor_name,
    store as store_cd,
    store_name as store_name,
    'NA' as store_type,
    product_barcode as ean,
    sap_code as matl_num,
    product_key as pka_product_key,
    product_key_desc as pka_product_key_description,
    sales_qty as so_sls_qty,
    net_sales as so_sls_value
  from source
) as base
where
  not (
    coalesce(base.so_sls_value, 0) = 0 and coalesce(base.so_sls_qty, 0) = 0
  )
  and base.day > (
    select
      to_date(param_value, 'YYYY-MM-DD')
    from itg_mds_ap_customer360_config
    where
      code = 'min_date'
  )
  and base.mnth_id >= (
    case
      when (
        select
          param_value
        from itg_mds_ap_customer360_config
        where
          code = 'base_load_sg'
      ) = 'ALL'
      then '190001'
      else to_char(
        dateadd(
          month,
          -(
            (
              SELECT
                param_value
              FROM itg_mds_ap_customer360_config
              WHERE
                code = 'base_load_sg'
            )::INT
          ),
          TO_DATE(CURRENT_DATE::varchar, 'YYYY-MM-DD')::timestamp_ntz(9)
        )::timestamp_ntz(9),
        'yyyymm'
      )
    END
  ))

select * from final