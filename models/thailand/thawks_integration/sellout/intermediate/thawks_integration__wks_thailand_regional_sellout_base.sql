with edw_vw_os_time_dim as (
select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
itg_mds_th_mt_branch_master as (
select * from {{ ref('thaitg_integration__itg_mds_th_mt_branch_master') }}
),
itg_mds_th_customer_hierarchy as (
select * from {{ ref('thaitg_integration__itg_mds_th_customer_hierarchy') }}
),
itg_th_pos_customer_dim as (
select * from {{ ref('thaitg_integration__itg_th_pos_customer_dim') }}
),
itg_th_sellout_sales_fact as (
select * from {{ ref('thaitg_integration__itg_th_sellout_sales_fact') }}
),
itg_th_pos_sales_inventory_fact as (
select * from {{ ref('thaitg_integration__itg_th_pos_sales_inventory_fact') }}
),
itg_th_dtsdistributor as (
select * from {{ ref('thaitg_integration__itg_th_dtsdistributor') }}
),
edw_vw_os_customer_dim as (
select * from {{ ref('thaedw_integration__edw_vw_th_customer_dim') }} where sap_cntry_cd ='TH'
),
itg_th_dstrbtr_material_dim as (
select * from {{ ref('thaitg_integration__itg_th_dstrbtr_material_dim') }}
),
itg_th_dstrbtr_customer_dim as (
select * from {{ ref('thaitg_integration__itg_th_dstrbtr_customer_dim') }}
),
itg_mds_ap_customer360_config as (
select * from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }}
),
union_1 as (
select 
      'TH' as cntry_cd, 
      'Thailand' as cntry_nm, 
      case when upper(customer) in ('TOPS', '7-ELEVEN') then 'STOCK TRANSFER' else 'POS' end as data_src, 
      sap_prnt_cust_key as distributor_code, 
      sap_prnt_cust_desc as distributor_name, 
      fact.branch_code as store_code, 
      fact.branch_name as store_name, 
      branch.branch_type as store_type_code, 
      sap_prnt_cust_key, 
      sold_to_code, 
      material_number as sku_cd, 
      bar_code as ean, 
      substring(trans_dt, 1, 4) as year, 
      b.mnth_id, 
      trans_dt as day, 
      material_name as customer_product_desc, 
      'NA' as region, 
      'NA' as zone_or_area, 
      'NA' as distributor_additional_attribute1, 
      sum(sales_qty_converted) as so_sls_qty, 
      sum(sales_baht) as so_sls_value, 
      bar_code as msl_product_code, 
      --'na' as msl_product_desc,
      cust_hier.re as retail_env, 
      cust_hier.channel 
    from 
      itg_th_pos_sales_inventory_fact fact 
      left join edw_vw_os_time_dim b on fact.trans_dt = b.cal_date 
      left join itg_mds_th_mt_branch_master branch on fact.branch_code = branch.branch_code 
      left join itg_mds_th_customer_hierarchy cust_hier on fact.sold_to_code = cust_hier.sold_to 
    where 
      (
        nvl(fact.foc_product, 'NA') = 'N' 
        or fact.foc_product is null
      ) 
      and not (
        concat(customer, fact.branch_code) in (
          select 
            distinct concat(cus.cust_cd, cus.brnch_no) as concat 
          from 
            itg_th_pos_customer_dim cus 
          where 
            upper(cus.brnch_typ) = 'DISTRIBUTION CENTER' 
            and cus.cust_cd = 'Lotus'
        )
      ) 
    group by 
      customer, 
      sap_prnt_cust_key, 
      sap_prnt_cust_desc, 
      fact.branch_code, 
      fact.branch_name, 
      branch.branch_type, 
      sap_prnt_cust_key, 
      sold_to_code, 
      material_number, 
      bar_code, 
      trans_dt, 
      material_name, 
      --branch.province,
      b.mnth_id, 
      trans_dt, 
      cust_hier.re, 
      cust_hier.channel 

),
union_2 as (
select 
      'TH' as cntry_cd, 
      'Thailand' as cntry_nm, 
      'SELL-OUT' as data_src, 
      sellout.dstrbtr_id as distributor_code, 
      --distributor.dstrbtr_nm as distributor_name,
      distributor.dist_nm as distributor_name, 
      sellout.ar_cd as store_code, 
      sellout.ar_nm as store_name, 
      sellout.ar_typ_cd as store_type_code, 
      customer.sap_prnt_cust_key as sap_prnt_cust_key, 
      distributor.dstrbtr_cd as sold_to_code, 
      sellout.prod_cd as sku_cd, 
      null as ean, 
      substring(order_dt, 1, 4) as year, 
      b.mnth_id, 
      order_dt as day, 
      prod_desc as customer_product_desc, 
      --'na'as region,
      --'na' as zone_or_area,
      cust_distributor.region_desc as region, 
      cust_distributor.sls_dist_city_eng as zone_or_area, 
      'NA' as distributor_additional_attribute1, 
      sum(sellout.qty) as so_sls_qty, 
      sum(
        case when (
          upper(
            substring(sellout.cn_reason_cd, 1, 1)
          ) = 'D' 
          or sellout.cn_reason_cd is null 
          or sellout.cn_reason_cd = ''
        ) then sellout.total_bfr_vat else 0 end
      ) as so_sls_value, 
      mat_dim.bar_cd as msl_product_code, 
      --'na' as msl_product_desc,
      cust_distributor.typ_grp_nm as retail_env, 
      'GT' as channel 
    from 
      itg_th_sellout_sales_fact sellout 
      left join edw_vw_os_time_dim b on sellout.order_dt = b.cal_date 
      left join itg_th_dtsdistributor distributor on sellout.dstrbtr_id = distributor.dstrbtr_id --left join itg_th_dstrbtr_customer_dim distributor on sellout.dstrbtr_id=distributor.dstrbtr_id
      left join edw_vw_os_customer_dim customer on distributor.dstrbtr_cd = customer.sap_cust_id 
      left join itg_th_dstrbtr_material_dim mat_dim on sellout.prod_cd = mat_dim.item_cd 
      left join itg_th_dstrbtr_customer_dim cust_distributor on sellout.dstrbtr_id = cust_distributor.dstrbtr_id 
      and sellout.ar_cd = cust_distributor.ar_cd 
      and cust_distributor.actv_status = '1' 
      and lower(
        cust_distributor.dstrbtr_status
      ) = 'active' 
    group by 
      sellout.dstrbtr_id, 
      distributor.dist_nm, 
      sellout.ar_cd, 
      sellout.ar_nm, 
      sellout.ar_typ_cd, 
      customer.sap_prnt_cust_key, 
      distributor.dstrbtr_cd, 
      sellout.prod_cd, 
      substring(order_dt, 1, 4), 
      b.mnth_id, 
      order_dt, 
      prod_desc, 
      mat_dim.bar_cd, 
      cust_distributor.typ_grp_nm, 
      cust_distributor.region_desc, 
      cust_distributor.sls_dist_city_eng
),
transformed as (
select 
  base.cntry_cd, 
  base.cntry_nm, 
  base.data_src, 
  base.distributor_code, 
  base.distributor_name, 
  base.store_code, 
  base.store_name, 
  base.store_type_code, 
  base.sap_prnt_cust_key, 
  base.sold_to_code, 
  base.sku_cd, 
  base.ean, 
  base.year, 
  base.mnth_id, 
  base.day, 
  base.customer_product_desc, 
  base.region, 
  base.zone_or_area, 
  base.distributor_additional_attribute1, 
  base.so_sls_qty, 
  base.so_sls_value, 
  base.msl_product_code --,base.msl_product_desc,
  --base.store_grade,
  , 
  upper(base.retail_env) as retail_env, 
  base.channel, 
  current_timestamp() as crtd_dttm, 
  current_timestamp() as updt_dttm 
from 
  (
    --source selection
    select * from
    union_1
    union all 
    select * from 
    union_2
  ) base 
where 
  not (
    nvl(base.so_sls_value, 0) = 0 
    and nvl(base.so_sls_qty, 0) = 0
  ) 
  and base.day > (
    select 
      to_date(param_value, 'YYYY-MM-DD') 
    from 
      itg_mds_ap_customer360_config 
    where 
      code = 'min_date'
  ) 
  and base.mnth_id >= (
    case when (
      select 
        param_value 
      from 
        itg_mds_ap_customer360_config 
      where 
        code = 'base_load_th'
    )= 'ALL' then '190001' else to_char(
      add_months(
        current_date, 
        -(
          (
            select 
              param_value 
            from 
              itg_mds_ap_customer360_config 
            where 
              code = 'base_load_th'
          ):: integer
        )
      ), 
      'YYYYMM'
    ) end
  )
),
final as (
select
    cntry_cd::varchar(2) as cntry_cd,
    cntry_nm::varchar(8) as cntry_nm,
    data_src::varchar(14) as data_src,
    distributor_code::varchar(12) as distributor_code,
    distributor_name::varchar(100) as distributor_name,
    store_code::varchar(20) as store_code,
    store_name::varchar(500) as store_name,
    store_type_code::varchar(50) as store_type_code,
    sap_prnt_cust_key::varchar(12) as sap_prnt_cust_key,
    sold_to_code::varchar(255) as sold_to_code,
    sku_cd::varchar(25) as sku_cd,
    ean::varchar(20) as ean,
    year::varchar(16) as year,
    mnth_id::varchar(23) as mnth_id,
    day::timestamp_ntz(9) as day,
    customer_product_desc::varchar(500) as customer_product_desc,
    region::varchar(100) as region,
    zone_or_area::varchar(100) as zone_or_area,
    distributor_additional_attribute1::varchar(2) as distributor_additional_attribute1,
    so_sls_qty::number(38,6) as so_sls_qty,
    so_sls_value::number(38,6) as so_sls_value,
    msl_product_code::varchar(20) as msl_product_code,
    retail_env::varchar(300) as retail_env,
    channel::varchar(200) as channel,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final