with 
edw_material_dim as (
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_product_key_attributes as (
    select * from {{ ref('aspedw_integration__edw_product_key_attributes') }}
),
itg_query_parameters as (
    select * from {{ source('aspitg_integration','itg_query_parameters') }}
),

itg_sg_scan_data_watsons as (
    select * from {{ ref('sgpitg_integration__itg_sg_scan_data_watsons') }}
),
itg_sg_scan_data_dfi as (
    select * from {{ ref('sgpitg_integration__itg_sg_scan_data_dfi') }}
),
itg_sg_scan_data_guardian as (
    select * from {{ ref('sgpitg_integration__itg_sg_scan_data_guardian') }}
),
itg_sg_scan_data_marketplace as (
    select * from {{ ref('sgpitg_integration__itg_sg_scan_data_marketplace') }}
),
itg_sg_scan_data_scommerce as (
    select * from {{ ref('sgpitg_integration__itg_sg_scan_data_scommerce') }}
),
itg_sg_scan_data_redmart as (
    select * from {{ ref('sgpitg_integration__itg_sg_scan_data_redmart') }}
),
itg_sg_scan_data_ntuc as (
    select * from {{ ref('sgpitg_integration__itg_sg_scan_data_ntuc') }}
),
itg_sg_scan_data_amazon as (
    select * from {{ ref('sgpitg_integration__itg_sg_scan_data_amazon') }}
),
itg_mds_sg_product_mapping as (
    select * from {{ ref('sgpitg_integration__itg_mds_sg_product_mapping') }}
),
itg_mds_sg_store_master as (
    select * from {{ ref('sgpitg_integration__itg_mds_sg_store_master') }}
),
prdt_map as ( 
  select distinct
    master_code,
    customer_product_code,
    material_code,
    customer_name
  from itg_mds_sg_product_mapping
),
mtrl_dim as (    
  select
    ltrim(matl_num, '0') as sku,
    pka_product_key as prod_key,
    pka_product_key_description as prod_key_desc
  from edw_material_dim
  where
    not prod_key_desc is null
),
a as (
select
      ctry_nm,
      ltrim(ean_upc, '0') as ean_upc,
      ltrim(matl_num, '0') as sku,
      lst_nts as nts_date,
      pka_productkey as prod_key,
      pka_productdesc as prod_key_desc
    from edw_product_key_attributes
    where
      (
        matl_type_cd = 'FERT' OR matl_type_cd = 'HALB' OR matl_type_cd = 'SAPR'
      )
      and not lst_nts is null
      ),
b as (
    select
      ctry_nm,
      ltrim(ean_upc, '0') as ean_upc,
      ltrim(matl_num, '0') as sku,
      lst_nts as latest_nts_date,
      row_number() over (partition by ctry_nm, ean_upc order by lst_nts desc) as row_number
    from edw_product_key_attributes
    where
      (
        matl_type_cd = 'FERT' OR matl_type_cd = 'HALB' OR matl_type_cd = 'SAPR'
      )
      and not lst_nts is null
),
map_sku as (     
  select
    a.ctry_nm,
    a.ean_upc,
    a.sku,
    a.prod_key,
    a.prod_key_desc
  from a
  join b
    on a.ctry_nm = b.ctry_nm
    and a.ean_upc = b.ean_upc
    and a.sku = b.sku
    and b.latest_nts_date = a.nts_date
    and b.row_number = 1
    and upper(a.ctry_nm) = 'SINGAPORE'
),


wat_sg as (select
    'WAT' as cust_id,
    year,
    mnth_id,
    week,
    prdt_map.master_code as master_code,
    coalesce(ltrim(prdt_map.material_code, 0), mtrl_dim.sku, map_sku.sku) as sap_code,
    barcode as product_barcode,
    brand,
    store,
    prdtcode as item_code,
    prdtdesc as item_desc,
    sales_qty,
    net_sales,
    bill_date,
    str_mstr.customer_store_name as store_name,
    qry_param.parameter_value as sold_to_code,
    coalesce(mtrl_dim.prod_key, map_sku.prod_key) as product_key,
    coalesce(mtrl_dim.prod_key_desc, map_sku.prod_key_desc) as product_key_desc,
    file_name as source,
    null as store_type,
    cust_name
  from itg_sg_scan_data_watsons as sg_wat
  left join (
    select distinct
      customer_store_name,
      customer_store_code,
      customer_name
    from itg_mds_sg_store_master
  ) as str_mstr
    on sg_wat.store = str_mstr.customer_store_code
    and upper(sg_wat.cust_name) = upper(str_mstr.customer_name)
  left join prdt_map
    on upper(sg_wat.prdtcode) = upper(prdt_map.customer_product_code)
    and upper(sg_wat.cust_name) = upper(prdt_map.customer_name)
  left join mtrl_dim
    on ltrim(prdt_map.material_code, 0) = mtrl_dim.sku
  left join itg_query_parameters as qry_param
    on qry_param.parameter_name = 'WAT' and qry_param.country_code = 'SG'
  left join map_sku
    on ltrim(sg_wat.barcode, 0) = map_sku.ean_upc
),
gdn_sg as   (
  select
    'GDN' as cust_id,
    year,
    mnth_id,
    week,
    prdt_map.master_code as master_code,
    coalesce(ltrim(prdt_map.material_code, 0), mtrl_dim.sku, map_sku.sku) as sap_code,
    barcode as product_barcode,
    brand,
    store_code as store,
    item_code,
    item_desc,
    sales_qty,
    sales_amount as net_sales,
    trx_date as bill_date,
    store_desc as store_name,
    qry_param.parameter_value as sold_to_code,
    coalesce(mtrl_dim.prod_key, map_sku.prod_key) as product_key,
    coalesce(mtrl_dim.prod_key_desc, map_sku.prod_key_desc) as product_key_desc,
    file_name as source,
    null as store_type,
    cust_name
  from itg_sg_scan_data_guardian as sg_gdn
  left join prdt_map
    on upper(sg_gdn.item_code) = upper(prdt_map.customer_product_code)
    and upper(sg_gdn.cust_name) = upper(prdt_map.customer_name)
  left join mtrl_dim
    on ltrim(prdt_map.material_code, 0) = mtrl_dim.sku
  left join map_sku
    on ltrim(sg_gdn.barcode, 0) = map_sku.ean_upc
  left join itg_query_parameters as qry_param
    on qry_param.parameter_name = 'GDN' and qry_param.country_code = 'SG'),

dfi as (
  select
    case
      when store_code like 'G%'
      then 'GNT'
      when store_code like 'Q%'
      then 'CSG'
      else '711'
    end as cust_id,
    year,
    mnth_id,
    week,
    prdt_map.master_code as master_code,
    coalesce(ltrim(prdt_map.material_code, 0), mtrl_dim.sku, map_sku.sku) as sap_code,
    barcode as product_barcode,
    brand,
    store_code as store,
    item_code,
    item_desc,
    sales_qty,
    sales_amount as net_sales,
    trx_date as bill_date,
    store_desc as store_name,
    qry_param.parameter_value as sold_to_code,
    coalesce(mtrl_dim.prod_key, map_sku.prod_key) as product_key,
    coalesce(mtrl_dim.prod_key_desc, map_sku.prod_key_desc) as product_key_desc,
    file_name as source,
    null as store_type,
    cust_name
  from itg_sg_scan_data_dfi as sg_dfi
  left join prdt_map
    on upper(sg_dfi.item_code) = upper(prdt_map.customer_product_code)
    and upper(sg_dfi.cust_name) = upper(prdt_map.customer_name)
  left join mtrl_dim
    on ltrim(prdt_map.material_code, 0) = mtrl_dim.sku
  left join map_sku
    on ltrim(sg_dfi.barcode, 0) = map_sku.ean_upc
  left join itg_query_parameters as qry_param
    on qry_param.parameter_name = 'DFI' and qry_param.country_code = 'SG' ),

marketplace as (
  select
    case
      when upper(store_name) = 'LAZADA'
      then 'LAZ'
      when upper(store_name) = 'SHOPEE'
      then 'SHP'
      when upper(store_name) = 'ZALORA'
      then 'ZLR'
      when upper(store_name) = 'REDMART'
      then 'RDM'
      when upper(store_name) LIKE 'QOO%'
      then 'QOO'
      else 'OTHERS'
    end as cust_id,
    year,
    mnth_id,
    week,
    prdt_map.master_code as master_code,
    coalesce(ltrim(prdt_map.material_code, 0), mtrl_dim.sku) as sap_code,
    barcode as product_barcode,
    brand,
    store_name as store,
    item_code,
    item_desc,
    sales_qty,
    net_sales,
    trx_date as bill_date,
    store_name,
    qry_param.parameter_value as sold_to_code,
    mtrl_dim.prod_key as product_key,
    mtrl_dim.prod_key_desc as product_key_desc,
    file_name as source,
    null as store_type,
    cust_name
  from itg_sg_scan_data_marketplace as sg_mkpl
  left join prdt_map
    on upper(sg_mkpl.item_code) = upper(prdt_map.customer_product_code)
    and upper(sg_mkpl.cust_name) = upper(prdt_map.customer_name)
  left join mtrl_dim
    on ltrim(prdt_map.material_code, 0) = mtrl_dim.sku
  left join itg_query_parameters as qry_param
    on qry_param.parameter_name = 'MKT_PLC' and qry_param.country_code = 'SG'
  where
    upper(sg_mkpl.store_name) <> 'REDMART'),

scommerce as (
  select
    'SCM' AS cust_id,
    year,
    mnth_id,
    week,
    prdt_map.master_code as master_code,
    coalesce(ltrim(prdt_map.material_code, 0), mtrl_dim.sku) as sap_code,
    barcode as product_barcode,
    brand,
    store,
    item_code,
    item_desc,
    sales_qty,
    net_sales,
    trx_date as bill_date,
    store_name,
    qry_param.parameter_value as sold_to_code,
    mtrl_dim.prod_key as product_key,
    mtrl_dim.prod_key_desc as product_key_desc,
    file_name as source,
    null as store_type,
    store_name as cust_name
  from itg_sg_scan_data_scommerce as sg_scm
  left join prdt_map
    on upper(sg_scm.item_code) = upper(prdt_map.customer_product_code)
    and upper(sg_scm.store_name) = upper(prdt_map.customer_name)
  left join mtrl_dim
    on ltrim(prdt_map.material_code, 0) = mtrl_dim.sku
  left join itg_query_parameters as qry_param
    on qry_param.parameter_name = 'SCM' and qry_param.country_code = 'SG'),

redmart as (
  select
    'RDM' as cust_id,
    year,
    mnth_id,
    week,
    prdt_map.master_code as master_code,
    coalesce(ltrim(prdt_map.material_code, 0), mtrl_dim.sku) as sap_code,
    barcode as product_barcode,
    brand,
    store,
    item_code,
    item_desc,
    sales_qty,
    net_sales,
    trx_date as bill_date,
    store_name,
    qry_param.parameter_value as sold_to_code,
    mtrl_dim.prod_key as product_key,
    mtrl_dim.prod_key_desc as product_key_desc,
    file_name as source,
    null as store_type,
    store_name as cust_name
  from itg_sg_scan_data_redmart as sg_rdm
  left join prdt_map
    on upper(sg_rdm.item_code) = upper(prdt_map.customer_product_code)
    and upper(sg_rdm.store_name) = upper(prdt_map.customer_name)
  left join mtrl_dim
    on ltrim(prdt_map.material_code, 0) = mtrl_dim.sku
  left join itg_query_parameters as qry_param
    on qry_param.parameter_name = 'RDM' and qry_param.country_code = 'SG'),

ntuc as (
  select
    'NTU' as cust_id,
    year,
    mnth_id,
    left(trx_date, 4) || lpad(substring(attribute2, 7), 2, 0) as week,
    prdt_map.master_code as master_code,
    coalesce(ltrim(prdt_map.material_code, 0), mtrl_dim.sku) as sap_code,
    barcode as product_barcode,
    brand,
    store_code as store,
    item_code,
    item_desc,
    sales_qty,
    net_sales,
    trx_date as bill_date,
    store_name,
    qry_param.parameter_value as sold_to_code,
    mtrl_dim.prod_key as product_key,
    mtrl_dim.prod_key_desc as product_key_desc,
    file_name as source,
    store_format as store_type,
    cust_name
  from itg_sg_scan_data_ntuc as sg_ntuc
  left join prdt_map
    on upper(sg_ntuc.item_code) = upper(prdt_map.customer_product_code)
    and upper(sg_ntuc.cust_name) = upper(prdt_map.customer_name)
  left join mtrl_dim
    on ltrim(prdt_map.material_code, 0) = mtrl_dim.sku
  left join itg_query_parameters as qry_param
    on qry_param.parameter_name = 'NTU' and qry_param.country_code = 'SG'),

amazon as (
  select
    'AMZ' as cust_id,
    year,
    mnth_id,
    week,
    prdt_map.master_code as master_code,
    coalesce(ltrim(prdt_map.material_code, 0), mtrl_dim.sku) as sap_code,
    barcode as product_barcode,
    brand,
    store,
    item_code,
    item_desc,
    sales_qty,
    net_sales,
    trx_date as bill_date,
    store_name,
    qry_param.parameter_value as sold_to_code,
    mtrl_dim.prod_key as product_key,
    mtrl_dim.prod_key_desc as product_key_desc,
    file_name as source,
    null as store_type,
    store_name as cust_name
  from itg_sg_scan_data_amazon as sg_amz
  left join prdt_map
    on upper(sg_amz.item_code) = upper(prdt_map.customer_product_code)
    and upper(sg_amz.store_name) = upper(prdt_map.customer_name)
  left join mtrl_dim
    on ltrim(prdt_map.material_code, 0) = mtrl_dim.sku
  left join itg_query_parameters as qry_param
    on qry_param.parameter_name = 'AMZ' and qry_param.country_code = 'SG'
),

transformed as (
    select * from wat_sg 
    union all
    select * from gdn_sg
    union all
    select * from dfi
    union all
    select * from marketplace
    union all
    select * from scommerce
    union all
    select * from redmart
    union all
    select * from ntuc
    union all
    select * from amazon
),
final as (
select 
  cust_id::varchar(10) as cust_id,
  year::varchar(20) as year,
  mnth_id::varchar(28) as mnth_id,
  week::varchar(20) as week,
  master_code::varchar(255) as master_code,
  sap_code::varchar(255) as sap_code,
  product_barcode::varchar(255) as product_barcode,
  brand::varchar(300) as brand,
  store::varchar(300) as store,
  item_code::varchar(300) as item_code,
  item_desc::varchar(500) as item_desc,
  sales_qty::number(10,0) as sales_qty,
  net_sales::number(10,6) as net_sales,
  bill_date::date as bill_date,
  store_name::varchar(300) as store_name,
  sold_to_code::varchar(200) as sold_to_code,
  product_key::varchar(300) as product_key,
  product_key_desc::varchar(500) as product_key_desc,
  source::varchar(255) as source,
  store_type::varchar(200) as store_type,
  cust_name::varchar(20) as cust_name
  from transformed
)
    

select * from final