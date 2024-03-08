
with wks_th_makro_temp as 
(select * from dev_dna_core.snaposewks_integration.wks_th_makro_temp ),
edw_vw_os_customer_dim as (
select * from dev_dna_core.snaposeedw_integration.edw_vw_os_customer_dim
),
sdl_mds_th_product_master as (
select * from dev_dna_load.thasdl_raw.sdl_mds_th_product_master
),
edw_list_price as (
select * from dev_dna_core.snapaspedw_integration.edw_list_price
),
final as (
select
  cast(cal_mnth_id as varchar) as month,
  cust_dim.sap_prnt_cust_key,
  cust_dim.sap_prnt_cust_desc,
  cast('116819' as varchar) as sold_to_code,
  barcode,
  th_prod_dim.matl_num,
  item_number,
  item_desc,
  case
    when (
      item_desc like (
        'P%'
      ) or item_desc like (
        'D%'
      )
    )
    then 'Y'
    when (
      barcode is null or barcode = ''
    )
    then 'Y'
    else 'N'
  end as foc_product,
  retailer_unit_conversion,
  inventory_qty as inventory_qty_raw,
  (
    inventory_qty * coalesce(retailer_unit_conversion, 1)
  ) as inventory_qty,
  (
    inventory_qty * coalesce(retailer_unit_conversion, 1) * list_price
  ) as inventory_val,
  sellout_qty as sellout_qty_raw,
  (
    sellout_qty * coalesce(retailer_unit_conversion, 1)
  ) as sellout_qty,
  (
    sellout_qty * coalesce(retailer_unit_conversion, 1) * list_price
  ) as sellout_val,
  location_number,
  location_name,
  list_price,
  trans_date
from wks_th_makro_temp as makro
left join (
  select
    sap_cust_id,
    sap_prnt_cust_key,
    sap_prnt_cust_desc
  from edw_vw_os_customER_DIM
  where
    sap_cntry_cd = 'TH'
) AS cust_dim
  ON cust_dim.sap_cust_id = '116819'
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
) as th_prod_dim
  on th_prod_dim.barcd = makro.barcode
left join (
  select
    *
  from (
    select
      ltrim(cast(edw_list_price.material as text), cast(cast(0 as varchar) as text)) as material,
      edw_list_price.amount as list_price,
      row_number() over (partition by ltrim(cast(edw_list_price.material as text), 
      cast(cast(0 as varchar) as text)) order by to_date(cast(edw_list_price.valid_to as text), 
      cast(cast('YYYYMMDD' as varchar) as text)) desc, 
      to_date(cast(edw_list_price.dt_from as text), 
      cast(cast('YYYYMMDD' as varchar) as text)) desc) as rn
    from edw_list_price
    where
      cast(edw_list_price.sls_org as text) in ('2400')
  )
  where
    rn = 1
) as os_matl_dim
  on os_matl_dim.material = th_prod_dim.matl_num
)
select * from final 