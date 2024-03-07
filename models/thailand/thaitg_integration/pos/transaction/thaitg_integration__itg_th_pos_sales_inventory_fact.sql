{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['customer']
    )
}}
with 
itg_th_tims_transdata as(
    select * from {{ ref('thaitg_integration__itg_th_tims_transdata') }}
),
edw_vw_os_customer_dim as(
    select * from {{ ref('thaedw_integration__edw_vw_th_customer_dim') }}
),
itg_th_pos_customer_dim as(
    select * from {{ ref('thaitg_integration__itg_th_pos_customer_dim') }}
),
edw_list_price as(
    select * from {{ ref('aspedw_integration__edw_list_price') }}
),
sdl_mds_th_product_master as(
    select * from {{ source('thasdl_raw','sdl_mds_th_product_master') }}
),
sdl_mds_th_customer_rsp as(
    select * from {{ source('thasdl_raw','sdl_mds_th_customer_rsp') }}
),
itg_lookup_retention_period as(
    select * from {{ source('thaitg_integration','itg_lookup_retention_period') }}
),
temp as 
(
    select
      barcode,
      code,
      name as matl_name,
      retailer_unit_conversion,
      row_number() over (partition by barcode order by createdate desc nulls last, code) as rnk
    from sdl_mds_th_product_master
    where
      barcode <> ''
  
),
product_master as 
(
  select distinct
    code as matl_num,
    barcode as barcd,
    matl_name,
    retailer_unit_conversion
  from temp
  where
    rnk = 1
),
temp2 as 
(
    select
      ltrim(cast(edw_list_price.material as text), 
      cast(cast(0 as varchar) as text)) as material,
      edw_list_price.amount as list_price,
      row_number() over (partition by ltrim(cast(edw_list_price.material as text), 
      cast(cast(0 as varchar) as text)) order by to_date(cast(edw_list_price.valid_to as text), 
      cast(cast('yyyymmdd' as varchar) as text)) desc, 
      to_date(cast(edw_list_price.dt_from as text), 
      cast(cast('yyyymmdd' as varchar) as text)) desc) as rn
    from edw_list_price
    where
      cast(edw_list_price.sls_org as text) in ('2400')
  
),
lp as 
(
    select
        *
    from temp2
        where rn = 1
),

trans as 
(
    select
        'Sales-Inv' as data_set,
        'Lotus' as customer,
        '108832' as sold_to_code,
        cust_dim.sap_prnt_cust_key,
        cust_dim.sap_prnt_cust_desc,
        null as supplier_code,
        to_date(so_inv.trans_dt) as trans_dt,
        product_master.matl_num as material_number,
        product_master.matl_name as material_name,
        so_inv.upc as barcode,
        so_inv.brnch_no as branch_code,
        th_cust_dim.branch_nm as branch_nm,
        so_inv.invt_qty,
        so_inv.sls_qty,
        coalesce(product_master.retailer_unit_conversion, 1) as retailer_unit_conversion,
        case
            when so_inv.invt_qty < 0
            then 0
            else so_inv.invt_qty * coalesce(product_master.retailer_unit_conversion, 1)
        end as inventory_qty_converted,
        so_inv.sls_qty * coalesce(product_master.retailer_unit_conversion, 1) as sales_qty_converted,
        lp.list_price,
        case
            when so_inv.invt_qty < 0
            then 0
            else (
            so_inv.invt_qty * coalesce(product_master.retailer_unit_conversion, 1)
            ) * lp.list_price
        end as inventory_gts,
        (
            so_inv.sls_qty * coalesce(product_master.retailer_unit_conversion, 1)
        ) * lp.list_price as sales_gts,
        cust_rsp.rsp as customer_rsp,
        case
            when so_inv.invt_qty < 0
            then 0
            else (
            so_inv.invt_qty * product_master.retailer_unit_conversion
            ) * cust_rsp.rsp
        end as stock_baht,
        so_inv.sls_baht
        --so_inv.foc_product
        from itg_th_tims_transdata as so_inv
        left join edw_vw_os_customer_dim as cust_dim
        on cust_dim.sap_cust_id = '108832'
        left join  product_master
        on product_master.barcd = so_inv.upc
        left join itg_th_pos_customer_dim as th_cust_dim
        on th_cust_dim.brnch_no = so_inv.brnch_no and upper(th_cust_dim.cust_cd) = 'LOTUS'
        left join  lp
        on lp.material = product_master.matl_num
        left join sdl_mds_th_customer_rsp as cust_rsp
        on cust_rsp.barcode = so_inv.upc
        and cust_rsp.valid_from <= so_inv.trans_dt
        and cust_rsp.valid_to >= so_inv.trans_dt
        and cust_rsp.account_name = 'Lotus'
        where
        coalesce(so_inv.trans_dt, to_date(current_timestamp())) > (
            select
            date_trunc('year',dateadd(year,retention_years * -1, current_timestamp()))
        from itg_lookup_retention_period
        where
        upper(table_name) = 'ITG_TH_POS_SALES_FACT'
    )
),
final as 
(
    select
        data_set::varchar(20) as data_set,
        customer::varchar(20) as customer,
        sold_to_code::varchar(6) as sold_to_code,
        sap_prnt_cust_key::varchar(12) as sap_prnt_cust_key,
        sap_prnt_cust_desc::varchar(50) as sap_prnt_cust_desc,
        supplier_code::varchar(20) as supplier_code,
        trans_dt::date as trans_dt,
        material_number::varchar(20) as material_number,
        material_name::varchar(500) as material_name,
        barcode::varchar(20) as bar_code,
        branch_code::varchar(20) as branch_code,
        branch_nm::varchar(200) as branch_name,
        invt_qty::number(17,4) as inventory_qty_source,
        sls_qty::number(17,4) as sales_qty_source,
        retailer_unit_conversion::number(31,0) as retailer_unit_conversion,
        inventory_qty_converted::number(17,4) as inventory_qty_converted,
        sales_qty_converted::number(17,4) as sales_qty_converted,
        list_price::number(20,4) as list_price,
        inventory_gts::number(20,4) as inventory_gts,
        sales_gts::number(20,4) as sales_gts,
        customer_rsp::number(20,4) as customer_rsp,
        stock_baht::number(20,4) as inventory_baht,
        sls_baht::number(20,4) as sales_baht,
        null::varchar(1) as foc_product
    from trans
)

select * from final