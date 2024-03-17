with itg_th_tims_transdata as(
    select * from snaposeitg_integration.itg_th_tims_transdata
),
edw_vw_th_customer_dim as(
    select * from snaposeedw_integration.edw_vw_os_customer_dim
    where sap_cntry_cd = 'TH'
),
itg_th_pos_customer_dim as(
    select * from snaposeitg_integration.itg_th_pos_customer_dim
),
edw_list_price as(
    select * from snapaspedw_integration.edw_list_price
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
itg_th_mt_7_11 as   
(
    select * from dev_dna_core.snaposeitg_integration.itg_th_mt_7_11
),
itg_th_mt_tops as
(
    select * from dev_dna_core.snaposeitg_integration.itg_th_mt_tops
),
itg_th_mt_bigc as
(
    select * from dev_dna_core.snaposeitg_integration.itg_th_mt_bigc
),
th_makro_inv_so as
(
   select * from dev_dna_core.snaposeitg_integration.th_makro_inv_so 
),
product_master as 
(
    select distinct
        code as matl_num,
        barcode as barcd,
        matl_name,
        retailer_unit_conversion
    from
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
        )
    where
    rnk = 1
  
),
lp as 
(
    select * from
    (
        select
            ltrim(cast(edw_list_price.material as text), 
            cast(cast(0 as varchar) as text)) as material,
            edw_list_price.amount as list_price,
            row_number() over   (
                                    partition by ltrim(cast(edw_list_price.material as text), 
                                    cast(cast(0 as varchar) as text)) order by to_date(cast(edw_list_price.valid_to as text), 
                                    cast(cast('yyyymmdd' as varchar) as text)) desc, 
                                    to_date(cast(edw_list_price.dt_from as text), 
                                    cast(cast('yyyymmdd' as varchar) as text)) desc
                                ) as rn
        from edw_list_price
        where
        cast(edw_list_price.sls_org as text) in ('2400')
    )
    where rn = 1
),
lotus as 
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
        so_inv.upc as bar_code,
        so_inv.brnch_no as branch_code,
        th_cust_dim.branch_nm as branch_name,
        so_inv.invt_qty as inventory_qty_source,
        so_inv.sls_qty as sales_qty_source,
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
        end as inventory_baht,
        so_inv.sls_baht as sales_baht,
        null as foc_product
        --so_inv.foc_product
        from itg_th_tims_transdata as so_inv
        left join edw_vw_th_customer_dim as cust_dim
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
itg_7_11 as 
(
    select
        supplier_code,
        inventory_date,
        barcode,
        inventory_location,
        sales_qty,
        case
        when inventory_date < '2021-10-18'
        then (
            actual_onhand_stock_qty + qty_in_transit
        )
        else (
            (
            case
                when (
                actual_onhand_stock_qty - sales_qty
                ) < 0
                then 0
                else (
                actual_onhand_stock_qty - sales_qty
                )
            end
            ) + qty_in_transit
        )
        end as inventory_qty
    from itg_th_mt_7_11
),
seven_eleven as 
(
    select
        'Sales-Inv' AS data_set,
        '7-Eleven' AS customer,
        '108827' AS sold_to_code,
        os_cust_dim.sap_prnt_cust_key as sap_prnt_cust_key,
        os_cust_dim.sap_prnt_cust_desc as sap_prnt_cust_desc,
        so_inv.supplier_code as supplier_code,
        so_inv.inventory_date::date as trans_dt,
        product_master.matl_num as material_number,
        product_master.matl_name as material_name,
        so_inv.barcode as bar_code,
        so_inv.inventory_location as branch_code,
        cust_dim.branch_nm as branch_name,
        so_inv.inventory_qty as inventory_qty_source,
        so_inv.sales_qty as sales_qty_source,
        null as retailer_unit_conversion,
        case when so_inv.inventory_qty < 0 then 0 else so_inv.inventory_qty end as inventory_qty_converted,
        so_inv.sales_qty as sales_qty_converted,
        lp.list_price as list_price,
        case when so_inv.inventory_qty < 0 then 0 else so_inv.inventory_qty * lp.list_price end as inventory_gts,
        so_inv.sales_qty * lp.list_price as sales_gts,
        null as customer_rsp,
        null as inventory_baht,
        null as sales_baht,
        null as foc_product,
    from itg_7_11 as so_inv
    left join itg_th_pos_customer_dim as cust_dim
    on cust_dim.brnch_no = so_inv.inventory_location
    and upper(cust_dim.cust_cd) = '7-ELEVEN'
    left join edw_vw_th_customer_dim as os_cust_dim
    ON os_cust_dim.sap_cust_id = '108827'
    left join product_master
    ON product_master.barcd = so_inv.barcode
    left join lp
    ON product_master.matl_num = lp.material
),

tops as
(   select
        'Sales-Inv' AS data_set,
        'Tops' AS customer,
        '110611' AS sold_to_code,
        os_cust_dim.sap_prnt_cust_key as sap_prnt_cust_key,
        os_cust_dim.sap_prnt_cust_desc as sap_prnt_cust_desc,
        null as supplier_code,
        so_inv.inventory_date::date as trans_dt,
        product_master.matl_num as material_number,
        product_master.matl_name as material_name,
        so_inv.barcode as bar_code,
        cust_dim.brnch_no as branch_code,
        cust_dim.branch_nm as branch_name,
        so_inv.inventory_qty as inventory_qty_source,
        so_inv.sales_qty as sales_qty_source,
        null as retailer_unit_conversion,
        case when so_inv.inventory_qty < 0 then 0 else so_inv.inventory_qty end as inventory_qty_converted,
        so_inv.sales_qty as sales_qty_converted,
        lp.list_price as list_price,
        case when so_inv.inventory_qty < 0 then 0 else so_inv.inventory_qty * lp.list_price end as inventory_gts,
        so_inv.sales_qty * lp.list_price as sales_gts,
        null as customer_rsp,
        null as inventory_baht,
        null as sales_baht,
        null as foc_product
        from 
    (
        select
            inventory_date,
            inventory_location,
            barcode,
            sales_qty,
            (
            actual_onhand_stock_qty + qty_in_transit
            ) as inventory_qty
        from itg_th_mt_tops
    ) as so_inv
    left join itg_th_pos_customer_dim as cust_dim
    on cust_dim.brnch_no = so_inv.inventory_location and upper(cust_dim.cust_cd) = 'TOPS'
    left join edw_vw_th_customer_dim as os_cust_dim
    on os_cust_dim.sap_cust_id = '110611'
    left join product_master
    on product_master.barcd = so_inv.barcode
    left join lp
    on product_master.matl_num = lp.material
),
bigc as
(
    select 
        'Sales-Inv' as data_set,
        'BigC' as customer,
        so_inv.sold_to_code,
        cust_dim.sap_prnt_cust_key,
        cust_dim.sap_prnt_cust_desc,
        null as supplier_code,
        so_inv.trans_date as trans_dt,
        so_inv.matl_num as material_number,
        product_master.matl_name as material_name,
        so_inv.barcode as bar_code,
        split_part(so_inv.store, ':', 1) as branch_code,
        split_part(so_inv.store, ':', 2) as branch_name,
        so_inv.inv_qty_raw as inventory_qty_source,
        so_inv.sls_qty_raw as sales_qty_source,
        so_inv.retailer_unit_conversion,
        case
            when so_inv.inventory_quantity < 0 then 0
            else so_inv.inventory_quantity
        end as inventory_qty_converted,
        so_inv.sellout_quantity as sales_qty_converted,
        so_inv.list_price,
        case
            when so_inv.inventory_value < 0 then 0
            else so_inv.inventory_value
        end inventory_gts,
        so_inv.sellout_value as sales_gts,
        null as customer_rsp,
        so_inv.stock_baht as inventory_baht,
        so_inv.sales_baht as sales_baht,
        so_inv.foc_product
from itg_th_mt_bigc as so_inv
    left join edw_vw_th_customer_dim cust_dim on cust_dim.sap_cust_id = so_inv.sold_to_code
    left join  product_master on product_master.barcd = so_inv.barcode
),
makro as
(
    select 
        'Sales-Inv' as data_set,
        'Makro' as customer,
        so_inv.sold_to_code,
        so_inv.sap_prnt_cust_key,
        so_inv.sap_prnt_cust_desc,
        null as supplier_code,
        so_inv.trans_date as trans_dt,
        so_inv.matl_num as material_number,
        so_inv.item_desc as material_name,
        so_inv.barcode as bar_code,
        so_inv.location_number as   branch_code,
        so_inv.location_name as branch_name,
        so_inv.inventory_qty_raw as inventory_qty_source,
        so_inv.sellout_qty_raw as sales_qty_source,
        so_inv.retailer_unit_conversion as retailer_unit_conversion,
        case when so_inv.inventory_qty_raw < 0 then 0 else so_inv.inventory_qty end as inventory_qty_converted,
        so_inv.sellout_qty as sales_qty_converted,
        so_inv.list_price as list_price,
        case when so_inv.inventory_qty_raw < 0 then 0 else so_inv.inventory_val end as inventory_gts,
        so_inv.sellout_val as sales_gts,
        cust_rsp.rsp as customer_rsp,
        case when so_inv.inventory_qty_raw < 0 then 0 else so_inv.inventory_qty*cust_rsp.rsp end inventory_baht,
        so_inv.sellout_qty * cust_rsp.rsp as sales_baht,
        so_inv.foc_product
    from th_makro_inv_so so_inv
    left join sdl_mds_th_customer_rsp cust_rsp
    on trim(cust_rsp.barcode) = so_inv.barcode 
    and cust_rsp.valid_from <= so_inv.trans_date and cust_rsp.valid_to >= so_inv.trans_date
    and cust_rsp.account_name = 'Makro'  
    where (inventory_qty <> 0 or sellout_qty <> 0 )
),
combined as
(
    select * from lotus
    union all
    select * from seven_eleven
    union all
    select * from tops
    union all
    select * from bigc
    union all
    select * from makro
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
        bar_code::varchar(20) as bar_code,
        branch_code::varchar(20) as branch_code,
        branch_name::varchar(200) as branch_name,
        inventory_qty_source::number(17,4) as inventory_qty_source,
        sales_qty_source::number(17,4) as sales_qty_source,
        retailer_unit_conversion::number(31,0) as retailer_unit_conversion,
        inventory_qty_converted::number(17,4) as inventory_qty_converted,
        sales_qty_converted::number(17,4) as sales_qty_converted,
        list_price::number(20,4) as list_price,
        inventory_gts::number(20,4) as inventory_gts,
        sales_gts::number(20,4) as sales_gts,
        customer_rsp::number(20,4) as customer_rsp,
        inventory_baht::number(20,4) as inventory_baht,
        sales_baht::number(20,4) as sales_baht,
        foc_product::varchar(1) as foc_product,
    from combined
)
select * from final