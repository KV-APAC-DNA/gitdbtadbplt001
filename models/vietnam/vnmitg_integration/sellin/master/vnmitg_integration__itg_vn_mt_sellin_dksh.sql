{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['billing_no', 'invoice_date'],
        pre_hook= "{%if is_incremental()%}
        delete from {{this}} where (billing_no,invoice_date) in (select distinct billing_no, case when invoice_date like '%-%' then to_char(to_date(invoice_date,'DD-MM-YYYY'),'YYYYMMDD') else invoice_date end as invoice_date from {{ source('vnmsdl_raw', 'sdl_vn_mt_sellin_dksh') }});
        {% endif %}"
    )
}}

with source as(
    select *, dense_rank() over (partition by billing_no, invoice_date order by filename desc) rnk
    from {{ source('vnmsdl_raw', 'sdl_vn_mt_sellin_dksh') }}
    where filename not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_mt_sellin_dksh__null_test')}}
    )
),
itg_query_parameters as(
    select * from {{ source('aspitg_integration', 'itg_query_parameters') }}
),
final as(
    select
        supplier_code::varchar(10) as supplier_code,
        supplier_name::varchar(100) as supplier_name,
        plant::varchar(10) as plant,
        productid::varchar(20) as productid,
        product::varchar(255) as product,
        brand::varchar(100) as brand,
        sellin_category::varchar(100) as sellin_category,
        product_group::varchar(100) as product_group,
        product_sub_group::varchar(100) as product_sub_group,
        unit_of_measurement::varchar(10) as unit_of_measurement,
        custcode::varchar(20) as custcode,
        customer::varchar(100) as customer,
        address::varchar(100) as address,
        district::varchar(100) as district,
        province::varchar(50) as province,
        region::varchar(20) as region,
        zone::varchar(20) as zone,
        case when cust_group in (
            select 
            distinct parameter_value 
            from 
            itg_query_parameters 
            where 
            country_code = 'VN' 
            and parameter_name = 'ecom_cust_group'
        ) then 'ECOM' else channel end::varchar(10) as channel, 
        sellin_sub_channel::varchar(50) as sellin_sub_channel, 
        cust_group::varchar(50) as cust_group, 
        billing_no::varchar(20) as billing_no, 
        --invoice date should be in YYYYMMDD format as per existing data flow
        case when invoice_date like '%-%' then to_char(
            to_date(invoice_date, 'DD-MM-YYYY'), 
            'YYYYMMDD'
        ) else invoice_date end::varchar(20) as invoice_date, 
        qty_include_foc::number(18,0) as qty_include_foc, 
        qty_exclude_foc::number(18,0) as qty_exclude_foc, 
        foc::varchar(100) as foc, 
        net_price_wo_vat::number(20,5) as net_price_wo_vat, 
        tax::number(20,5) as tax, 
        net_amount_wo_vat::number(20,5) as net_amount_wo_vat, 
        net_amount_w_vat::number(20,5) as net_amount_w_vat, 
        gross_amount_wo_vat::number(20,5) as gross_amount_wo_vat, 
        gross_amount_w_vat::number(20,5) as gross_amount_w_vat, 
        list_price_wo_vat::number(20,5) as list_price_wo_vat, 
        vendor_lot::varchar(100) as vendor_lot, 
        order_type::varchar(200) as order_type, 
        red_invoice_no::varchar(200) as red_invoice_no, 
        expiry_date::varchar(200) as expiry_date, 
        order_no::varchar(200) as order_no, 
        order_date::varchar(200) as order_date, 
        period::varchar(20) as period, 
        sellout_sub_channel::varchar(50) as sellout_sub_channel, 
        group_account::varchar(50) as group_account, 
        account::varchar(50) as account, 
        name_st_or_ddp::varchar(100) as name_st_or_ddp, 
        zone_or_area::varchar(20) as zone_or_area, 
        franchise::varchar(50) as franchise, 
        sellout_category::varchar(50) as sellout_category, 
        sub_cat::varchar(50) as sub_cat, 
        sub_brand::varchar(50) as sub_brand, 
        barcode::varchar(20) as barcode, 
        base_or_bundle::varchar(20) as base_or_bundle, 
        size::varchar(20) as size, 
        key_chain::varchar(20) as key_chain, 
        status::varchar(20) as status, 
        filename::varchar(100) as filename, 
        run_id::number(14,0) as run_id, 
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final