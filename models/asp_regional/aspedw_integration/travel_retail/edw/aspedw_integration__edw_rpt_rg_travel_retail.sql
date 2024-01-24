--Import CTE
with edw_rg_travel_retail as (
    select * from {{ref('aspedw_integration__edw_rg_travel_retail')}}
),
wks_rg_travel_retail_inventory as (
    select * from {{source('aspwks_integration','wks_rg_travel_retail_inventory')}}
),
edw_material_dim as (
    select * from {{ref('aspedw_integration__edw_material_dim')}}
),
itg_rg_travel_retail_channel_mapping as (
    select * from {{ref('aspitg_integration__itg_rg_travel_retail_channel_mapping')}}
),
wks_rg_travel_retail_inventory_excl_sku as (
    select * from {{source('aspwks_integration','wks_rg_travel_retail_inventory_excl_sku')}}
),

--Logical CTE
tr_sales as (
    select sales.ctry_cd::varchar(2) as ctry_cd,
            sales.crncy_cd,
            sales.country_name,
            sales.retailer_name::varchar(75) as retailer_name,
            sales."year" as year,
            sales."month" as month,
            sales.year_month::varchar(20) as year_month,
            sales.dcl_code,
            sales.matl_num,
            sales.sub_brand,
            sales.sub_brand_desc,
            sales.variant,
            sales.variant_desc,
            sales.sub_variant,
            sales.sub_variant_desc,
            sales.ingredient,
            sales.ingredient_desc,
            sales.cover,
            sales.cover_desc,
            sales.form,
            sales.form_desc,
            sales.product_key_desc,
            sales.target_type::varchar(10) as target_type,
            sales.doors::varchar(75) as doors,
            sales.cust_num,
            sales.sales_location::varchar(50) as sales_location,
            sales.sales_channel::varchar(50) as sales_channel,
            coalesce(sales.sellout_qty, 0) as sellout_qty,
            coalesce(sales.sellout_value_usd, 0) as sellout_value_usd,
            coalesce(sales.sellin_qty, 0) as sellin_qty,
            coalesce(sales.sellin_value_usd, 0) as sellin_value_usd,
            coalesce(sales.stock_qty, 0) as inv_qty,
            coalesce(sales.stock_value_usd, 0) as inv_value_usd,
            0 as opening_inv_qty,
            0 as closing_inv_qty,
            0 as opening_inv_value_usd,
            0 as closing_inv_value_usd,
            coalesce(sales.nts_tgt_usd, 0) as nts_tgt_usd
        from edw_rg_travel_retail as sales
),
tr_inv as (
    select inv.ctry_cd::varchar(2) as ctry_cd,
            inv.crncy_cd,
            inv.country_name,
            inv.retailer_name::varchar(75) as retailer_name,
            inv.year,
            inv.month,
            inv.year_month::varchar(20) as year_month,
            inv.dcl_code,
            inv.matl_num,
            mat.pka_sub_brand_cd as sub_brand,
            mat.pka_sub_brand_desc as sub_brand_desc,
            mat.pka_variant_cd as variant,
            mat.pka_variant_desc as variant_desc,
            mat.pka_sub_variant_cd as sub_variant,
            mat.pka_sub_variant_desc as sub_variant_desc,
            mat.pka_ingredient_cd as ingredient,
            mat.pka_ingredient_desc as ingredient_desc,
            mat.pka_cover_cd as cover,
            mat.pka_cover_desc as cover_desc,
            mat.pka_form_cd as form,
            mat.pka_form_desc as form_desc,
            mat.pka_product_key_description as product_key_desc,
            'NA'::varchar(10) as target_type,
            'NA'::varchar(75) as doors,
            inv.cust_num,
            cm.sales_location::varchar(50) as sales_location,
            inv.sales_channel::varchar(50) as sales_channel,
            0 as sellout_qty,
            0 as sellout_value_usd,
            0 as sellin_qty,
            0 as sellin_value_usd,
            case
                when upper(inv.ctry_cd) in ('SG') then coalesce(inv.closing_inv_qty, 0)
                else 0
            end as inv_qty,
            case
                when upper(inv.ctry_cd) in ('SG') then coalesce(inv.closing_inv_value_usd, 0)
                else 0
            end as inv_value_usd,
            coalesce(inv.opening_inv_qty, 0) as opening_inv_qty,
            coalesce(inv.closing_inv_qty, 0) as closing_inv_qty,
            coalesce(inv.opening_inv_value_usd, 0) as opening_inv_value_usd,
            coalesce(inv.closing_inv_value_usd, 0) as closing_inv_value_usd,
            0 as nts_tgt_usd
        from wks_rg_travel_retail_inventory as inv
            left join edw_material_dim as mat on inv.matl_num = ltrim(mat.matl_num, 0)
            left join (
                select distinct ctry_cd,
                    country_name,
                    cust_num,
                    retailer_name,
                    sales_location,
                    sales_channel
                from itg_rg_travel_retail_channel_mapping
            ) as cm on inv.cust_num = cm.cust_num
            and upper(inv.country_name) = upper(cm.country_name)
),
tr_inv_es as (
    select inv1.ctry_cd::varchar(2) as ctry_cd,
            inv1.crncy_cd,
            inv1.country_name,
            inv1.retailer_name::varchar(75) as retailer_name,
            inv1.year,
            inv1.month,
            inv1.year_month::varchar(20) as year_month,
            'NA' as dcl_code,
            'NA' as matl_num,
            'NA' as sub_brand,
            'UNASSIGNED' as sub_brand_desc,
            'NA' as variant,
            'UNASSIGNED' as variant_desc,
            'NA' as sub_variant,
            'UNASSIGNED' as sub_variant_desc,
            'NA' as ingredient,
            'UNASSIGNED' as ingredient_desc,
            'NA' as cover,
            'UNASSIGNED' as cover_desc,
            'NA' as form,
            'UNASSIGNED' as form_desc,
            'UNASSIGNED' as product_key_desc,
            'NA'::varchar(10) as target_type,
            cm.door_name::varchar(75) as doors,
            inv1.cust_num,
            cm.sales_location::varchar(50) as sales_location,
            inv1.sales_channel::varchar(50) as sales_channel,
            0 as sellout_qty,
            0 as sellout_value_usd,
            0 as sellin_qty,
            0 as sellin_value_usd,
            case
                when (
                    upper(inv1.ctry_cd) in ('CN')
                    and inv1.cust_num = '135673'
                ) then coalesce(inv1.closing_inv_qty, 0)
                else 0
            end as inv_qty,
            case
                when (
                    upper(inv1.ctry_cd) in ('CN')
                    and inv1.cust_num = '135673'
                ) then coalesce(inv1.closing_inv_value_usd, 0)
                else 0
            end as inv_value_usd,
            coalesce(inv1.opening_inv_qty, 0) as opening_inv_qty,
            coalesce(inv1.closing_inv_qty, 0) as closing_inv_qty,
            coalesce(inv1.opening_inv_value_usd, 0) as opening_inv_value_usd,
            coalesce(inv1.closing_inv_value_usd, 0) as closing_inv_value_usd,
            0 as nts_tgt_usd
        from wks_rg_travel_retail_inventory_excl_sku as inv1
            left join (
                select distinct ctry_cd,
                    country_name,
                    cust_num,
                    retailer_name,
                    sales_location,
                    sales_channel,
                    door_name
                from itg_rg_travel_retail_channel_mapping
            ) as cm on inv1.cust_num = cm.cust_num
            and upper(inv1.country_name) = upper(cm.country_name)
),
combined as (
    select * from tr_sales
    union all
    select * from tr_inv
    union all
    select * from tr_inv_es
),

transformed as (
    select 
    ctry_cd::varchar(2) as ctry_cd,
    crncy_cd::varchar(3) as crncy_cd,
    country_name::varchar(30) as country_name,
    retailer_name::varchar(75) as retailer_name,
    year::number(18,0) as "year",
    month::number(18,0) as "month",
    year_month::varchar(20) as year_month,
    dcl_code::varchar(50) as dcl_code,
    matl_num::varchar(30) as matl_num,
    sub_brand::varchar(4) as sub_brand,
    coalesce(sub_brand_desc, 'UNASSIGNED')::varchar(30) as sub_brand_desc,
    variant::varchar(4) as variant,
    coalesce(variant_desc, 'UNASSIGNED')::varchar(30) as variant_desc,
    sub_variant::varchar(4) as sub_variant,
    coalesce(sub_variant_desc, 'UNASSIGNED')::varchar(30) as sub_variant_desc,
    ingredient::varchar(4) as ingredient,
    coalesce(ingredient_desc, 'UNASSIGNED')::varchar(30) as ingredient_desc,
    cover::varchar(4) as cover,
    coalesce(cover_desc, 'UNASSIGNED')::varchar(30) as cover_desc,
    form::varchar(4) as form,
    coalesce(form_desc, 'UNASSIGNED')::varchar(30) as form_desc,
    coalesce(product_key_desc, 'UNASSIGNED')::varchar(255) as product_key_desc,
    target_type::varchar(10) as target_type,
    doors::varchar(75) as doors,
    cust_num::varchar(30) as cust_num,
    initcap(sales_location)::varchar(50) as sales_location,
    sales_channel::varchar(50) as sales_channel,
    sellout_qty::number(38,5) as sellout_qty,
    sellout_value_usd::number(38,5) as sellout_value_usd,
    sellin_qty::number(38,5) as sellin_qty,
    sellin_value_usd::number(38,5) as sellin_value_usd,
    inv_qty::number(38,5) as inv_qty,
    inv_value_usd::number(38,5) as inv_value_usd,
    opening_inv_qty::number(38,5) as opening_inv_qty,
    closing_inv_qty::number(38,5) as closing_inv_qty,
    opening_inv_value_usd::number(38,5) as opening_inv_value_usd,
    closing_inv_value_usd::number(38,5) as closing_inv_value_usd,
    nts_tgt_usd::number(38,5) as nts_tgt_usd
    from combined
    where date_part(YEAR, to_date(year_month, 'YYYYMM')) >= date_part(year, current_timestamp()) - 2
),

--Final CTE
final as (
    select * from combined
)

select * from final