with wks_ph_siso_propagate_final as (
    select * from {{ ref('phlwks_integration__wks_ph_siso_propagate_final') }}
),
wks_ph_inv_prod_hier as (
    select * from {{ ref('phlwks_integration__wks_ph_inv_prod_hier') }}
),
wks_ph_inv_cust_hier as (
    select * from {{ ref('phlwks_integration__wks_ph_inv_cust_hier') }}
),
final as
(   
    select 
        month,
        case
            when a.dstrbtr_grp_cd is null then 'NA'
            when a.dstrbtr_grp_cd = '' then 'NA'
            else trim(a.dstrbtr_grp_cd)
        end as dstrbtr_grp_cd,
        case
            when a.dstr_cd_nm is null then 'NA'
            when a.dstr_cd_nm = '' then 'NA'
            else trim(a.dstr_cd_nm)
        end as dstrbtr_grp_cd_nm,
        case
            when product.global_prod_brand is null then 'NA'
            when product.global_prod_brand = '' then 'NA'
            else trim(product.global_prod_brand)
        end as global_prod_brand,
        case
            when product.global_prod_variant is null then 'NA'
            when product.global_prod_variant = '' then 'NA'
            else trim(product.global_prod_variant)
        end as global_prod_variant,
        case
            when product.global_prod_segment is null then 'NA'
            when product.global_prod_segment = '' then 'NA'
            else trim(product.global_prod_segment)
        end as global_prod_segment,
        case
            when product.global_prod_category is null then 'NA'
            when product.global_prod_category = '' then 'NA'
            else trim(product.global_prod_category)
        end as global_prod_category,
        trim(nvl(nullif(product.pka_product_key, ''), 'NA')) as pka_product_key,
        trim(nvl(nullif(product.pka_size_desc, ''), 'NA')) as pka_size_desc,
        case
            when cust_hier.sap_prnt_cust_key is null then 'Not Assigned'
            when cust_hier.sap_prnt_cust_key = '' then 'Not Assigned'
            else trim(cust_hier.sap_prnt_cust_key)
        end as sap_prnt_cust_key,
        sum(last_3months_so_value) as last_3months_so_val,
        sum(last_6months_so_value) as last_6months_so_val,
        sum(last_12months_so_value) as last_12months_so_val,
        sum(last_36months_so_value) as last_36months_so_val,
        case
            when coalesce(last_36months_so_val, 0) > 0
            and coalesce(last_12months_so_val, 0) <= 0 then 'N'
            else 'Y'
        end as healthy_inventory
    from 
        (
            select *
            from wks_ph_siso_propagate_final f
            where left (month, 4) >= (date_part(year, current_timestamp) -2)
        ) as a,
        wks_ph_inv_prod_hier as product,
        wks_ph_inv_cust_hier as cust_hier
    where a.matl_num = product.sku_cd(+)
        and a.sap_parent_customer_key = cust_hier.sap_prnt_cust_key(+)
        and a.parent_customer_cd = cust_hier.parent_cust_cd(+)
    group by month,
        a.dstrbtr_grp_cd,
        a.dstr_cd_nm,
        product.global_prod_brand,
        product.global_prod_variant,
        product.global_prod_segment,
        product.global_prod_category,
        product.pka_product_key,
        product.pka_size_desc,
        cust_hier.sap_prnt_cust_key
)
select * from final