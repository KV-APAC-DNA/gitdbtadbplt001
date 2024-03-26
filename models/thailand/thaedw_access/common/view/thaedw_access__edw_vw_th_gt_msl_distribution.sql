with source as (
    select * from {{ ref('thaedw_integration__edw_vw_th_gt_msl_distribution') }}
),
final as (
    select 
        cntry_cd as "cntry_cd",
        crncy_cd as "crncy_cd",
        distributor_id as "distributor_id",
        distributor_name as "distributor_name",
        store_type_id as "store_type_id",
        store_type_name as "store_type_name",
        store_id as "store_id",
        store_name as "store_name",
        sales_rep_id as "sales_rep_id",
        sales_rep_name as "sales_rep_name",
        category_code as "category_code",
        category as "category",
        brand_code as "brand_code",
        brand as "brand",
        barcode as "barcode",
        product_description as "product_description",
        survey_date as "survey_date",
        no_distribution as "no_distribution",
        osa as "osa",
        oos as "oos",
        oos_reason as "oos_reason",
        valid_flag as "valid_flag",
    from source
)
select * from final