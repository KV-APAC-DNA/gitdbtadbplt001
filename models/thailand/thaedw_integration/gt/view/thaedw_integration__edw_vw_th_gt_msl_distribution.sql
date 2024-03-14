with source as
(
    select * from snaposeitg_integration.itg_th_gt_msl_distribution
),
final as
(   
    SELECT 
        cntry_cd,
        crncy_cd,
        distributor_id,
        distributor_name,
        re_code AS store_type_id,
        re_name AS store_type_name,
        store_id,
        store_name,
        sales_rep_id,
        sales_rep_name,
        category_code,
        category,
        brand_code,
        brand,
        barcode,
        product_description,
        survey_date,
        no_distribution,
        osa,
        oos,
        oos_reason,
        case
            when
            (left(trim((store_id)::text),1) = '_'::text) 
            then 'N'::text
            else 'Y'::text
        end as valid_flag
    from source
)
select * from final