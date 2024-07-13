with edw_product_dim as 
(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),

itg_in_mds_product_category_mapping as
(
    select * from {{ ref('inditg_integration__itg_in_mds_product_category_mapping') }}
),

final as 
(
    SELECT 
        pd.product_category_name,
        pd.franchise_name,
        pd.brand_name,
        pd.variant_name,
        pd.mothersku_name,
        pd.product_code,
        COALESCE(CASE 
                WHEN ((pmap.product_category1)::TEXT = (''::CHARACTER VARYING)::TEXT)
                    THEN NULL::CHARACTER VARYING
                ELSE pmap.product_category1
                END, 'Missing Product Category1'::CHARACTER VARYING) AS product_category1,
        COALESCE(CASE 
                WHEN ((pmap.product_category2)::TEXT = (''::CHARACTER VARYING)::TEXT)
                    THEN NULL::CHARACTER VARYING
                ELSE pmap.product_category2
                END, 'Missing Product Category2'::CHARACTER VARYING) AS product_category2,
        COALESCE(CASE 
                WHEN ((pmap.product_category3)::TEXT = (''::CHARACTER VARYING)::TEXT)
                    THEN NULL::CHARACTER VARYING
                ELSE pmap.product_category3
                END, 'Missing Product Category3'::CHARACTER VARYING) AS product_category3,
        COALESCE(CASE 
                WHEN ((pmap.product_category4)::TEXT = (''::CHARACTER VARYING)::TEXT)
                    THEN NULL::CHARACTER VARYING
                ELSE pmap.product_category4
                END, 'Missing Product Category4'::CHARACTER VARYING) AS product_category4
    FROM 
    (
        edw_product_dim pd 
        LEFT JOIN 
        itg_in_mds_product_category_mapping pmap 
        ON (
            (
                (
                    (
                        ((pmap.franchise_name)::TEXT = (pd.franchise_name)::TEXT)
                        AND ((pmap.brand_name)::TEXT = (pd.brand_name)::TEXT)
                        )
                    AND ((pmap.product_category)::TEXT = (pd.product_category_name)::TEXT)
                    )
                AND ((pmap.variant_name)::TEXT = (pd.variant_name)::TEXT)
                )
            )
    )
)
select * from final
