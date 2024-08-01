with 
edw_product_dim as 
(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),
itg_ventasys_jnj_prod_mapping as 
(
    select * from {{ source('inditg_integration', 'itg_ventasys_jnj_prod_mapping') }}
), 
final as 
(
    SELECT prod_vent,
       listagg(product_name_sales,',') within group(ORDER BY product_name_sales) AS product_name_sales,
       franchise_code,
       franchise_name
FROM (SELECT DISTINCT mapp.prod_vent,
             prod.variant_name AS product_name_sales,
             prod.franchise_code AS franchise_code,
             prod.franchise_name AS franchise_name
      FROM edw_product_dim prod
        INNER JOIN itg_ventasys_jnj_prod_mapping mapp
                ON mapp.prod_name = prod.variant_name
               AND (mapp.prod_vent = 'ORSL (Core)'
                OR mapp.prod_vent = 'ORSL (Plus)'
                OR mapp.prod_vent = 'ORSL (Rehydrate)'))
GROUP BY prod_vent,
         franchise_code,
         franchise_name
ORDER BY prod_vent
)
select * from final