with itg_kr_coupang_product_ranking_daily as 
(
    select * from ntaitg_integration.itg_kr_coupang_product_ranking_daily
),
itg_kr_coupang_product_master as 
(
   select * from ntaitg_integration.itg_kr_coupang_product_master 
),
final as
(   
    SELECT 
        daily.product_ranking_date,
        daily.category_depth1,
        daily.category_depth2,
        daily.category_depth3,
        daily.coupang_sku_id,
        daily.coupang_sku_name,
        daily.ranking,
        daily.data_granularity,
        pm.all_brand,
        pm.jnj_product_flag
    FROM itg_kr_coupang_product_ranking_daily daily
        LEFT JOIN itg_kr_coupang_product_master pm ON 
        (
            (
                (
                    (daily.coupang_sku_id)::text = (pm.coupang_sku_id)::text
                )
                AND (
                    (daily.product_ranking_date)::text = (pm.yearmo)::text
                )
            )
        )
)
select * from final