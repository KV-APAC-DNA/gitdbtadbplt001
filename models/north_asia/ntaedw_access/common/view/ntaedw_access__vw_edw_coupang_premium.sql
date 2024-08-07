with source as (
    select * from {{ ref('ntaedw_integration__vw_edw_coupang_premium') }}
),
final as (
    select
        ctry_cd as "ctry_cd",
        ctry_nm as "ctry_nm",
        data_source as "data_source",
        reference_date as "reference_date",
        data_granularity as "data_granularity",
        category_depth1 as "category_depth1",
        category_depth2 as "category_depth2",
        category_depth3 as "category_depth3",
        ranking as "ranking",
        prev_day_ranking as "prev_day_ranking",
        prev_week_ranking as "prev_week_ranking",
        prev_mon_ranking as "prev_mon_ranking",
        sku_id as "sku_id",
        sku_name as "sku_name",
        jnj_brand as "jnj_brand",
        rank_change as "rank_change",
        vendoritemid as "vendoritemid",
        ean as "ean",
        all_brand as "all_brand",
        jnj_product_flag as "jnj_product_flag",
        coupang_product_name as "coupang_product_name",
        review_score_star as "review_score_star",
        review_contents as "review_contents",
        coupang_id as "coupang_id",
        new_user_count as "new_user_count",
        curr_user_count as "curr_user_count",
        tot_user_count as "tot_user_count",
        new_user_sales_amt as "new_user_sales_amt",
        curr_user_sales_amt as "curr_user_sales_amt",
        new_user_avg_product_sales_price as "new_user_avg_product_sales_price",
        curr_user_avg_product_sales_price as "curr_user_avg_product_sales_price",
        tot_user_avg_product_sales_price as "tot_user_avg_product_sales_price",
        search_keyword as "search_keyword",
        product_name as "product_name",
        by_search_keyword as "by_search_keyword",
        by_product_ranking as "by_product_ranking",
        product_ranking as "product_ranking",
        click_rate as "click_rate",
        cart_transition_rate as "cart_transition_rate",
        purchase_conversion_rate as "purchase_conversion_rate",
        fisc_year as "fisc_year",
        fisc_qrtr as "fisc_qrtr",
        fisc_month as "fisc_month",
        fisc_month_num as "fisc_month_num",
        fisc_month_name as "fisc_month_name",
        fisc_wk_num as "fisc_wk_num",
        fisc_month_wk_num as "fisc_month_wk_num",
        fisc_month_day as "fisc_month_day",
        cal_year as "cal_year",
        cal_qrtr as "cal_qrtr",
        cal_month as "cal_month",
        cal_month_num as "cal_month_num",
        cal_month_name as "cal_month_name",
        cal_wk_num as "cal_wk_num",
        cal_mnth_wk_num as "cal_mnth_wk_num",
        cal_mnth_day as "cal_mnth_day",
        prod_hier_l1 as "prod_hier_l1",
        prod_hier_l2 as "prod_hier_l2",
        prod_hier_l3 as "prod_hier_l3",
        prod_hier_l4 as "prod_hier_l4",
        prod_hier_l5 as "prod_hier_l5",
        prod_hier_l6 as "prod_hier_l6",
        prod_hier_l7 as "prod_hier_l7",
        prod_hier_l8 as "prod_hier_l8",
        prod_hier_l9 as "prod_hier_l9",
        lcl_prod_nm as "lcl_prod_nm"
    from source
)
select * from final