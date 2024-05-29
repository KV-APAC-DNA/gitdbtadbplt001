with vw_edw_coupang_product_ranking_daily as (
    select * from ntaedw_integration.vw_edw_coupang_product_ranking_daily
),
itg_kr_coupang_product_ranking_daily as(
    select * from ntaitg_integration.itg_kr_coupang_product_ranking_daily
),
itg_kr_coupang_product_summary_weekly as (
    select * from ntaitg_integration.itg_kr_coupang_product_summary_weekly
),
itg_kr_coupang_product_summary_monthly as (
    select * from ntaitg_integration.itg_kr_coupang_product_summary_monthly
),
cte1 as (
    SELECT 
        daily.product_ranking_date,
        daily.category_depth1,
        daily.category_depth2,
        daily.category_depth3,
        daily.all_brand,
        daily.coupang_sku_id,
        daily.coupang_sku_name,
        daily.ranking,
        prev_day.ranking AS prev_day_ranking,
        NULL::character varying AS prev_week_ranking,
        NULL::character varying AS prev_mon_ranking,
        (
            (((prev_day.ranking)::numeric)::numeric(18, 0))::numeric(38, 4) - (((daily.ranking)::numeric)::numeric(18, 0))::numeric(38, 4)
        ) AS rank_change,
        daily.data_granularity,
        daily.jnj_product_flag
    FROM (
            vw_edw_coupang_product_ranking_daily daily
            LEFT JOIN itg_kr_coupang_product_ranking_daily prev_day ON (
                (
                    (
                        (
                            (
                                (
                                    to_char(
                                        dateadd(
                                            day,
                                            (- (1)::bigint),
                                            (to_date(daily.product_ranking_date,'YYYYMMDD'))::timestamp without time zone
                                        ),
                                        ('YYYYMMDD'::character varying)::text
                                    ) = (prev_day.product_ranking_date)::text
                                )
                                AND (
                                    (daily.category_depth1)::text = (prev_day.category_depth1)::text
                                )
                            )
                            AND (
                                (daily.category_depth2)::text = (prev_day.category_depth2)::text
                            )
                        )
                        AND (
                            (daily.category_depth3)::text = (prev_day.category_depth3)::text
                        )
                    )
                    AND (
                        (daily.coupang_sku_id)::text = (prev_day.coupang_sku_id)::text
                    )
                )
            )
        )
),
cte2 as (
    SELECT 
        weekly.yearmo AS product_ranking_date,
        weekly.category_depth1,
        weekly.category_depth2,
        weekly.category_depth3,
        weekly.all_brand,
        weekly.coupang_sku_id,
        weekly.coupang_sku_name,
        weekly.ranking,
        NULL::character varying AS prev_day_ranking,
        prev_week.ranking AS prev_week_ranking,
        NULL::character varying AS prev_mon_ranking,
        (
            (((prev_week.ranking)::numeric)::numeric(18, 0))::numeric(38, 4) - (((weekly.ranking)::numeric)::numeric(18, 0))::numeric(38, 4)
        ) AS rank_change,
        'weekly'::character varying AS data_granularity,
        weekly.jnj_product_flag
    FROM (
            itg_kr_coupang_product_summary_weekly weekly
            LEFT JOIN itg_kr_coupang_product_summary_weekly prev_week ON (
                (
                    (
                        (
                            (
                                (
                                    to_char(
                                        dateadd(
                                            day,
                                            (- (7)::bigint),
                                            (to_date(weekly.yearmo,'YYYYMMDD'))::timestamp without time zone
                                        ),
                                        ('YYYYMMDD'::character varying)::text
                                    ) = (prev_week.yearmo)::text
                                )
                                AND (
                                    (weekly.category_depth1)::text = (prev_week.category_depth1)::text
                                )
                            )
                            AND (
                                (weekly.category_depth2)::text = (prev_week.category_depth2)::text
                            )
                        )
                        AND (
                            (weekly.category_depth3)::text = (prev_week.category_depth3)::text
                        )
                    )
                    AND (
                        (weekly.coupang_sku_id)::text = (prev_week.coupang_sku_id)::text
                    )
                )
            )
        )
),
cte3 as (
    SELECT 
        monthly.yearmo AS product_ranking_date,
        monthly.category_depth1,
        monthly.category_depth2,
        monthly.category_depth3,
        monthly.all_brand,
        monthly.coupang_sku_id,
        monthly.coupang_sku_name,
        monthly.ranking,
        NULL::character varying AS prev_day_ranking,
        NULL::character varying AS prev_week_ranking,
        prev_mon.ranking AS prev_mon_ranking,
        (
            (((prev_mon.ranking)::numeric)::numeric(18, 0))::numeric(38, 4) - (((monthly.ranking)::numeric)::numeric(18, 0))::numeric(38, 4)
        ) AS rank_change,
        'monthly'::character varying AS data_granularity,
        monthly.jnj_product_flag
    FROM (
            itg_kr_coupang_product_summary_monthly monthly
            LEFT JOIN itg_kr_coupang_product_summary_monthly prev_mon ON (
                (
                    (
                        (
                            (
                                (
                                    to_char(
                                        dateadd(
                                            month,
                                            (- (1)::bigint),
                                            (to_date(monthly.yearmo,'YYYYMMDD'))::timestamp without time zone
                                        ),
                                        ('YYYYMM'::character varying)::text
                                    ) = "substring"((prev_mon.yearmo)::text, 1, 6)
                                )
                                AND (
                                    (monthly.category_depth1)::text = (prev_mon.category_depth1)::text
                                )
                            )
                            AND (
                                (monthly.category_depth2)::text = (prev_mon.category_depth2)::text
                            )
                        )
                        AND (
                            (monthly.category_depth3)::text = (prev_mon.category_depth3)::text
                        )
                    )
                    AND (
                        (monthly.coupang_sku_id)::text = (prev_mon.coupang_sku_id)::text
                    )
                )
            )
        )
),
final as (
    select 
        product_ranking_date,
        category_depth1,
        category_depth2,
        category_depth3,
        all_brand,
        coupang_sku_id,
        coupang_sku_name,
        ranking,
        prev_day_ranking,
        prev_week_ranking,
        prev_mon_ranking,
        (rank_change)::numeric(38, 0) as rank_change,
        data_granularity,
        jnj_product_flag
    from (
        select * from cte1
        union all
        select * from cte2
        union all
        select * from cte3
    )
)
select * from final