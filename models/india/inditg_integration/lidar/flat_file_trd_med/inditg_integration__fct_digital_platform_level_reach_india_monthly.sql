{{ config(
   materialized='incremental',
  incremental_strategy='append',
  transient=false,
  post_hook="{{ get_harmonized_brand('TRAD_PLATFORMLEVEL_REACH', 'Y') }}"
) }}

with daily_exchange_rate as (
    select
        i.*,
        1 / AVG(t.rate) as rate
    from
        {{ source("paidmedia_integration","fct_currency_rate_global_daily") }}
            as t
        join
        (select distinct
            CAST(month_filter as date) as month_filter,
            CAST(month_filter as date) as start_date,
            LAST_DAY(start_date, 'month') as end_date
        from {{ source(
      "indsdl_raw",
      "sdl_lidar_ff_platformlevel_reach"
    ) }}) as i
        on t.date = i.start_date
    where t.date between i.start_date and i.end_date
    group by month_filter, start_date, end_date
    order by 2, 1
)

select
    report_name,
    data_source,
    mergeid,
    country,
    platform,
    brand_name,
    subbrand,
    campaign_phase,
    market_name,
    market_cluster,
    campaign_name_new,
    hva_tg_name,
    ad_type,
    buy_type,
    campaign_month,
    reach_source_file,
    currency,
    account_name,
    partner,
    advertiser,
    campaign_taxonomy,
    io_ad_set_name_taxonomy,
    line_item_type,
    media_type,
    buying_type,
    objective,
    de.rate as exchange_rate,
    NULL as gcph_brand,
    'India' as gcgh_country,
    NULL as brand_harmonized_by,
    load_date,
    CAST(src.month_filter as date) as month_filter,
    CAST(reporting_starts as date) as reporting_starts,
    CAST(reporting_ends as date) as reporting_ends,
    CAST(campaign_start as date) as campaign_start,
    CAST(campaign_end as date) as campaign_end,
    CAST(account_id as integer) as account_id,
    CAST(partner_id as integer) as partner_id,
    CAST(advertiser_id as integer) as advertiser_id,
    CAST(campaign_id as integer) as campaign_id,
    CAST(insertion_order_id as integer) as insertion_order_id,
    CAST(ad_set_id as integer) as ad_set_id,
    CAST(latest_update as date) as latest_update,
    CAST(reach as integer) as reach,
    CAST(estm_reach as float) as estm_reach,
    CAST(impressions as integer) as impressions,
    CAST(estm_impressions as float) as estm_impressions,
    CAST(spends as float) as spends,
    CAST(spends as float) * de.rate as spends_usd,
    CAST(estm_media_spends as float) as estm_media_spends,
    CAST(estm_media_spends as float) * de.rate as estm_media_spends_usd,
    CAST(clicks as integer) as clicks,
    CAST(estm_clicks as float) as estm_clicks,
    CAST(video_views_thruplays as integer) as video_views_thruplays,
    CAST(estm_video_views_thruplays as float) as estm_video_views_thruplays,
    CAST(quartile_video_views_1st as integer) as quartile_video_views_1st,
    CAST(quartile_video_views_2nd as integer) as quartile_video_views_2nd,
    CAST(quartile_video_views_3rd as integer) as quartile_video_views_3rd,
    CAST(quartile_video_views_4th as integer) as quartile_video_views_4th,
    CAST(trueview_views as integer) as trueview_views,
    CAST(thruplays as integer) as thruplays,
    CAST(second_video_plays_3 as integer) as second_video_plays_3,
    CAST(companion_views_video as integer) as companion_views_video,
    CAST(skips_video as integer) as skips_video,
    CAST(billable_impressions as float) as billable_impressions,
    CAST(media_cost_advertiser_currency as float)
        as media_cost_advertiser_currency,
    CAST(media_cost_advertiser_currency as float)
    * de.rate as media_cost_advertiser_currency_usd,
    CAST(clicks_all as integer) as clicks_all,
    CAST(page_engagement as integer) as page_engagement,
    CAST(post_engagements as integer) as post_engagements,
    CAST(follows_or_likes as float) as follows_or_likes
from
{{ source(
      "indsdl_raw",
      "sdl_lidar_ff_platformlevel_reach"
    ) }} as src
left join
    daily_exchange_rate as de
    on src.month_filter = de.month_filter
{% if is_incremental() %}
    where src.month_filter > (select MAX(month_filter) from {{ this }})
{% endif %}
