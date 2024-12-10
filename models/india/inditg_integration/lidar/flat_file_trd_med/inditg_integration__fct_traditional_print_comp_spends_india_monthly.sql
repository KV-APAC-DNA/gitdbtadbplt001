{{ config(
   materialized='incremental',
  incremental_strategy='append',
  transient=false,
  post_hook="{{ get_harmonized_brand('TRADITIONAL_PRINT_COMP_SPEND', 'Y') }}"
) }}

with curr_rate as (
    select
        date,
        avg(rate) as rate
    from {{ source('paidmedia_integration','fct_currency_rate_global_daily') }}
    where currency_code = 'INR'
    group by date
)

select
    medium,
    category,
    comp,
    compttn,
    yr_mth as month,
    cast(week as integer) as week,
    concate,
    super_category,
    product_group,
    advertiser,
    product,
    ad_main_type,
    ad_sub_type,
    parent_publication,
    publication,
    supplementary,
    cast(pageno as integer) as pageno,
    page_title,
    position,
    ad_type,
    ad_language,
    location,
    page_side,
    pub_nature,
    pub_group,
    pub_language,
    pub_periodicity,
    pub_genre,
    zone,
    state,
    edition,
    sales_promo,
    innovation,
    festival,
    agency,
    cast(col as integer) as col,
    cast(cm as integer) as cm,
    cast(ad_cm as integer) as ad_cm,
    cast(vol_cc as integer) as vol_cc,
    cast(vol_sqcm as integer) as vol_sqcm,
    cast(tam_cost as integer) as tam_cost,
    cast(yr as integer) as yr,
    day,
    cast(house_ads as integer) as house_ads,
    cast(ads as integer) as ads,
    cast(pagetag as integer) as pagetag,
    discounting_factor,
    file_name,
    null as gcph_brand,
    null as brand_harmonized_by,
    'India' as gcgh_country,
    to_date(cs.date, 'DD/MM/YYYY') as "DATE",
    floor(cast(tam_cost as integer) / er.rate, 2) as tam_cost_usd,
    floor(cast(adjusted_cost_in_cr as float), 2) as adjusted_cost_in_cr,
    floor(cast(adjusted_cost_in_cr as float) / er.rate, 2)
        as adjusted_cost_in_usd,
    to_timestamp(current_timestamp()) as crt_dttm,
    to_timestamp(current_timestamp()) as updt_dttm
from {{ source('indsdl_raw','sdl_lidar_ff_print_comp_spend') }} as cs
left join curr_rate as er
    on to_date(cs.date, 'DD/MM/YYYY') = er.date
{% if is_incremental() %}
    where to_date(cs.date, 'DD/MM/YYYY') > (select max("DATE") from {{ this }})
{% endif %}
