{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where file_name in (select distinct file_name from {{ source('ntasdl_raw','sdl_kr_dads_naver_keyword_search_volume') }});
        {% endif %}"
)}}

with source as (
     select * from {{ source('ntasdl_raw','sdl_kr_dads_naver_keyword_search_volume') }} 
),
final as (
    select
        no::varchar(255) as no,
        keyword::varchar(255) as keyword,
        total_monthly_searches::varchar(255) as total_monthly_search_volume,
        monthly_desktop_search_volume::varchar(255) as monthly_desktop_search_volume,
        monthl_mobile_searches::varchar(255) as monthly_mobile_search_volume,
        average_daily_search_volume::varchar(255) as average_daily_search_volume,
        keyword_first_appearance_date::varchar(255) as keyword_first_appearance_date,
        keyword_rating::varchar(255) as keyword_class,
        adult_keywords::varchar(255) as keyword_for_adult,
        blogrecent_mnthly_publications::varchar(255) as blog_recent_mnthly_publications,
        blogtotal_period_pbliction_vol::varchar(255) as blog_total_publications,
        caferecent_monthly_issue::varchar(255) as cafe_recent_mnthly_publications,
        "Cafe_Total_Period_Issue "::varchar(255) as cafe_total_publications,
        view_recent_mnthly_publication::varchar(255) as view_recent_mnthly_publications,
        view_total_period_issuance::varchar(255) as view_total_publications,
        search_volume_until_yesterday::varchar(255) as search_volume_until_yesterday,
        search_vol_end_of_the_month::varchar(255) as search_volume_until_endofmonth,
        blog_saturation_index::varchar(255) as blog_saturation_index,
        "Cafe_Saturation_Index "::varchar(255) as cafe_saturation_index,
        view_saturation_index::varchar(255) as view_saturation_index,
        related_keywords::varchar(4000) as relative_keyword,
        file_name::varchar(255) as file_name,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm,
        file_date::varchar(10) as file_date
    from source
)
select * from final