{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where (year,upper(target_type) , upper(target_category_code) , upper(brand)) in (select trim(year_code), upper(trim(target_type_code) ), upper(trim(target_category_code)), upper(nvl(trim(brand_name_code),'NA')) from {{ source('ntasdl_raw', 'sdl_mds_kr_tp_target') }});
        {% endif %}"
    )
}}

with source as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_tp_target') }}
),
cte1 as (
    select 
        'KR' as cntry_cd,
        'South Korea' as country_name,
        'KRW' as crncy_cd,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code) as sales_group_cd,
        trim(sales_group_name) as sales_group_name,
        upper(trim(target_type_code)) as target_type,
        cast(trim(year_code) as int) as year,
        cast(trim(year_code) || '-01-15' as date) as tgt_date,
        cast(ytd_target as numeric(18, 6)) as ytd_target_fy,
        nvl(cast(jan as numeric(18, 6)), 0) as tgt_value,
        null as filename,
        null as run_id,
        convert_timezone('UTC', current_timestamp()) as crtd_dttm,
        nvl(trim(brand_name_code), 'NA') as brand,
        trim(target_category_code) as target_category_code,
        trim(target_category_name) as target_category,
        nvl(jan_value, 0) as target_amt,
        nvl (ytd_target_value, 0) as ytd_target_amt
    from source
),
cte2 as (
    select 
        'KR' as cntry_cd,
        'South Korea' as country_name,
        'KRW' as crncy_cd,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code) as sales_group_cd,
        trim(sales_group_name) as sales_group_name,
        upper(trim(target_type_code)) as target_type,
        cast(trim(year_code) as int) as year,
        cast(trim(year_code) || '-02-15' as date) as tgt_date,
        cast(ytd_target as numeric(18, 6)) as ytd_target_fy,
        nvl(cast(feb as numeric(18, 6)), 0) as tgt_value,
        null as filename,
        null as run_id,
        convert_timezone('UTC', current_timestamp()) as crtd_dttm,
        nvl(trim(brand_name_code), 'NA') as brand,
        trim(target_category_code) as target_category_code,
        trim(target_category_name) as target_category,
        nvl(feb_value, 0) as target_amt,
        nvl (ytd_target_value, 0) as ytd_target_amt
    from source
),
cte3 as (
    select 
        'KR' as cntry_cd,
        'South Korea' as country_name,
        'KRW' as crncy_cd,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code) as sales_group_cd,
        trim(sales_group_name) as sales_group_name,
        upper(trim(target_type_code)) as target_type,
        cast(trim(year_code) as int) as year,
        cast(trim(year_code) || '-03-15' as date) as tgt_date,
        cast(ytd_target as numeric(18, 6)) as ytd_target_fy,
        nvl(cast(mar as numeric(18, 6)), 0) as tgt_value,
        null as filename,
        null as run_id,
        convert_timezone('UTC', current_timestamp()) as crtd_dttm,
        nvl(trim(brand_name_code), 'NA') as brand,
        trim(target_category_code) as target_category_code,
        trim(target_category_name) as target_category,
        nvl(mar_value, 0) as target_amt,
        nvl (ytd_target_value, 0) as ytd_target_amt
    from source
),
cte4 as (
    select 
        'KR' as cntry_cd,
        'South Korea' as country_name,
        'KRW' as crncy_cd,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code) as sales_group_cd,
        trim(sales_group_name) as sales_group_name,
        upper(trim(target_type_code)) as target_type,
        cast(trim(year_code) as int) as year,
        cast(trim(year_code) || '-04-15' as date) as tgt_date,
        cast(ytd_target as numeric(18, 6)) as ytd_target_fy,
        nvl(cast(apr as numeric(18, 6)), 0) as tgt_value,
        null as filename,
        null as run_id,
        convert_timezone('UTC', current_timestamp()) as crtd_dttm,
        nvl(trim(brand_name_code), 'NA') as brand,
        trim(target_category_code) as target_category_code,
        trim(target_category_name) as target_category,
        nvl(apr_value, 0) as target_amt,
        nvl (ytd_target_value, 0) as ytd_target_amt
    from source
),
cte5 as (
    select 
        'KR' as cntry_cd,
        'South Korea' as country_name,
        'KRW' as crncy_cd,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code) as sales_group_cd,
        trim(sales_group_name) as sales_group_name,
        upper(trim(target_type_code)) as target_type,
        cast(trim(year_code) as int) as year,
        cast(trim(year_code) || '-05-15' as date) as tgt_date,
        cast(ytd_target as numeric(18, 6)) as ytd_target_fy,
        nvl(cast(may as numeric(18, 6)), 0) as tgt_value,
        null as filename,
        null as run_id,
        convert_timezone('UTC', current_timestamp()) as crtd_dttm,
        nvl(trim(brand_name_code), 'NA') as brand,
        trim(target_category_code) as target_category_code,
        trim(target_category_name) as target_category,
        nvl(may_value, 0) as target_amt,
        nvl (ytd_target_value, 0) as ytd_target_amt
    from source
),
cte6 as (
    select 
        'KR' as cntry_cd,
        'South Korea' as country_name,
        'KRW' as crncy_cd,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code) as sales_group_cd,
        trim(sales_group_name) as sales_group_name,
        upper(trim(target_type_code)) as target_type,
        cast(trim(year_code) as int) as year,
        cast(trim(year_code) || '-06-15' as date) as tgt_date,
        cast(ytd_target as numeric(18, 6)) as ytd_target_fy,
        nvl(cast(jun as numeric(18, 6)), 0) as tgt_value,
        null as filename,
        null as run_id,
        convert_timezone('UTC', current_timestamp()) as crtd_dttm,
        nvl(trim(brand_name_code), 'NA') as brand,
        trim(target_category_code) as target_category_code,
        trim(target_category_name) as target_category,
        nvl(jun_value, 0) as target_amt,
        nvl (ytd_target_value, 0) as ytd_target_amt
    from source
),
cte7 as (
    select 
        'KR' as cntry_cd,
        'South Korea' as country_name,
        'KRW' as crncy_cd,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code) as sales_group_cd,
        trim(sales_group_name) as sales_group_name,
        upper(trim(target_type_code)) as target_type,
        cast(trim(year_code) as int) as year,
        cast(trim(year_code) || '-07-15' as date) as tgt_date,
        cast(ytd_target as numeric(18, 6)) as ytd_target_fy,
        nvl(cast(jul as numeric(18, 6)), 0) as tgt_value,
        null as filename,
        null as run_id,
        convert_timezone('UTC', current_timestamp()) as crtd_dttm,
        nvl(trim(brand_name_code), 'NA') as brand,
        trim(target_category_code) as target_category_code,
        trim(target_category_name) as target_category,
        nvl(jul_value, 0) as target_amt,
        nvl (ytd_target_value, 0) as ytd_target_amt
    from source
),
cte8 as (
    select 
        'KR' as cntry_cd,
        'South Korea' as country_name,
        'KRW' as crncy_cd,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code) as sales_group_cd,
        trim(sales_group_name) as sales_group_name,
        upper(trim(target_type_code)) as target_type,
        cast(trim(year_code) as int) as year,
        cast(trim(year_code) || '-08-15' as date) as tgt_date,
        cast(ytd_target as numeric(18, 6)) as ytd_target_fy,
        nvl(cast(aug as numeric(18, 6)), 0) as tgt_value,
        null as filename,
        null as run_id,
        convert_timezone('UTC', current_timestamp()) as crtd_dttm,
        nvl(trim(brand_name_code), 'NA') as brand,
        trim(target_category_code) as target_category_code,
        trim(target_category_name) as target_category,
        nvl(aug_value, 0) as target_amt,
        nvl (ytd_target_value, 0) as ytd_target_amt
    from source
),
cte9 as (
    select 
        'KR' as cntry_cd,
        'South Korea' as country_name,
        'KRW' as crncy_cd,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code) as sales_group_cd,
        trim(sales_group_name) as sales_group_name,
        upper(trim(target_type_code)) as target_type,
        cast(trim(year_code) as int) as year,
        cast(trim(year_code) || '-09-15' as date) as tgt_date,
        cast(ytd_target as numeric(18, 6)) as ytd_target_fy,
        nvl(cast(sep as numeric(18, 6)), 0) as tgt_value,
        null as filename,
        null as run_id,
        convert_timezone('UTC', current_timestamp()) as crtd_dttm,
        nvl(trim(brand_name_code), 'NA') as brand,
        trim(target_category_code) as target_category_code,
        trim(target_category_name) as target_category,
        nvl(sep_value, 0) as target_amt,
        nvl (ytd_target_value, 0) as ytd_target_amt
    from source
),
cte10 as (
    select 
        'KR' as cntry_cd,
        'South Korea' as country_name,
        'KRW' as crncy_cd,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code) as sales_group_cd,
        trim(sales_group_name) as sales_group_name,
        upper(trim(target_type_code)) as target_type,
        cast(trim(year_code) as int) as year,
        cast(trim(year_code) || '-10-15' as date) as tgt_date,
        cast(ytd_target as numeric(18, 6)) as ytd_target_fy,
        nvl(cast(oct as numeric(18, 6)), 0) as tgt_value,
        null as filename,
        null as run_id,
        convert_timezone('UTC', current_timestamp()) as crtd_dttm,
        nvl(trim(brand_name_code), 'NA') as brand,
        trim(target_category_code) as target_category_code,
        trim(target_category_name) as target_category,
        nvl(oct_value, 0) as target_amt,
        nvl (ytd_target_value, 0) as ytd_target_amt
    from source
),
cte11 as (
    select 
        'KR' as cntry_cd,
        'South Korea' as country_name,
        'KRW' as crncy_cd,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code) as sales_group_cd,
        trim(sales_group_name) as sales_group_name,
        upper(trim(target_type_code)) as target_type,
        cast(trim(year_code) as int) as year,
        cast(trim(year_code) || '-11-15' as date) as tgt_date,
        cast(ytd_target as numeric(18, 6)) as ytd_target_fy,
        nvl(cast(nov as numeric(18, 6)), 0) as tgt_value,
        null as filename,
        null as run_id,
        convert_timezone('UTC', current_timestamp()) as crtd_dttm,
        nvl(trim(brand_name_code), 'NA') as brand,
        trim(target_category_code) as target_category_code,
        trim(target_category_name) as target_category,
        nvl(nov_value, 0) as target_amt,
        nvl (ytd_target_value, 0) as ytd_target_amt
    from source
),
cte12 as (
    select 
        'KR' as cntry_cd,
        'South Korea' as country_name,
        'KRW' as crncy_cd,
        trim(channel_code) as channel,
        trim(store_type_code) as store_type,
        trim(sales_group_code) as sales_group_cd,
        trim(sales_group_name) as sales_group_name,
        upper(trim(target_type_code)) as target_type,
        cast(trim(year_code) as int) as year,
        cast(trim(year_code) || '-12-15' as date) as tgt_date,
        cast(ytd_target as numeric(18, 6)) as ytd_target_fy,
        nvl(cast(dec as numeric(18, 6)), 0) as tgt_value,
        null as filename,
        null as run_id,
        convert_timezone('UTC', current_timestamp()) as crtd_dttm,
        nvl(trim(brand_name_code), 'NA') as brand,
        trim(target_category_code) as target_category_code,
        trim(target_category_name) as target_category,
        nvl(dec_value, 0) as target_amt,
        nvl (ytd_target_value, 0) as ytd_target_amt
    from source
),
transformed as (
    select * from cte1
    union all
    select * from cte2
    union all
    select * from cte3
    union all
    select * from cte4
    union all
    select * from cte5
    union all
    select * from cte6
    union all
    select * from cte7
    union all
    select * from cte8
    union all
    select * from cte9
    union all
    select * from cte10
    union all
    select * from cte11
    union all
    select * from cte12
),
final as (
    select
        cntry_cd::varchar(10) as cntry_cd,
        country_name::varchar(30) as country_name,
        crncy_cd::varchar(10) as crncy_cd,
        channel::varchar(50) as channel,
        store_type::varchar(100) as store_type,
        sales_group_cd::varchar(10) as sales_group_cd,
        sales_group_name::varchar(100) as sales_group_name,
        target_type::varchar(10) as target_type,
        year::number(18,0) as year,
        tgt_date::date as tgt_date,
        ytd_target_fy::number(18,6) as ytd_target_fy,
        tgt_value::number(18,6) as tgt_value,
        filename::varchar(100) as filename,
        run_id::number(14,0) as run_id,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        brand::varchar(200) as brand,
        target_category_code::varchar(100) as target_category_code,
        target_category::varchar(100) as target_category,
        target_amt::number(31,3) as target_amt,
        ytd_target_amt::number(31,3) as ytd_target_amt
    from transformed
)
select * from final