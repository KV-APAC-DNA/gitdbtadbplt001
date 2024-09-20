{{
    config
    (
        materialized = 'incremental',
        incremental_logic = 'append'
    )
}}

with report_006_a
as (
    select * from {{ ref('jpndcledw_integration__report_006_a') }}
    ),
cld_m
as (
    select * from {{ ref('jpndcledw_integration__cld_m') }}
    ),
transformed
as (
    select
        channel_name as channel_name,
        channel_id as channel_id,
        yymm as yymm,
        "ユニーク契約者数" as total,
        cast(date_part(dow, convert_timezone('Asia/Tokyo', current_timestamp()) - interval '7 day') as int) as day_of_week,
        cast(convert_timezone('Asia/Tokyo', current_timestamp()) as date) - interval '7 day' as report_exec_date
    from report_006_a
    where yymm =
        (
            select
                year_445 || lpad(month_445, 2, 0)
            from cld_m
            where to_date(ymd_dt) = to_date(dateadd(day, - 7, convert_timezone('Asia/Tokyo', current_timestamp())))
        )
    ),

final as
(
    select
        channel_name::varchar(6) as channel_name,
    	channel_id::varchar(6) as channel_id,
    	yymm::varchar(100) as yymm,
    	total::number(38,0) as total,
    	day_of_week::number(38,0) as day_of_week,
    	report_exec_date::date as report_exec_date
    from transformed
)
select * from final