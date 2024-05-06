{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "DELETE FROM {{this}} WHERE to_date(start_time) IN (SELECT DISTINCT TO_DATE(SUBSTRING(start_time,0,23),'DD/MM/YYYY HH12:MI:SS AM')
                    FROM {{ source('pcfsdl_raw','sdl_perenso_diary_item') }});"
    )
}}

with sdl_perenso_diary_item as (
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_diary_item') }}
),
final as (
    SELECT diary_item_key::number(10,0) as diary_item_key,
        diary_item_type_key::number(10,0) as diary_item_type_key,
        TO_TIMESTAMP(SUBSTRING(START_TIME,0,23),'DD/MM/YYYY HH12:MI:SS AM') as start_time,
        TO_TIMESTAMP(SUBSTRING(end_time,0,23),'DD/MM/YYYY HH12:MI:SS AM') as end_time,
        acct_key::number(10,0) as acct_key,
        acct_key_1::number(10,0) as acct_key_1,
        create_user_key::number(10,0) as create_user_key,
        complete::varchar(5) as complete,
        run_id::number(14,0) as run_id,
        current_timestamp()::timestamp_ntz(9) as create_dt,
        current_timestamp()::timestamp_ntz(9) as update_dt
    FROM sdl_perenso_diary_item
)
select * from final