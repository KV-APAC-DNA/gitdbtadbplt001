{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['store_chk_hdr_key','over_and_above_key'],
        pre_hook= "DELETE FROM {{this}} WHERE start_time IN (SELECT DISTINCT TO_DATE(SUBSTRING(start_time,0,23),'DD/MM/YYYY HH12:MI:SS AM')
                    FROM {{'pcfsdl_raw','sdl_perenso_diary_item'}});"
    )
}}


with sdl_perenso_diary_item as (
select * from {{ source('pcfsdl_raw', 'sdl_perenso_diary_item') }}
),
final as (
SELECT diary_item_key::number(10,0),

       diary_item_type_key::number(10,0),

       TO_TIMESTAMP(SUBSTRING(START_TIME,0,23),'DD/MM/YYYY HH12:MI:SS AM') as start_time,
       TO_TIMESTAMP(SUBSTRING(end_time,0,23),'DD/MM/YYYY HH12:MI:SS AM') as start_time,

       acct_key::number(10,0),

       acct_key_1::number(10,0),

       create_user_key::number(10,0),

       complete::varchar(5),

       run_id::number(14,0),

       current_timestamp()::timestamp_ntz(9) as create_dt

FROM sdl_perenso_diary_item
)
select * from final