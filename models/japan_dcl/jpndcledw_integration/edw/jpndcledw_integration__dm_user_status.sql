{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                    delete from {{this}} where kokyano in ( select kokyano from {{ ref('jpndcledw_integration__temp_kesai_016') }} );
                    {% endif %}",
        post_hook = "delete from {{this}} where kokyano not in (select kokyano from {{ ref('jpndcledw_integration__temp_existing_kokyano_list_016') }});"
    )
}}



with dm_user_status_tmp as(
    select * from {{ ref('jpndcledw_integration__dm_user_status_tmp') }}
),

temp_kesai_016 as(
    select * from {{ ref('jpndcledw_integration__temp_kesai_016') }}
),

final as(
    select 
        base::varchar(11) as base,
        kokyano::varchar(60) as kokyano,
        dt::timestamp_ntz(9) as dt,
        status::varchar(8) as status
    from dm_user_status_tmp
)
select * from final