{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook =["{% if is_incremental() %}
        DELETE FROM {{this}} WHERE (UPPER(TRIM(mother_sku_name)),UPPER(TRIM(account_name)),UPPER(TRIM(re)),pos_dt) IN (SELECT DISTINCT UPPER(TRIM(mother_sku_name)),UPPER(TRIM(account_name)),UPPER(TRIM(re)),pos_dt FROM {{ source('indsdl_raw', 'sdl_pos_historical_btl') }});
        {% endif %}",
        "{% if is_incremental()%}
        delete from {{this}} itg where itg.filename  in (select sdl.filename from
        {{ source('indsdl_raw', 'sdl_pos_historical_btl') }} sdl)
        {% endif %}"
        ] 
    )
}}
with sdl_pos_historical_btl as 
(
    select * from {{ source('indsdl_raw', 'sdl_pos_historical_btl') }}
),
final as 
(
    select
        mother_sku_name::varchar(255) as mother_sku_name,
	    account_name::varchar(255) as account_name,
	    re::varchar(255) as re,
	    pos_dt::date as pos_dt,
        promos::number(38,6) as promos,
	    filename::varchar(100) as filename,
	    run_id::number(14,0) as run_id,
	    crt_dttm::timestamp_ntz(9) as crt_dttm
           
    from sdl_pos_historical_btl
)
select * from final
