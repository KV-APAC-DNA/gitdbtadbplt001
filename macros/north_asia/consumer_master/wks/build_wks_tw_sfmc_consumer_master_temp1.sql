
{% macro build_wks_tw_sfmc_consumer_master_temp1(filename) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Setting query to build wks_tw_sfmc_consumer_master_temp1: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set build_wks_tw_sfmc_consumer_master_temp1_query %}
        create or replace table
        {% if target.name=='prod' %}
                ntawks_integration.wks_tw_sfmc_consumer_master_temp1
            {% else %}
                {{schema}}.ntawks_integration__wks_tw_sfmc_consumer_master_temp1
            {% endif %}
        as (
        with itg_sfmc_consumer_master_temp as
        (
            select * from
            {% if target.name=='prod' %}
                ntaitg_integration.itg_sfmc_consumer_master_temp
            {% else %}
                {{schema}}.ntaitg_integration__itg_sfmc_consumer_master_temp
            {% endif %}
            where cntry_cd = 'TW'
        ),
        sdl_tw_sfmc_consumer_master as (
            select * from {{ ref('ntawks_integration__wks_sdl_tw_sfmc_consumer_master') }}
        ),
        final as
        (
            select
                temp.subscriber_key as subscriber_key,
                temp.md5_sdl as md5_sdl,
                temp.md5_itg as md5_itg,
                case
                    when temp.md5_sdl = temp.md5_itg then 'NO_CHANGE'
                    else 'UPDATED'
                end as compare
            from
                (
                    select itg.subscriber_key,
                        md5(nvl(lower(itg.subscriber_key), '') || nvl(lower(last_logon_time), '') || lower(tier)) as md5_itg,
                        md5_sdl
                    from itg_sfmc_consumer_master_temp itg,
                        (
                            select sdl.subscriber_key,
                                md5(nvl(lower(sdl.subscriber_key), '') || nvl(LOWER(last_logon_time), '') || lower(tier)) as md5_sdl
                            from sdl_tw_sfmc_consumer_master sdl
                            where file_name = '{{filename}}'
                        ) as sdl
                    where sdl.subscriber_key = itg.subscriber_key
                        and itg.valid_to = '31-DEC-9999'::timestamp_ntz
                        and ITG.cntry_cd = 'TW'
                ) temp
        )
        select * from final
        );

    {% endset %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Set the query to build wks_tw_sfmc_consumer_master_temp1 for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}

    {{ log("Started building model wks_tw_sfmc_consumer_master_temp1 for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(build_wks_tw_sfmc_consumer_master_temp1_query) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Ended building model wks_tw_sfmc_consumer_master_temp1 for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}

{% endmacro %}