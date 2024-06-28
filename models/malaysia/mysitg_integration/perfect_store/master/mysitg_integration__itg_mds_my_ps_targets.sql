{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook="{% if is_incremental() %}
        DELETE FROM {{this}} WHERE (SELECT COUNT(*) FROM {{source('myssdl_raw','sdl_mds_my_ps_targets')}}) != 0;
        {% endif %}"
    )
}}

WITH sources
AS (
    SELECT *
    FROM {{source('myssdl_raw', 'sdl_mds_my_ps_targets') }}
    ),
final
AS (
    SELECT kpi::VARCHAR(50) as kpi,
        re::VARCHAR(200) as re,
        attribute_1::VARCHAR(50) as attribute_1,
        attribute_2::VARCHAR(100) as attribute_2,
        channel::VARCHAR(100) as channel,
        value::NUMBER(20,4) as target ,
        convert_timezone('Asia/Singapore',current_timestamp()::timestamp_ntz(9)) as crtd_dttm
    FROM sources
    )
SELECT *
FROM final