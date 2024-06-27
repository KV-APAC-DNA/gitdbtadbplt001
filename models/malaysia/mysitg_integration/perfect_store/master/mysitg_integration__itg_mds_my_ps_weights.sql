{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook="{% if is_incremental() %}
        DELETE FROM {{this}} WHERE (SELECT COUNT(*) FROM {{source('myssdl_raw','sdl_mds_my_ps_weights')}}) != 0;
        {% endif %}"
    )
}}

WITH sources
AS (
    SELECT *
    FROM {{source('myssdl_raw', 'sdl_mds_my_ps_weights') }}
    ),
final
AS (
    SELECT kpi::VARCHAR(100) AS kpi,
        name::VARCHAR(255) AS name,
        weight::NUMBER(20,4) AS weight,
        channel::VARCHAR(50) AS channel,
        convert_timezone('Asia/Singapore',current_timestamp()::timestamp_ntz(9)) as crtd_dttm
    FROM sources
    )
SELECT *
FROM final