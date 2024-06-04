{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where 0 != (
        select count(*)
        from {{ source('idnsdl_raw', 'sdl_mds_id_dist_reporting_control') }}
        where upper(trim(sourcetype)) = 'STOCK'
    );
    {% endif %}"
    )
}}
with source as 
(
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_dist_reporting_control') }}
),
edw_time_dim as 
(
    select * from {{ source('idnedw_integration', 'edw_time_dim') }}
),
trans as 
(
    SELECT distributor_cd,
            source_system,
            CASE
                WHEN refresh_from IS NOT NULL THEN refresh_from
                ELSE effective_from
            END AS effective_from,
            CASE
                WHEN effective_to IS NOT NULL THEN effective_to
                ELSE (
                    SELECT DISTINCT jj_mnth_id
                    FROM edw_time_dim
                    WHERE (cal_date) = current_timestamp()::date
                )
            END AS effective_to,
            source_type,
            crtd_dttm
        FROM (
                SELECT DISTINCT UPPER(TRIM(distributorcode)) AS distributor_cd,
                    UPPER(TRIM(sourcesystem)) AS source_system,
                    CAST(TRIM(effectivefrom) AS NUMERIC) AS effective_from,
                    CAST(TRIM(effectiveto) AS NUMERIC) AS effective_to,
                    CAST(TRIM(refreshfrom) AS NUMERIC) AS refresh_from,
                    UPPER(TRIM(sourcetype)) AS source_type,
                    convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) AS crtd_dttm
                FROM source
                WHERE UPPER(TRIM(sourcetype)) = 'STOCK'
                ORDER BY UPPER(TRIM(distributorcode)),
                    CAST(TRIM(effectivefrom) AS NUMERIC),
                    CAST(TRIM(refreshfrom) AS NUMERIC)
            )
    
),
final as
(
    select distinct 
    distributor_cd::varchar(30) as distributor_cd,
	source_system::varchar(255) as source_system,
	effective_from::number(31,0) as effective_from,
	effective_to::number(31,0) as effective_to,
	source_type::varchar(30) as source_type,
	crtd_dttm as crtd_dttm
    from trans
    order by distributor_cd,
    effective_from
)
select * from final