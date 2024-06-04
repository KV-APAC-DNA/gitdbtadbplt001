{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where 0 != (
        select count(*)
        from {{ source('idnsdl_raw', 'sdl_mds_id_dist_reporting_control') }}
        where upper(trim(sourcetype)) = 'SALES'
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
    select distributor_cd,
            source_system,
            case
                when refresh_from is not null then refresh_from
                else effective_from
            end as effective_from,
            case
                when effective_to is not null then effective_to
                else (
                    select distinct jj_mnth_id
                    from edw_time_dim
                    where (cal_date) = current_timestamp()::date
                )
            end as effective_to,
            source_type,
            crtd_dttm
        from (
                select distinct upper(trim(distributorcode)) as distributor_cd,
                    upper(trim(sourcesystem)) as source_system,
                    cast(trim(effectivefrom) as numeric) as effective_from,
                    cast(trim(effectiveto) as numeric) as effective_to,
                    cast(trim(refreshfrom) as numeric) as refresh_from,
                    upper(trim(sourcetype)) as source_type,
                    convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as crtd_dttm
                from source
                where upper(trim(sourcetype)) = 'SALES'
                order by upper(trim(distributorcode)),
                    cast(trim(effectivefrom) as numeric),
                    cast(trim(refreshfrom) as numeric)
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
	crtd_dttm::timestamp_ntz(9) as crtd_dttm
    from trans
    order by distributor_cd,
    effective_from
)
select * from final