{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
		pre_hook="{% if is_incremental() %}
        delete from {{this}} WHERE (upper(kpi),upper(datatype),coalesce(upper(cluster), 'NA'),coalesce(upper(market), 'NA'),coalesce(upper(segment), 'NA'),coalesce(upper(brand), 'NA'),coalesce(upper(yearmonth), 'NA'),year,coalesce(quarter, 9999),coalesce(upper(target_type), 'NA')) IN (
        SELECT upper(kpi),upper(datatype),coalesce(upper(cluster), 'NA'),coalesce(upper(market), 'NA'),coalesce(upper(segment), 'NA'),coalesce(upper(brand), 'NA'),coalesce(upper(yearmonth), 'NA'),year,coalesce(quarter, 9999),coalesce(upper(target_type), 'NA')
        FROM {{source('aspsdl_raw', 'sdl_okr_alteryx_automation')}} WHERE filename = '/File_Name');
        {% endif %}"
    )
}}
with sdl_okr_alteryx_automation as
(
    select * from {{source('aspsdl_raw', 'sdl_okr_alteryx_automation')}}
),
final as
(
    select
        kpi::varchar(50) as kpi,
	    datatype::varchar(20) as datatype,
	    cluster::varchar(50) as cluster,
	    market::varchar(50) as market,
	    segment::varchar(50) as segment,
	    brand::varchar(100) as brand,
	    yearmonth::varchar(20) as yearmonth,
	    year::number(38,0) as year,
	    quarter::number(38,0) as quarter,
	    actuals::number(32,4) as actuals,
	    target::number(32,4) as target,
	    target_type::varchar(20) as target_type,
	    filename::varchar(50) as filename,
	    run_id::varchar(14) as run_id,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
    from sdl_okr_alteryx_automation
)
select * from final