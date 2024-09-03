{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} a USING {{ ref('aspwks_integration__sdl_okr_alteryx_automation') }} b 
        where upper(a.datatype) = upper(b.datatype) 
        and upper(a.kpi) = upper(b.kpi)
        and nvl(a.cluster,'') =nvl(b.cluster,'')
        and nvl(a.market,'')=nvl(b.market,'')
        and nvl(a.segment,'') = nvl(b.segment,'')
        and nvl(a.brand,'')= nvl(b.brand,'')
        and nvl(a.yearmonth,'') = nvl(b.yearmonth,'')
        and nvl(a.target_type,'') = nvl(b.target_type,'')
        and a.year= b.year
        and nvl(a.quarter,9999) = nvl(b.quarter,9999)
        and replace(a.filename,'_'||split_part(a.filename,'_',4)) = replace(b.filename,'_'||split_part(b.filename,'_',4));
        {% endif %}"
    )
}}
with sdl_okr_alteryx_automation as(
    select * from {{ ref('aspwks_integration__sdl_okr_alteryx_automation') }}
),
final as(
select        
kpi::varchar(50) as kpi,
datatype::varchar(20) as datatype,
nullif(cluster,'')::varchar(50) as cluster,
nullif(market,'')::varchar(50) as market,
nullif(segment,'')::varchar(50) as segment,
nullif(brand,'')::varchar(100) as brand,
nullif(yearmonth,'')::varchar(20) as yearmonth,
year::number(38,0) as year,
quarter::number(38,0) as quarter,
actuals::number(32,4) as actuals,
target::number(32,4) as target,
nullif(target_type,'')::varchar(20) as target_type,
filename::varchar(50) as filename,
run_id::varchar(14) as run_id,
crt_dttm::timestamp_ntz(9) as crt_dttm,
convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
from sdl_okr_alteryx_automation
)
select * from final
