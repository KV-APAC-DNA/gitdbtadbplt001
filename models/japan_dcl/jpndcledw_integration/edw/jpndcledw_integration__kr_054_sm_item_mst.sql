{% if build_month_end_job_models()  %}
with kr_054_cewq008mnth_p_mst
as (
    select *
    from  {{ ref('jpndcledw_integration__kr_054_cewq008mnth_p_mst') }}
    ),
c1
as (
    select distinct mst.sm_kb as sm_kb,
        mst.sm_nm as sm_nm
    from kr_054_cewq008mnth_p_mst mst
    ),
final
as (
    select sm_kb::number(38, 18) as sm_kb,
        sm_nm::varchar(48) as sm_nm
    from c1
    )
select *
from final
{% else %}
    select * from {{this}}
{% endif %}