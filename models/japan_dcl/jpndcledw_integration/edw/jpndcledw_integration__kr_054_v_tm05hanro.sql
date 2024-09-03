{% if build_month_end_job_models()  %}
with tbecsalesroutemst
as (
    select *
    from  {{ ref('jpndclitg_integration__tbecsalesroutemst') }}
    ),
c1
as (
    select dirouteid as hanrocode,
        dsroutename as hanroname,
        '' as konyudaibuncode,
        '' as konyuchubuncode,
        '' as konyusyobuncode,
        '' as konyusaibuncode
    from tbecsalesroutemst
    ),
final
as (
    select hanrocode::number(38, 0) as hanrocode,
        hanroname::varchar(48) as hanroname,
        konyudaibuncode::varchar(1) as konyudaibuncode,
        konyuchubuncode::varchar(1) as konyuchubuncode,
        konyusyobuncode::varchar(1) as konyusyobuncode,
        konyusaibuncode::varchar(1) as konyusaibuncode
    from c1
    )
select *
from final
{% else %}
    select * from {{this}}
{% endif %}