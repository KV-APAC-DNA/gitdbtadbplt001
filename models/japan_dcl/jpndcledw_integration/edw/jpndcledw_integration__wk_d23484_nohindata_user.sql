{{
    config
    (
        pre_hook = "{% if build_month_end_job_models()  %}
                    truncate table {{ source('jpdcledw_integration', 'wk_rankdt_tmp') }};
                    {% endif %}",
        post_hook = "{% if build_month_end_job_models()  %}
                    {{wk_rankdt_fun()}}
                    {% endif %}"
    )
}}


{% if build_month_end_job_models()  %}

with wk_d22687_ruikei
as (
  select *
  from {{ ref('jpndcledw_integration__wk_d22687_ruikei') }}
  ),
wk_d22687_2021nen_sumi
as (
  select *
  from {{ ref('jpndcledw_integration__wk_d22687_2021nen_sumi') }}
  ),
transformed
as (
  select distinct usrid
  from wk_d22687_ruikei
  
  union
  
  select distinct diecusrid
  from wk_d22687_2021nen_sumi
  ),
final
as (
  select usrid::number(38, 0) as usrid
  from transformed
  )
select *
from final
{% else %}
    select * from {{this}}
{% endif %}