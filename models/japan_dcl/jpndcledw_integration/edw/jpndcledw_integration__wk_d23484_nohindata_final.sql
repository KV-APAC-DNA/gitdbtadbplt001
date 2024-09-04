{% if build_month_end_job_models()  %}
{{
    config(
        post_hook = ["update {{this}} set sum = b.sum from (
    select usrid, rankdt, sum(price) as sum from {{ ref('jpndcledw_integration__wk_d22687_ruikei') }} group by usrid, rankdt order by usrid)b
    where {{this}}.usrid = b.usrid and {{this}}.rankdt = b.rankdt;",
    
    "update {{this}} set point = b.point from (
    select diecusrid as usrid,point as point from {{ ref('jpndcledw_integration__wk_d22687_2021nen_sumi') }}) b where {{this}}.usrid = b.usrid;",
    
    "update {{this}} set usrid = cast(ltrim({{encryption_1('usrid')}}, '0')as bigint);"]
    )
}}
with wk_rankdt_tmp 
as (
    select *
    from {{ source('jpdcledw_integration', 'wk_rankdt_tmp') }}
    ),
wk_d23484_nohindata_user
as (
    select *
    from {{ ref('jpndcledw_integration__wk_d23484_nohindata_user') }}
    ),
transformed
as (
    select usrid as usrid,
        0 as point,
        rankdt,
        0 as price
    from wk_rankdt_tmp a,
        wk_d23484_nohindata_user b
    ),
final
as (
    select usrid::number(38,0) as usrid,
        point::number(18,0) as point,
        rankdt::varchar(9) as rankdt,
        price::number(38,0) as sum
    from transformed
    )
select *
from final  
{% else %}
    select * from {{this}}
{% endif %}