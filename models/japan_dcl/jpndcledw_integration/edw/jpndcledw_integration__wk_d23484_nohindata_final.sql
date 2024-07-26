{{
    config(
        post_hook = ["update {{this}} set sum = b.sum from (
    select usrid, rankdt, sum(price) as sum from dev_dna_core.jpdcledw_integration.wk_d22687_ruikei group by usrid, rankdt order by usrid)b
    where {{this}}.usrid = b.usrid and {{this}}.rankdt = b.rankdt;",
    
    "update {{this}} set point = b.point from (
    select diecusrid as userid,point as point from dev_dna_core.jpdcledw_integration.wk_d22687_2021nen_sumi) b where {{this}}.usrid = b.userid;",
    
    "update {{this}} set usrid = cast(ltrim({{encryption_1('cast(usrid as varchar)')}}, '0')as bigint);"]
    )
}}
with wk_rankdt_tmp
as (
    select *
    from dev_dna_core.jpdcledw_integration.wk_rankdt_tmp
    ),
wk_d23484_nohindata_user
as (
    select *
    from dev_dna_core.jpdcledw_integration.wk_d23484_nohindata_user
    ),
transformed
as (
    select usrid as userid,
        0 as point,
        rankdt,
        0 as price
    from wk_rankdt_tmp a,
        wk_d23484_nohindata_user b
    ),
final
as (
    select userid::number(38,0) as usrid,
        point::number(18,0) as point,
        rankdt::varchar(9) as rankdt,
        price::number(38,0) as sum
    from transformed
    )
select *
from final  
