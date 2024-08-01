with wk_so_planet_modified as(
    select * from {{ ref('jpnwks_integration__wk_so_planet_modified') }}
),
planet_no_dup_unt_prc as (
    select * from {{ ref('jpnwks_integration__planet_no_dup_unt_prc') }}
),

wk_so_planet_no_dup as (
    select * from {{ ref('jpnwks_integration__wk_so_planet_no_dup') }}
),

insert1 as (
    select a.jcp_rec_seq,
        case when a.net_prc = 0 then cast(b.v_chgnum as integer) else cast(a.net_prc as integer) end as v_price
        from wk_so_planet_modified a,
        planet_no_dup_unt_prc b where a.jcp_rec_seq = b.jcp_rec_seq
        and a.price_type = 'T'
        and a.jcp_rec_seq in (
                select jcp_rec_seq
                from wk_so_planet_no_dup
        )
),

insert2 as (
    select a.jcp_rec_seq,
    case when a.net_prc = 0 then cast(b.v_chgnum as integer) else cast(a.net_prc as integer) end as v_price
    from wk_so_planet_modified a,
    planet_no_dup_unt_prc b where a.jcp_rec_seq = b.jcp_rec_seq
    and a.price_type in ('K', 'P')
    and a.jcp_rec_seq in (
        select jcp_rec_seq
        from wk_so_planet_no_dup
        )
),

insert3 as (
    select a.jcp_rec_seq,
    case when a.net_prc = 0 then cast(b.v_chgnum as integer) else cast(a.net_prc as integer) end as v_price
    from wk_so_planet_no_dup a,
    planet_no_dup_unt_prc b where a.jcp_rec_seq = b.jcp_rec_seq
    and a.price_type = 'T'
    and a.jcp_rec_seq not in (
        select jcp_rec_seq
        from wk_so_planet_modified
        )
),

insert4 as (
    select a.jcp_rec_seq,
    case when a.net_prc = 0 then cast(b.v_chgnum as integer) else cast(a.net_prc as integer) end as v_price
    from wk_so_planet_no_dup a,
    planet_no_dup_unt_prc b where a.jcp_rec_seq = b.jcp_rec_seq
    and a.price_type in ('K', 'P')
    and a.jcp_rec_seq not in (
        select jcp_rec_seq
        from wk_so_planet_modified
        )
),

result as 

(
    select * from insert1
    union all
    select * from insert2
    union all
    select * from insert3
    union all
    select * from insert4
)

,

final as (
select 
   jcp_rec_seq::number(10,0) as jcp_rec_seq,
    v_price::varchar(256) as v_price
from result
)

select * from final