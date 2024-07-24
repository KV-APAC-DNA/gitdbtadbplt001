with modified as(
    select * from dev_dna_core.snapjpnwks_integration.wk_so_planet_modified
),
no_dup_unt as (
    select * from dev_dna_core.snapjpnwks_integration.planet_no_dup_unt_prc
),

no_dup as (
    select * from dev_dna_core.snapjpnwks_integration.wk_so_planet_no_dup
),

insert1 as (
    select a.jcp_rec_seq,
        case when a.net_prc = 0 then cast(b.v_chgnum as integer) else cast(a.net_prc as integer) end as v_price
        from modified a,
        no_dup_unt b where a.jcp_rec_seq = b.jcp_rec_seq
        and a.price_type = 'T'
        and a.jcp_rec_seq in (
                select jcp_rec_seq
                from no_dup
        )
),

insert2 as (
    select a.jcp_rec_seq,
    case when a.net_prc = 0 then cast(b.v_chgnum as integer) else cast(a.net_prc as integer) end as v_price
    from modified a,
    no_dup_unt b where a.jcp_rec_seq = b.jcp_rec_seq
    and a.price_type in ('K', 'P')
    and a.jcp_rec_seq in (
        select jcp_rec_seq
        from no_dup
        )
),

insert3 as (
    select a.jcp_rec_seq,
    case when a.net_prc = 0 then cast(b.v_chgnum as integer) else cast(a.net_prc as integer) end as v_price
    from no_dup a,
    no_dup_unt b where a.jcp_rec_seq = b.jcp_rec_seq
    and a.price_type = 'T'
    and a.jcp_rec_seq not in (
        select jcp_rec_seq
        from modified
        )
),

insert4 as (
    select a.jcp_rec_seq,
    case when a.net_prc = 0 then cast(b.v_chgnum as integer) else cast(a.net_prc as integer) end as v_price
    from no_dup a,
    no_dup_unt b where a.jcp_rec_seq = b.jcp_rec_seq
    and a.price_type in ('K', 'P')
    and a.jcp_rec_seq not in (
        select jcp_rec_seq
        from modified
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



select 
   jcp_rec_seq::number(10,0) as jcp_rec_seq,
    v_price::varchar(256) as v_price
from result