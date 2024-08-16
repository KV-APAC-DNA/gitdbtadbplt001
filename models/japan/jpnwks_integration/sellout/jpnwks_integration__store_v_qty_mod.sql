with wk_so_planet_no_dup as (
    select * from {{ ref('jpnwks_integration__wk_so_planet_no_dup') }}
),

store_variable as (
    select * from {{ ref('jpnwks_integration__store_variable') }}
),

result as (
    select a.jcp_rec_seq,
    case 
        when b.v_f_pc = '1'
            then (cast(a.qty as integer) * cast(b.v_pc as integer))
        else cast(a.qty as integer)
        end v_qty_mod 
    from wk_so_planet_no_dup a,
    store_variable b 
    where a.jcp_rec_seq = b.jcp_rec_seq
    
),

final as (
    select
        jcp_rec_seq::number(10,0) as jcp_rec_seq,
        v_qty_mod::varchar(10) as v_qty_mod
    from result
)

select * from final