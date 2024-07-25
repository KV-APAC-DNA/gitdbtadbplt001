
with wk_so_planet_no_dup as (
    select * from {{ ref('jpnwks_integration__wk_so_planet_no_dup') }}
),

edi_bgn_sndr as (
    select * from {{ ref('jpnedw_integration__edi_bgn_sndr') }}
),
store_v_price_stg as (
    select * from {{ ref('jpnwks_integration__store_v_price_stg') }}
),

stg
as (
    select a.jcp_rec_seq,
        a.bgn_sndr_cd,
        a.opt_fld
    from wk_so_planet_no_dup a
    where a.price_type <> 'T'
    ),

stg1
as (
    select stg.jcp_rec_seq,
        stg.bgn_sndr_cd,
        stg.opt_fld,
        b.tax_round v_tax_rn,
        b.tax_include v_tax_in
    from stg,
        edi_bgn_sndr b
    where b.bgn_sndr_cd = stg.bgn_sndr_cd
    ),

stg2
as (
    select stg1.jcp_rec_seq,
        case 
            when stg1.opt_fld = 'S'
                then 1
            when ltrim(stg1.opt_fld) is null
                and stg1.v_tax_in = 'S'
                then 1
            else 0
            end v_f_price,
        stg1.v_tax_rn
    from stg1
    ),

insert1
as (
    select stg2.jcp_rec_seq,
        case 
            when stg2.v_f_price = '1'
                and stg2.v_tax_rn = '1'
                then trunc(prc.v_price / 1.08 + 0.99)
            when stg2.v_f_price = '1'
                and stg2.v_tax_rn = '2'
                then trunc(prc.v_price / 1.08)
            when stg2.v_f_price = '1'
                then round(prc.v_price / 1.08)
            else cast(prc.v_price as integer)
            end v_price
    from stg2,
        store_v_price_stg prc
    where stg2.jcp_rec_seq = prc.jcp_rec_seq
    ),

stg3
as (
    select c.jcp_rec_seq,
        c.bgn_sndr_cd,
        c.opt_fld,
        c.qty v_qty
    from wk_so_planet_no_dup c
    where c.price_type = 'T'
    ),
insert2
as (
    select stg3.jcp_rec_seq,
        abs(prc1.v_price) * stg3.v_qty as v_price
    from stg3,
        store_v_price_stg prc1
    where stg3.jcp_rec_seq = prc1.jcp_rec_seq
    )
,

result as (
    select * from insert1   
    union all
    select * from insert2
)

select
    jcp_rec_seq::number(10,0) jcp_rec_seq,
	v_price::varchar(256) as v_price
from result


