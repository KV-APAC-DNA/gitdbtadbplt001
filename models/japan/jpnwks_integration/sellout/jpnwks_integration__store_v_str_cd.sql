with no_dup as (
    select * from {{ ref('jpnwks_integration__wk_so_planet_no_dup') }}
),

cd_chng as (
    select * from {{ ref('jpnedw_integration__edi_rtlr_cd_chng') }}
),

insert1
as (
    select a.jcp_rec_seq,
        a.rtl_cd v_str_cd
    from wk_so_planet_no_dup a
    where (a.rtl_type) is null
        or rtrim(ltrim(rtl_type)) = ''
    ),

stg
as (
    select a.jcp_rec_seq,
        a.bgn_sndr_cd,
        a.rtl_cd
    from wk_so_planet_no_dup a
    where rtrim(ltrim(a.rtl_type)) != ''
    ),
    
insert2
as (
    select stg.jcp_rec_seq,
        cd.str_cd v_str_cd
    from stg,
        edi_rtlr_cd_chng cd
    where stg.bgn_sndr_cd = cd.bgn_sndr_cd
        and stg.rtl_cd = cd.rtlr_cd
    )
,

result as (
    select * from insert1
    union all
    select * from insert2
),

final as (
    select
        jcp_rec_seq::number(10,0) as jcp_rec_seq,
        v_str_cd::varchar(256) as v_str_cd
    from result
)

select * from final

