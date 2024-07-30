with wk_so_planet_modified as(
    select * from {{ ref('jpnwks_integration__wk_so_planet_modified') }}
),

item_cd_check as (
    select * from {{ ref('jpnwks_integration__item_cd_check') }}
),

consistency_error_2 as (
    select * from {{ ref('jpnwks_integration__consistency_error_2') }}
),

wk_so_planet_no_dup as (
    select * from {{ ref('jpnwks_integration__wk_so_planet_no_dup') }}
),

itg_mds_jp_mt_so_item_chg as (
    select * from {{ ref('jpnitg_integration__itg_mds_jp_mt_so_item_chg') }}
),

edi_item_m as (
    select * from {{ ref('jpnedw_integration__edi_item_m') }}
),
insert1 as (
    select ta.jcp_rec_seq as jcp_rec_seq,
    tb.ext_jan_cd as v_item_cd,
    tb.int_jan_cd as v_item_cd_jc 
    from (
    select a.*
    from wk_so_planet_modified a,
        item_cd_check c
    where a.jcp_rec_seq = c.jcp_rec_seq
        and c.v_f_item_cd = 1
    ) ta,
    itg_mds_jp_mt_so_item_chg tb where tb.int_jan_cd = ta.item_cd
    and ta.bgn_sndr_cd = tb.bgn_sndr_cd
    and ta.jcp_rec_seq in (
        select t.jcp_rec_seq
        from consistency_error_2 t
        where t.error_cd = 'ERR_012'
        ) order by ta.item_cd
)

,
insert2 as (
    select ta.jcp_rec_seq as jcp_rec_seq,
    tb.item_cd as v_item_cd,
    tb.jan_cd_so as v_item_cd_jc 
    from (
    select a.*
    from wk_so_planet_modified a,
        item_cd_check c
    where a.jcp_rec_seq = c.jcp_rec_seq
        and c.v_f_item_cd = 1
    ) ta,
    edi_item_m tb where tb.jan_cd_so = ta.item_cd
    and ta.jcp_rec_seq not in (
        select t.jcp_rec_seq
        from consistency_error_2 t
        where t.error_cd = 'ERR_012'
        ) order by ta.item_cd
),

insert3 as(
    select ta.jcp_rec_seq as jcp_rec_seq,
    tb.item_cd as v_item_cd,
    tb.jan_cd_so v_item_cd_jc 
    from (
    select a.*
    from wk_so_planet_no_dup a,
        item_cd_check c
    where a.jcp_rec_seq = c.jcp_rec_seq
        and c.v_f_item_cd = 1
    ) ta,
    edi_item_m tb where tb.jan_cd_so = ta.item_cd
    and ta.jcp_rec_seq not in (
        select t.jcp_rec_seq
        from wk_so_planet_modified t
        ) order by ta.item_cd
),

insert4 as (
    select ta.jcp_rec_seq as jcp_rec_seq,
    tb.ext_jan_cd as v_item_cd,
    tb.int_jan_cd v_item_cd_jc
    from (
    select a.*
    from wk_so_planet_no_dup a,
        item_cd_check c
    where a.jcp_rec_seq = c.jcp_rec_seq
        and c.v_f_item_cd = 1
    ) ta,
    itg_mds_jp_mt_so_item_chg tb where tb.int_jan_cd = ta.item_cd
    and tb.bgn_sndr_cd = ta.bgn_sndr_cd
    and ta.jcp_rec_seq not in (
        select t.jcp_rec_seq
        from wk_so_planet_modified t
        ) order by ta.item_cd
)

,
result as (
    select * from insert1
    union all
    select * from insert2
    union all
    select * from insert3
    union all 
    select * from insert4
),


final as(
select
    jcp_rec_seq::number(10,0) as jcp_rec_seq,
	v_item_cd::varchar(256) as v_item_cd,
	v_item_cd_jc::varchar(256) as v_item_cd_jc
from result
)

select * from final