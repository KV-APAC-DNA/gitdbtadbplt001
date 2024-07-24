
with modified as(
    select * from dev_dna_core.snapjpnwks_integration.wk_so_planet_modified
),

cd_check as (
    select * from dev_dna_core.snapjpnwks_integration.item_cd_check
),

err_2 as (
    select * from dev_dna_core.snapjpnwks_integration.consistency_error_2
),

no_dup as (
    select * from dev_dna_core.snapjpnwks_integration.wk_so_planet_no_dup
),

item_chg as (
    select * from dev_dna_core.snapjpnitg_integration.itg_mds_jp_mt_so_item_chg
),

item_m as (
    select * from dev_dna_core.snapjpnedw_integration.edi_item_m
),
insert1 as (
    select ta.jcp_rec_seq as jcp_rec_seq,
    tb.ext_jan_cd as v_item_cd,
    tb.int_jan_cd as v_item_cd_jc 
    from (
    select a.*
    from modified a,
        cd_check c
    where a.jcp_rec_seq = c.jcp_rec_seq
        and c.v_f_item_cd = 1
    ) ta,
    item_chg tb where tb.int_jan_cd = ta.item_cd
    and ta.bgn_sndr_cd = tb.bgn_sndr_cd
    and ta.jcp_rec_seq in (
        select t.jcp_rec_seq
        from err_2 t
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
    from modified a,
        cd_check c
    where a.jcp_rec_seq = c.jcp_rec_seq
        and c.v_f_item_cd = 1
    ) ta,
    item_m tb where tb.jan_cd_so = ta.item_cd
    and ta.jcp_rec_seq not in (
        select t.jcp_rec_seq
        from err_2 t
        where t.error_cd = 'ERR_012'
        ) order by ta.item_cd
),

insert3 as(
    select ta.jcp_rec_seq as jcp_rec_seq,
    tb.item_cd as v_item_cd,
    tb.jan_cd_so v_item_cd_jc 
    from (
    select a.*
    from no_dup a,
        cd_check c
    where a.jcp_rec_seq = c.jcp_rec_seq
        and c.v_f_item_cd = 1
    ) ta,
    item_m tb where tb.jan_cd_so = ta.item_cd
    and ta.jcp_rec_seq not in (
        select t.jcp_rec_seq
        from modified t
        ) order by ta.item_cd
),

insert4 as (
    select ta.jcp_rec_seq as jcp_rec_seq,
    tb.ext_jan_cd as v_item_cd,
    tb.int_jan_cd v_item_cd_jc
    from (
    select a.*
    from no_dup a,
        cd_check c
    where a.jcp_rec_seq = c.jcp_rec_seq
        and c.v_f_item_cd = 1
    ) ta,
    item_chg tb where tb.int_jan_cd = ta.item_cd
    and tb.bgn_sndr_cd = ta.bgn_sndr_cd
    and ta.jcp_rec_seq not in (
        select t.jcp_rec_seq
        from modified t
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
)


select
    jcp_rec_seq::number(10,0) as jcp_rec_seq,
	v_item_cd::varchar(256) as v_item_cd,
	v_item_cd_jc::varchar(256) as v_item_cd_jc
from result
