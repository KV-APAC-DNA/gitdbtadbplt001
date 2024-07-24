
with no_dup as (
    select * from dev_dna_core.snapjpnwks_integration.wk_so_planet_no_dup
),

item_cd_chng as (
    select * from dev_dna_core.snapjpnedw_integration.edi_item_cd_chng
),

result as (
    select ta.jcp_rec_seq,
    case 
        when tb.bgn_sndr_cd is null
            and tb.item_cd_typ is null
            and tb.whls_item_cd is null
            then 0
        else tb.pc
        end v_f_pc,
    case 
        when tb.bgn_sndr_cd is null
            and tb.item_cd_typ is null
            and tb.whls_item_cd is null
            then 0
        else 1
        end v_pc from no_dup ta,
    item_cd_chng tb where tb.bgn_sndr_cd(+) = ta.bgn_sndr_cd
    and tb.item_cd_typ(+) = ta.item_cd_typ
    and tb.whls_item_cd(+) = ta.item_cd
)

select
    jcp_rec_seq::number(10,0) as jcp_rec_seq,
	v_f_pc::varchar(2) as v_f_pc,
	v_pc::varchar(10) as v_pc
from result
