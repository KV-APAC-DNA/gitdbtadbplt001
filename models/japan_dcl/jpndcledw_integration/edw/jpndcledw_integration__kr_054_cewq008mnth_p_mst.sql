with kr_054_v_ptrgstdivmst
as (
    select *
    from dev_dna_core.snapjpdcledw_integration.kr_054_v_ptrgstdivmst
    ),
kr_054_v_tm05hanro
as (
    select *
    from dev_dna_core.snapjpdcledw_integration.kr_054_v_tm05hanro
    ),
transformed
as (
    select diregistdivcode as trk_kb,
        c_dsregistdivname as trk_kbnm,
        hanrocode as sm_kb,
        hanroname as sm_nm,
        row_number() over (
            order by diregistdivcode,
                hanrocode
            ) as disp_seq
    from (
        (
            select distinct diregistdivcode,
                c_dsregistdivname,
                hanrocode,
                hanroname
            from kr_054_v_ptrgstdivmst,
                kr_054_v_tm05hanro
            )
        
        union all
        
        select distinct diregistdivcode,
            c_dsregistdivname,
            99 as hanrocode,
            'etc' as hanroname
        from kr_054_v_ptrgstdivmst
        )
    ),
final
as (
    select trk_kb::varchar(7) as trk_kb,
        trk_kbnm::varchar(60) as trk_kbnm,
        sm_kb::number(38, 18) as sm_kb,
        sm_nm::varchar(48) as sm_nm,
        disp_seq::number(38, 18) as disp_seq
    from transformed
    )
select *
from final
