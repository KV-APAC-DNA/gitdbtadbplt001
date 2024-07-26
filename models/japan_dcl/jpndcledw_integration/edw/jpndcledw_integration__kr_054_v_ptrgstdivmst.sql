with c_tbecpointregistdivmst
as (
    select *
    from dev_dna_core.snapjpdclitg_integration.c_tbecpointregistdivmst
    ),
    -- c4 as (
    --     select '50001' as diregistdivcode,
    --     '基本ポイント' as c_dsregistdivname,
    --     '02' as c_dspointtype
    -- ),--基本ポイント
c1
as (
    select diregistdivcode,
        c_dsregistdivname,
        c_dspointtype
    from c_tbecpointregistdivmst
    where c_tbecpointregistdivmst.dielimflg = '0'
    ),
c2
as (
    select '50005' as diregistdivcode,
        'ポイント５倍キャンペーン' as c_dsregistdivname,
        '02' as c_dspointtype
    ), --ポイント5倍
c3
as (
    select '50008' as diregistdivcode,
        'ポイント１０倍キャンペーン' as c_dsregistdivname,
        '02' as c_dspointtype
    ), --ポイント10倍
    -- c5 as (
    --     select '50006' as diregistdivcode,
    --     'ステージアップボーナスポイント' as c_dsregistdivname,
    --     '02' as c_dspointtype
    -- ),--ステージアップポイント
    -- c6 as (
    --     select '50007' as diregistdivcode,
    --     'シーラボポイントアッププログラム' as c_dsregistdivname,
    --     '02' as c_dspointtype
    -- ), -- シーラボポイントアッププログラム
transformed
as (
    select *
    from c1
    
    union all
    
    select *
    from c2
    
    union all
    
    select *
    from c3
    ),
final
as (
    select diregistdivcode::varchar(7) as diregistdivcode,
        c_dsregistdivname::varchar(60) as c_dsregistdivname,
        c_dspointtype::varchar(3) as c_dspointtype
    from transformed
    )
select *
from final
