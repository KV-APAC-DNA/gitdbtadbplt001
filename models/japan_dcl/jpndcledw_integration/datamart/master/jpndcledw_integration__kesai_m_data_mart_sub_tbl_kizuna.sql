with KESAI_M_DATA_MART_SUB_BEFORE_kizuna as(
    select * from {{ ref('jpndcledw_integration__kesai_m_data_mart_sub_before_kizuna') }}
),
transformed as(
    SELECT SUB.SALENO,
        SUB.GYONO,
        SUB.MEISAIKBN,
        SUB.ITEMCODE,
        SUB.ITEMNAME,
        SUB.DIID,
        SUB.DISETID,
        SUB.SETITEMCD,
        SUB.SETITEMNM,
        SUB.SURYO,
        SUB.TANKA,
        SUB.KINGAKU,
        SUB.MEISAINUKIKINGAKU,
        SUB.WARIRITU,
        SUB.WARIMAEKOMITANKA,
        SUB.WARIMAENUKIKINGAKU,
        SUB.WARIMAEKOMIKINGAKU,
        SUB.BUN_TANKA,
        SUB.BUN_KINGAKU,
        SUB.BUN_MEISAINUKIKINGAKU,
        SUB.BUN_WARIRITU,
        SUB.BUN_WARIMAEKOMITANKA,
        SUB.BUN_WARIMAENUKIKINGAKU,
        SUB.BUN_WARIMAEKOMIKINGAKU,
        SUB.DISPSALENO,
        SUB.KESAIID,
        SUB.DIORDERID,
        SUB.HENPINSTS,
        SUB.C_DSPOINTITEMFLG,
        SUB.C_DIITEMTYPE,
        SUB.C_DIADJUSTPRC,
        SUB.DITOTALPRC,
        SUB.DIITEMTAX,
        SUB.C_DIITEMTOTALPRC,
        SUB.C_DIDISCOUNTMEISAI,
        SUB.DISETMEISAIID,
        SUB.C_DSSETITEMKBN,
        SUB.MAKER
    -- delete by Kizuna Project 2022/11/25 start --
    -- , SUB.SALENO_P
    -- , SUB.GYONO_P
    -- , SUB.ITEMCODE_P
    -- , SUB.ITEMCODE_HANBAI_P
    -- , SUB.SURYO_P
    -- , SUB.JYU_SURYO_P
    -- , SUB.OYAFLG_P
    -- , SUB.TANKA_P
    -- , SUB.HENSU_P
    -- , SUB.KINGAKU_P
    -- , SUB.MEISAINUKIKINGAKU_P
    -- , SUB.MEISAITAX_P
    -- , SUB.JUCHGYONO_P
    -- , SUB.DISPSALENO_P
    -- , SUB.JUCH_SHUR_P
    -- , SUB.TYOSEIKIKINGAKU_P
    -- , SUB.ANBUNMEISAINUKIKINGAKU_P
    -- , SUB.DEN_NEBIKI_ABN_KIN_P
    -- , SUB.DEN_NB_AB_SZ_KIN_P
    -- , SUB.DCLSM_HIN_HIN_NIBU_ID_P
    -- , SUB.KKNG_KBN_P
    -- , SUB.SHIMEBI_P
    -- , SUB.TANKA_TUKA_P
    -- , SUB.KINGAKU_TUKA_P
    -- , SUB.MEISAINUKIKINGAKU_TUKA_P
    -- , SUB.MEISAITAX_TUKA_P
    -- , SUB.MARKER_P
    -- , SUB.URI_HEN_KBN_P
    -- , SUB.SAL_JISK_IMP_SNSH_NO_P
    -- , SUB.DCLJUCH_ID_P
    -- , SUB.MARKER_NP
    ----BGN-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
    ----DnA側でデータマートを作成するため廃止
    ----, SUB.JOIN_REC_UPDDATE
    ----END-ADD 20200108 D.YAMASHITA ***変更19855(JJ連携処理の追加におけるDWHデータの差分抽出実現化)****
    -- delete by Kizuna Project 2022/11/25 end --
    FROM KESAI_M_DATA_MART_SUB_BEFORE_kizuna SUB
),
final as(
    select 
        saleno::varchar(63) as saleno,
        gyono::number(18,0) as gyono,
        meisaikbn::varchar(36) as meisaikbn,
        itemcode::varchar(60) as itemcode,
        itemname::varchar(192) as itemname,
        diid::varchar(60) as diid,
        disetid::varchar(60) as disetid,
        setitemcd::varchar(60) as setitemcd,
        setitemnm::varchar(192) as setitemnm,
        suryo::number(18,0) as suryo,
        tanka::number(18,0) as tanka,
        kingaku::number(18,0) as kingaku,
        meisainukikingaku::number(18,0) as meisainukikingaku,
        wariritu::number(18,0) as wariritu,
        warimaekomitanka::number(18,0) as warimaekomitanka,
        warimaenukikingaku::number(18,0) as warimaenukikingaku,
        warimaekomikingaku::number(18,0) as warimaekomikingaku,
        bun_tanka::number(18,0) as bun_tanka,
        bun_kingaku::number(18,0) as bun_kingaku,
        bun_meisainukikingaku::number(18,0) as bun_meisainukikingaku,
        bun_wariritu::number(18,0) as bun_wariritu,
        bun_warimaekomitanka::number(18,0) as bun_warimaekomitanka,
        bun_warimaenukikingaku::number(18,0) as bun_warimaenukikingaku,
        bun_warimaekomikingaku::number(18,0) as bun_warimaekomikingaku,
        dispsaleno::varchar(63) as dispsaleno,
        kesaiid::varchar(62) as kesaiid,
        diorderid::number(18,0) as diorderid,
        henpinsts::varchar(8) as henpinsts,
        c_dspointitemflg::varchar(8) as c_dspointitemflg,
        c_diitemtype::varchar(8) as c_diitemtype,
        c_diadjustprc::number(18,0) as c_diadjustprc,
        ditotalprc::number(18,0) as ditotalprc,
        diitemtax::number(18,0) as diitemtax,
        c_diitemtotalprc::number(18,0) as c_diitemtotalprc,
        c_didiscountmeisai::number(18,0) as c_didiscountmeisai,
        disetmeisaiid::number(18,0) as disetmeisaiid,
        c_dssetitemkbn::varchar(8) as c_dssetitemkbn,
        maker::number(18,0) as maker,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        null::varchar(100) as inserted_by ,
        current_timestamp()::timestamp_ntz(9) as updated_date,
        null::varchar(100) as updated_by
    from transformed
)
select * from final