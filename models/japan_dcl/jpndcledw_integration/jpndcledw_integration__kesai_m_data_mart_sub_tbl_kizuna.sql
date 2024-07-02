with KESAI_M_DATA_MART_SUB_BEFORE_kizuna as(
    select * from SNAPJPDCLEDW_INTEGRATION.KESAI_M_DATA_MART_SUB_BEFORE_kizuna
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
)
select * from transformed