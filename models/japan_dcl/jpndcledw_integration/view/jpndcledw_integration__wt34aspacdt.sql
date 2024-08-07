WITH aspac_common_new AS
(
    SELECT * FROM dev_dna_core.snapjpdcledw_integration.aspac_common_new
),

final AS
(
    SELECT aspac_common_new.ourino,
        aspac_common_new.tokuicode,
        aspac_common_new.tokuiname,
        aspac_common_new.tokuiname_ryaku,
        aspac_common_new.tokuiname_kana,
        aspac_common_new.itemkbn,
        aspac_common_new.itemkbnname,
        aspac_common_new.senyodenno,
        aspac_common_new.juchkbn,
        aspac_common_new.itemcode,
        aspac_common_new.jancode,
        aspac_common_new.tokuiitemcode,
        aspac_common_new.itemname,
        aspac_common_new.suryo,
        aspac_common_new.tanka,
        aspac_common_new.kingaku,
        aspac_common_new.shimebi,
        aspac_common_new.kouritanka,
        aspac_common_new.sum_todofuken,
        aspac_common_new.tokuiname2,
        aspac_common_new.todofukennm,
        aspac_common_new.sensyukai,
        aspac_common_new.unity_itemname,
        aspac_common_new.sampleflg,
        aspac_common_new.sum_name,
        aspac_common_new.hansoku_tanka,
        aspac_common_new.hansoku_ext,
        aspac_common_new.sum_tokuiname,
        aspac_common_new.haibanhinmokucode,
        aspac_common_new.nohindate,
        aspac_common_new.shukadate,
        aspac_common_new.processtype_cd,
        aspac_common_new.juchno,
        aspac_common_new.torikeikbn,
        aspac_common_new.tokuizokuseino,
        aspac_common_new.skysk_name,
        aspac_common_new.skysk_cd,
        aspac_common_new.urikbn,
        aspac_common_new.shokei,
        aspac_common_new.tax,
        aspac_common_new.sogokei,
        aspac_common_new.juch_bko,
        aspac_common_new.daihyou_shukask_cd,
        aspac_common_new.daihyou_shukask_nmr,
        aspac_common_new.shohzei_ritsu
    FROM aspac_common_new
)

SELECT * FROM final