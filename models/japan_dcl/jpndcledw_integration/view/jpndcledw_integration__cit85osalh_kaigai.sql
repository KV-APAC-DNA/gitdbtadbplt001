with cit85osalh_uri_mv as (
    select * from dev_dna_core.snapjpdcledw_integration.cit85osalh_uri_mv
),

cit85osalh_hen_mv as (
    select * from dev_dna_core.snapjpdcledw_integration.cit85osalh_hen_mv
),

cte1 as (
SELECT cit85osalh_uri.ourino,
    cit85osalh_uri.juchkbn,
    cit85osalh_uri.torikeikbn,
    cit85osalh_uri.juchdate,
    cit85osalh_uri.shukadate,
    cit85osalh_uri.tokuicode,
    cit85osalh_uri.shokei,
    cit85osalh_uri.tax,
    cit85osalh_uri.sogokei,
    cit85osalh_uri.nukikingaku,
    cit85osalh_uri.sendenno,
    cit85osalh_uri.updatedate,
    cit85osalh_uri.kkng_kbn,
    cit85osalh_uri.tuka_cd,
    cit85osalh_uri.shokei_tuka,
    cit85osalh_uri.tax_tuka,
    cit85osalh_uri.sogokei_tuka,
    cit85osalh_uri.kakokbn,
    cit85osalh_uri.tenpobunrui,
    cit85osalh_uri.shokuikibunrui,
    cit85osalh_uri.cancelflg,
    cit85osalh_uri.kokyano,
    cit85osalh_uri.henreasoncode,
    cit85osalh_uri.henreasonname,
    cit85osalh_uri.dispourino,
    cit85osalh_uri.tokuiname_aspac,
    cit85osalh_uri.tokuiname_kaigai,
    cit85osalh_uri.touroku_user,
    cit85osalh_uri.bmn_hyouji_cd,
    cit85osalh_uri.bmn_nms,
    cit85osalh_uri.daihyou_shukask_cd,
    cit85osalh_uri.daihyou_shukask_nmr,
    cit85osalh_uri.skysk_name,
    cit85osalh_uri.skysk_cd,
    cit85osalh_uri.juch_bko,
    cit85osalh_uri.processtype_cd,
    cit85osalh_uri.juchno,
    '' AS henpinno
FROM cit85osalh_uri_mv cit85osalh_uri
WHERE ((cit85osalh_uri.kkng_kbn)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
),

cte2 as (
SELECT cit85osalh_hen.ourino,
    cit85osalh_hen.juchkbn,
    cit85osalh_hen.torikeikbn,
    cit85osalh_hen.juchdate,
    cit85osalh_hen.shukadate,
    cit85osalh_hen.tokuicode,
    cit85osalh_hen.shokei,
    cit85osalh_hen.tax,
    cit85osalh_hen.sogokei,
    cit85osalh_hen.nukikingaku,
    cit85osalh_hen.sendenno,
    cit85osalh_hen.updatedate,
    cit85osalh_hen.kkng_kbn,
    cit85osalh_hen.tuka_cd,
    cit85osalh_hen.shokei_tuka,
    cit85osalh_hen.tax_tuka,
    cit85osalh_hen.sogokei_tuka,
    cit85osalh_hen.kakokbn,
    cit85osalh_hen.tenpobunrui,
    cit85osalh_hen.shokuikibunrui,
    cit85osalh_hen.cancelflg,
    cit85osalh_hen.kokyano,
    cit85osalh_hen.henreasoncode,
    cit85osalh_hen.henreasonname,
    cit85osalh_hen.dispourino,
    cit85osalh_hen.tokuiname_aspac,
    cit85osalh_hen.tokuiname_kaigai,
    cit85osalh_hen.touroku_user,
    cit85osalh_hen.bmn_hyouji_cd,
    cit85osalh_hen.bmn_nms,
    '' AS daihyou_shukask_cd,
    '' AS daihyou_shukask_nmr,
    cit85osalh_hen.skysk_name,
    cit85osalh_hen.skysk_cd,
    cit85osalh_hen.juch_bko,
    cit85osalh_hen.processtype_cd,
    '' AS juchno,
    cit85osalh_hen.henpinno
FROM cit85osalh_hen_mv cit85osalh_hen
WHERE ((cit85osalh_hen.kkng_kbn)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
),

final as (
    select * from cte1
    union all
    select * from cte2
)

select * from final