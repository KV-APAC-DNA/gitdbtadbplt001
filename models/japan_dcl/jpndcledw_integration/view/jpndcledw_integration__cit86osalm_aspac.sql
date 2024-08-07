with cit86osalm_aspac_ikou as (
    select * from dev_dna_core.snapjpdcledw_integration.cit86osalm_aspac_ikou
),
cit85osalh as (
    select * from dev_dna_core.snapjpdcledw_integration.cit85osalh
),
cit86osalm as (
select * from dev_dna_core.snapjpdcledw_integration.cit86osalm
),

cte1 as (
SELECT cit86osalm_aspac_ikou.ourino,
    cit86osalm_aspac_ikou.gyono,
    cit86osalm_aspac_ikou.tokuicode,
    cit86osalm_aspac_ikou.tokuiname,
    cit86osalm_aspac_ikou.seikyusiharainame AS skysk_name,
    cit86osalm_aspac_ikou.seikyusiharaicode AS skysk_cd,
    cit86osalm_aspac_ikou.senyodenno,
    cit86osalm_aspac_ikou.juchkbn,
    ' ' AS juch_bko,
    cit86osalm_aspac_ikou.itemcode,
    cit86osalm_aspac_ikou.itemcode_hanbai,
    cit86osalm_aspac_ikou.dengokeikingaku AS shokei,
    cit86osalm_aspac_ikou.dengokeisyohizei AS tax,
    cit86osalm_aspac_ikou.zeikomikingaku AS sogokei,
    cit86osalm_aspac_ikou.suryo,
    cit86osalm_aspac_ikou.tanka,
    cit86osalm_aspac_ikou.meisainukikingaku,
    cit86osalm_aspac_ikou.nouki AS shimebi,
    cit86osalm_aspac_ikou.kouritanka,
    '00' AS torikeikbn,
    1 AS kakokbn,
    '0' AS shokuikibunrui,
    '000' AS henreasoncode,
    ' ' AS henreasonname,
    cit86osalm_aspac_ikou.ourino AS dispourino,
    cit86osalm_aspac_ikou.syukadate AS shukadate,
    '0' AS processtype_cd,
    '' AS juchno,
    '' AS daihyou_shukask_cd,
    '' AS daihyou_shukask_nmr,
    cit86osalm_aspac_ikou.bukacode AS bmn_hyouji_cd,
    cit86osalm_aspac_ikou.bukaname AS bmn_nms,
    NULL AS shohzei_ritsu
FROM cit86osalm_aspac_ikou
),

cte2 as (
SELECT cit85osalh.ourino,
    cit86osalm.gyono,
    cit85osalh.tokuicode,
    cit85osalh.tokuiname_aspac AS tokuiname,
    cit85osalh.skysk_name,
    cit85osalh.skysk_cd,
    cit85osalh.sendenno AS senyodenno,
    cit85osalh.juchkbn,
    cit85osalh.juch_bko,
    cit86osalm.itemcode,
    cit86osalm.itemcode_hanbai,
    cit85osalh.shokei,
    cit85osalh.tax,
    cit85osalh.sogokei,
    CASE 
        WHEN ((cit85osalh.juchkbn)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
            THEN cit86osalm.suryo
        WHEN ((cit85osalh.juchkbn)::TEXT = ('90'::CHARACTER VARYING)::TEXT)
            THEN cit86osalm.hensu
        ELSE (NULL::NUMERIC)::NUMERIC(18, 0)
        END AS suryo,
    cit86osalm.tanka,
    cit86osalm.meisainukikingaku,
    cit86osalm.shimebi,
    NULL AS kouritanka,
    cit85osalh.torikeikbn,
    0 AS kakokbn,
    cit85osalh.shokuikibunrui,
    cit85osalh.henreasoncode,
    cit85osalh.henreasonname,
    cit85osalh.dispourino,
    cit85osalh.shukadate,
    cit85osalh.processtype_cd,
    CASE 
        WHEN ((cit85osalh.juchkbn)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
            THEN cit85osalh.juchno
        WHEN ((cit85osalh.juchkbn)::TEXT = ('90'::CHARACTER VARYING)::TEXT)
            THEN cit85osalh.henpinno
        ELSE NULL::CHARACTER VARYING
        END AS juchno,
    cit85osalh.daihyou_shukask_cd,
    cit85osalh.daihyou_shukask_nmr,
    cit85osalh.bmn_hyouji_cd,
    cit85osalh.bmn_nms,
    cit86osalm.shohzei_ritsu
FROM (
    cit85osalh JOIN cit86osalm ON (((cit85osalh.ourino)::TEXT = (cit86osalm.ourino)::TEXT))
    )
WHERE (
        (
            (cit85osalh.kakokbn = ((0)::NUMERIC)::NUMERIC(18, 0))
            AND (cit86osalm.kakokbn = ((0)::NUMERIC)::NUMERIC(18, 0))
            )
        AND (((cit85osalh.torikeikbn)::NUMERIC)::NUMERIC(18, 0) = ((2)::NUMERIC)::NUMERIC(18, 0))
        )

),

final as (
select * from cte1
union all
select * from cte2
)

select * from final