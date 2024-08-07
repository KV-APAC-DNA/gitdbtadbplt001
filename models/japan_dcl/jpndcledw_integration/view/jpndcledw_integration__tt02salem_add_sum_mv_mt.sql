WITH tt02salem_add_uri_mv_mt AS
(
    SELECT * FROM dev_dna_core.snapjpdcledw_integration.tt02salem_add_uri_mv_mt
),

tt02salem_add_hen_mv_mt AS
(
    SELECT * FROM dev_dna_core.snapjpdcledw_integration.tt02salem_add_hen_mv_mt
),

ct1 AS
(
    SELECT tt02salem_add_uri_mv_mt.saleno,
        tt02salem_add_uri_mv_mt.gyono,
        tt02salem_add_uri_mv_mt.meisaikbn,
        tt02salem_add_uri_mv_mt.itemcode,
        tt02salem_add_uri_mv_mt.wariritu,
        tt02salem_add_uri_mv_mt.tanka,
        tt02salem_add_uri_mv_mt.warimaekomitanka,
        tt02salem_add_uri_mv_mt.suryo,
        tt02salem_add_uri_mv_mt.kingaku,
        tt02salem_add_uri_mv_mt.warimaekomikingaku,
        tt02salem_add_uri_mv_mt.meisainukikingaku,
        tt02salem_add_uri_mv_mt.warimaenukikingaku,
        tt02salem_add_uri_mv_mt.kesaiid,
        (trim((tt02salem_add_uri_mv_mt.saleno)::TEXT))::CHARACTER VARYING AS saleno_trm
    FROM tt02salem_add_uri_mv_mt
),


ct2 AS
(
SELECT tt02salem_add_hen_mv_mt.saleno,
    tt02salem_add_hen_mv_mt.gyono,
    tt02salem_add_hen_mv_mt.meisaikbn,
    tt02salem_add_hen_mv_mt.itemcode,
    tt02salem_add_hen_mv_mt.wariritu,
    tt02salem_add_hen_mv_mt.tanka,
    tt02salem_add_hen_mv_mt.warimaekomitanka,
    tt02salem_add_hen_mv_mt.suryo,
    tt02salem_add_hen_mv_mt.kingaku,
    tt02salem_add_hen_mv_mt.warimaekomikingaku,
    tt02salem_add_hen_mv_mt.meisainukikingaku,
    tt02salem_add_hen_mv_mt.warimaenukikingaku,
    tt02salem_add_hen_mv_mt.kesaiid,
    (trim((tt02salem_add_hen_mv_mt.saleno)::TEXT))::CHARACTER VARYING AS saleno_trm
FROM tt02salem_add_hen_mv_mt
),

final AS
(
    SELECT * FROM ct1
    UNION ALL
    SELECT * FROM ct2
)

SELECT * FROM final