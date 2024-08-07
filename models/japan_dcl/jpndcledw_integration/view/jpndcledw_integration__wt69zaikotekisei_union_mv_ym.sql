WITH wt69zaikotekisei_union_mv_ym_tbl AS
(
    SELECT * FROM dev_dna_core.snapjpdcledw_integration.wt69zaikotekisei_union_mv_ym_tbl
),

final AS
(
    SELECT wt69zaikotekisei_union_mv_ym_tbl.shukadate,
        wt69zaikotekisei_union_mv_ym_tbl.shukaym,
        wt69zaikotekisei_union_mv_ym_tbl.channel,
        wt69zaikotekisei_union_mv_ym_tbl.itemcode,
        wt69zaikotekisei_union_mv_ym_tbl.gokei
    FROM wt69zaikotekisei_union_mv_ym_tbl
)

SELECT * FROM final