with TSUHAN_GASSAN_MV_TBL as 
(
    select * from SNAPJPDCLEDW_INTEGRATION.TSUHAN_GASSAN_MV_TBL
),
final as
(
SELECT tsuhan_gassan_mv_tbl.saleno,
    tsuhan_gassan_mv_tbl.juchkbn,
    tsuhan_gassan_mv_tbl.torikeikbn,
    tsuhan_gassan_mv_tbl.juchym,
    tsuhan_gassan_mv_tbl.juchdate,
    tsuhan_gassan_mv_tbl.shukaym,
    tsuhan_gassan_mv_tbl.shukadate,
    tsuhan_gassan_mv_tbl.channel,
    tsuhan_gassan_mv_tbl.channelcname,
    tsuhan_gassan_mv_tbl.gyono,
    tsuhan_gassan_mv_tbl.itemcode,
    tsuhan_gassan_mv_tbl.itemcode_hanbai,
    tsuhan_gassan_mv_tbl.suryo,
    tsuhan_gassan_mv_tbl.hensu,
    tsuhan_gassan_mv_tbl.meisainukikingaku,
    tsuhan_gassan_mv_tbl.daihanrobunname,
    tsuhan_gassan_mv_tbl.chuhanrobunname,
    tsuhan_gassan_mv_tbl.syohanrobunname,
    tsuhan_gassan_mv_tbl.kokyano,
    tsuhan_gassan_mv_tbl.mediacode,
    tsuhan_gassan_mv_tbl.dipromid,
    tsuhan_gassan_mv_tbl.kaisha,
    tsuhan_gassan_mv_tbl.kakokbn,
    tsuhan_gassan_mv_tbl.juch_shur,
    tsuhan_gassan_mv_tbl.anbunmeisainukikingaku
FROM TSUHAN_GASSAN_MV_TBL
)
select * from final