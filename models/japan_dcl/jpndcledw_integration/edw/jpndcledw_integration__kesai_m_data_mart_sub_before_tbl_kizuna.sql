with item_hanbai_v as(
    select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.item_hanbai_v
),
kesai_h_data_mart_sub_n as(
    select * from {{ ref('jpndcledw_integration__kesai_h_data_mart_sub_n') }}
),
kesai_m_data_mart_sub_n as(
    select * from {{ ref('jpndcledw_integration__kesai_m_data_mart_sub_n') }}
),
hanbai as(
    SELECT h_itemcode AS itemcd,
		h_itemname AS itemnm,
		diid::VARCHAR(500) AS diid,
		syutoku_kbn AS h_syutoku_kbn
	FROM item_hanbai_v
	WHERE h_syutoku_kbn IN ('NEXT', 'NEXT_DEL', 'MANUAL')
	GROUP BY h_itemcode,
		h_itemname,
		diid,
		syutoku_kbn
),
union1 as(
SELECT 
    NVL(cn_m.saleno, '0') AS saleno,
	/* 売上No（KEY） */
	cn_m.gyono as gyono,
	/* 行No */
	cn_m.meisaikbn as meisaikbn,
	/* 明細区分 */
	cn_m.itemcode as itemcode,
	/* アイテムコード */
	cn_m.itemname as itemname,
	/* 商品名 */
	cn_m.diid as diid,
	/* 商品内部ID */
	cn_m.disetid as disetid,
	/* セット商品内部ID */
	coalesce(item.itemcd, cn_m.itemcode::VARCHAR(500)) as setitemcd,
	/* セット商品コード */
	coalesce(item.itemnm, cn_m.itemname::VARCHAR(500)) as setitemnm,
	/* セット商品名 */
	cn_m.suryo as suryo,
	/* 数量 */
	cn_m.tanka as tanka,
	/* 単価 */
	cn_m.kingaku as kingaku,
	/* 金額 */
	cn_m.meisainukikingaku as meisainukikingaku,
	/* 明細税抜金額 */
	cn_m.wariritu as wariritu,
	/* 適用割引率 */
	cn_m.warimaekomitanka as warimaekomitanka,
	/* 割引前税込単価 */
	cn_m.warimaenukikingaku as warimaenukikingaku,
	/* 割引前明細金額 */
	cn_m.warimaekomikingaku as warimaekomikingaku,
	/* 割引前明細税込金額 */
	cn_m.bun_tanka as bun_tanka,
	/* 分解後単価 */
	cn_m.bun_kingaku as bun_kingaku,
	/* 分解後金額 */
	cn_m.bun_meisainukikingaku as bun_meisainukikingaku,
	/* 分解後明細税抜金額 */
	cn_m.bun_wariritu as bun_wariritu,
	/* 分解後適用割引率 */
	cn_m.bun_warimaekomitanka as bun_warimaekomitanka,
	/* 分解後割引前税込単価 */
	cn_m.bun_warimaenukikingaku as bun_warimaenukikingaku,
	/* 分解後割引前明細金額 */
	cn_m.bun_warimaekomikingaku as bun_warimaekomikingaku,
	/* 分解後割引前明細税込金額 */
	cn_m.dispsaleno as dispsaleno,
	/* 売上No */
	cn_m.kesaiid as kesaiid,
	/* 決済ID */
	cn_m.diorderid as diorderid,
	/* 受注内部ID */
	cn_m.henpinsts as henpinsts,
	/* 返品ステータス */
	cn_m.c_dspointitemflg as c_dspointitemflg,

	cn_m.c_diitemtype as c_diitemtype,

	cn_m.c_diadjustprc as c_diadjustprc,
	/* 調整金額 */
	cn_m.ditotalprc as ditotalprc,
	/* 税込み額 */
	cn_m.diitemtax as diitemtax,
	/* 消費税額 */
	cn_m.c_diitemtotalprc as c_diitemtotalprc,
	/* 販売小計(税込) */
	cn_m.c_didiscountmeisai as c_didiscountmeisai,
	/* 割引金額 */
	cn_m.disetmeisaiid as disetmeisaiid,
	/* 親行取得用 */
	cn_m.c_dssetitemkbn as c_dssetitemkbn,
	/* セット品区分 */
	cn_m.maker as maker
FROM kesai_h_data_mart_sub_n AS cn
INNER JOIN
	/* 明細の取得(CI-NEXT) */
	kesai_m_data_mart_sub_n AS cn_m ON cn.saleno = cn_m.saleno
LEFT OUTER JOIN
	/* セット品の商品コードの取得 */
	hanbai AS item ON cn_m.disetid = item.diid
	AND DECODE(cn_m.meisaikbn, '特典', 'MANUAL', 'NEXT') = DECODE(item.h_syutoku_kbn, 'MANUAL', 'MANUAL', 'NEXT')
WHERE cn.maker = 1
),
union2 as( 

SELECT NVL(cn_m.saleno, '0') AS saleno,
	/* 売上No（KEY） */
	cn_m.gyono as gyono,
	/* 行No */
	cn_m.meisaikbn as meisaikbn,
	/* 明細区分 */
	cn_m.itemcode as itemcode,
	/* アイテムコード */
	cn_m.itemname as itemname,
	/* 商品名 */
	cn_m.diid as diid,
	/* 商品内部ID */
	cn_m.disetid as disetid,
	/* セット商品内部ID */
	coalesce(item.itemcd, cn_m.itemcode::VARCHAR(500))  as setitemcd,
	/* セット商品コード */
	coalesce(item.itemnm, cn_m.itemname::VARCHAR(500)) as setitemnm,
	/* セット商品名 */
	--    NVL2(cp.saleno, NVL2(min.saleno, cn_m.suryo, 0), cn_m.suryo) AS suryo,
	cn_m.suryo as suryo,
	/* 数量 */
	--    NVL2(cp.saleno, NVL2(min.saleno, cn_m.tanka, 0), cn_m.tanka) AS tanka,
	cn_m.tanka  as tanka,
	/* 単価 */
	--    NVL2(cp.saleno, NVL2(min.saleno, cn_m.kingaku, 0), cn_m.kingaku) AS kingaku,
	cn_m.kingaku  as kingaku,
	/* 金額 */
	--    NVL2(cp.saleno, NVL2(min.saleno, cn_m.meisainukikingaku, 0), cn_m.meisainukikingaku) AS meisainukikingaku,
	cn_m.meisainukikingaku as meisainukikingaku,
	/* 明細税抜金額 */
	--    NVL2(cp.saleno, NVL2(min.saleno, cn_m.wariritu, 0), cn_m.wariritu) AS wariritu,
	cn_m.wariritu  as wariritu,
	/* 適用割引率 */
	--    NVL2(cp.saleno, NVL2(min.saleno, cn_m.warimaekomitanka, 0), cn_m.warimaekomitanka) AS warimaekomitanka,
	cn_m.warimaekomitanka  as warimaekomitanka,
	/* 割引前税込単価 */
	--    NVL2(cp.saleno, NVL2(min.saleno, cn_m.warimaenukikingaku, 0), cn_m.warimaenukikingaku) AS warimaenukikingaku,
	cn_m.warimaenukikingaku as warimaenukikingaku,
	/* 割引前明細金額 */
	--    NVL2(cp.saleno, NVL2(min.saleno, cn_m.warimaekomikingaku, 0), cn_m.warimaekomikingaku) AS warimaekomikingaku,
	cn_m.warimaekomikingaku as warimaekomikingaku,
	/* 割引前明細税込金額 */
	--    NVL2(cp.saleno, NVL2(min.saleno, cn_m.bun_tanka, 0), cn_m.bun_tanka) AS bun_tanka,
	cn_m.bun_tanka  as bun_tanka,
	/* 分解後単価 */
	--    NVL2(cp.saleno, NVL2(min.saleno, cn_m.bun_kingaku, 0), cn_m.bun_kingaku) AS bun_kingaku,
	cn_m.bun_kingaku as bun_kingaku,
	/* 分解後金額 */
	--    NVL2(cp.saleno, NVL2(min.saleno, cn_m.bun_meisainukikingaku, 0), cn_m.bun_meisainukikingaku) AS bun_meisainukikingaku,
	cn_m.bun_meisainukikingaku  as bun_meisainukikingaku,
	/* 分解後明細税抜金額 */
	--    NVL2(cp.saleno, NVL2(min.saleno, cn_m.bun_wariritu, 0), cn_m.bun_wariritu) AS bun_wariritu,
	cn_m.bun_wariritu  as bun_wariritu,
	/* 分解後適用割引率 */
	--    NVL2(cp.saleno, NVL2(min.saleno, cn_m.bun_warimaekomitanka, 0), cn_m.bun_warimaekomitanka) AS bun_warimaekomitanka,
	cn_m.bun_warimaekomitanka  as bun_warimaekomitanka,
	/* 分解後割引前税込単価 */
	--    NVL2(cp.saleno, NVL2(min.saleno, cn_m.bun_warimaenukikingaku, 0), cn_m.bun_warimaenukikingaku) AS bun_warimaenukikingaku,
	cn_m.bun_warimaenukikingaku  as bun_warimaenukikingaku,
	/* 分解後割引前明細金額 */
	--    NVL2(cp.saleno, NVL2(min.saleno, cn_m.bun_warimaekomikingaku, 0), cn_m.bun_warimaekomikingaku) AS bun_warimaekomikingaku,
	cn_m.bun_warimaekomikingaku  as bun_warimaekomikingaku,
	/* 分解後割引前明細税込金額 */
	cn_m.dispsaleno as dispsaleno,
	/* 売上No */
	cn_m.kesaiid as kesaiid,
	/* 決済ID */
	cn_m.diorderid as diorderid,
	/* 受注内部ID */
	cn_m.henpinsts as henpinsts,
	/* 返品ステータス */
	cn_m.c_dspointitemflg as c_dspointitemflg,
    
	cn_m.c_diitemtype as c_diitemtype,
	--	NVL2(cp.saleno, NVL2(min.saleno, cn_m.c_diadjustprc, 0), cn_m.c_diadjustprc) AS c_diadjustprc,
	cn_m.c_diadjustprc  as c_diadjustprc,
	/* 調整金額 */
	--    NVL2(cp.saleno, NVL2(min.saleno, cn_m.ditotalprc, 0), cn_m.ditotalprc) AS ditotalprc,
	cn_m.ditotalprc as ditotalprc,
	/* 税込み額 */
	--    NVL2(cp.saleno, NVL2(min.saleno, cn_m.diitemtax, 0), cn_m.diitemtax) AS diitemtax,
	cn_m.diitemtax as diitemtax,
	/* 消費税額 */
	--    NVL2(cp.saleno, NVL2(min.saleno, cn_m.c_diitemtotalprc, 0), cn_m.c_diitemtotalprc) AS c_diitemtotalprc,
	cn_m.c_diitemtotalprc  as c_diitemtotalprc,
	/* 販売小計(税込) */
	--    NVL2(cp.saleno, NVL2(min.saleno, cn_m.c_didiscountmeisai, 0), cn_m.c_didiscountmeisai) AS c_didiscountmeisai,
	cn_m.c_didiscountmeisai as c_didiscountmeisai,
	/* 割引金額 */
	cn_m.disetmeisaiid as disetmeisaiid,
	/* 親行取得用 */
	cn_m.c_dssetitemkbn as c_dssetitemkbn,
	/* セット品区分 */
	cn_m.maker as maker
FROM kesai_h_data_mart_sub_n AS cn
--    LEFT OUTER JOIN kesai_h_data_mart_sub_p AS cp
--        ON cn.dispsaleno = cp.sal_jisk_imp_snsh_no AND cp.uri_hen_kbn = 'H'
INNER JOIN
	/* 明細の取得(CI-NEXT) */
	kesai_m_data_mart_sub_n AS cn_m ON cn.saleno = cn_m.saleno
LEFT OUTER JOIN
	/* セット品の商品コードの取得 */
	hanbai AS item ON cn_m.disetid = item.diid
	AND DECODE(cn_m.meisaikbn, '特典', 'MANUAL', 'NEXT') = DECODE(item.h_syutoku_kbn, 'MANUAL', 'MANUAL', 'NEXT')
WHERE cn.maker = 2
),
transformed as(
    select * from union1
    union all
    select * from union2
),
final as(
    select
        saleno::varchar(64) as saleno,
        gyono::number(18,0) as gyono,
        meisaikbn::varchar(36) as meisaikbn,
        itemcode::varchar(60) as itemcode,
        itemname::varchar(190) as itemname,
        diid::varchar(60) as diid,
        disetid::varchar(60) as disetid,
        setitemcd::varchar(60) as setitemcd,
        setitemnm::varchar(190) as setitemnm,
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
        dispsaleno::varchar(64) as dispsaleno,
        kesaiid::varchar(64) as kesaiid,
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