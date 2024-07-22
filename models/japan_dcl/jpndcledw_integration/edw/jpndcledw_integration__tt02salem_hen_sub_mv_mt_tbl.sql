WITH c_tbecinquiremeisai
AS (
  SELECT *
  FROM dev_dna_core.snapjpdclitg_integration.c_tbecinquiremeisai
  ),
c_tbecinquirekesai
AS (
  SELECT *
  FROM dev_dna_core.snapjpdclitg_integration.c_tbecinquirekesai
  ),
c_tbeckesai
AS (
  SELECT *
  FROM dev_dna_core.snapjpdclitg_integration.c_tbeckesai
  ),
transformed
AS (
  SELECT
        'H' || cast((c_tbEcInquireMeisai.diinquirekesaiid) as varchar) 	AS SALENO			--返品交換依頼依頼ID
        ,NVL(c_tbEcInquireMeisai.DIORDERMEISAIID,'0')				AS GYONO			--明細番号
        ,'商品'													AS MEISAIKBN		--明細区分
        ,c_tbEcInquireMeisai.dsItemID							AS ITEMCODE			--商品コード
        ,NVL(c_tbEcInquireMeisai.c_dihenpinnum,'0')				AS SURYO			--返品数量
        ,-1 * (
                    NVL(c_tbEcInquireMeisai.ditotalprc,'0') - 			--税込み額(単価)
                            c_tbEcInquireMeisai.c_didiscountmeisai		--割引額
        )	AS	TANKA													--割引後税込単価：税込み額(単価) - 割引額
        ,-1 * (
                (NVL(c_tbEcInquireMeisai.ditotalprc,'0') * NVL(c_tbEcInquireMeisai.c_dihenpinnum,'0'))												--税込金額(単価) * 返品数量
                    - (NVL(c_tbEcInquireMeisai.c_didiscountmeisai,'0')	* NVL(c_tbEcInquireMeisai.c_dihenpinnum,'0'))									--割引額 * 返品数量
        )AS	KINGAKU																																		--割引後明細税込金額：税込金額(単価) * 返品数量 - 割引額 * 返品数量
        ,DECODE(c_tbEcInquireMeisai.ditotalprc,0,0
            ,-1 * ceil(
                    (
                        (NVL(c_tbEcInquireMeisai.ditotalprc,'0') * NVL(c_tbEcInquireMeisai.c_dihenpinnum,'0'))												--税込金額(単価) * 返品数量
                            - (NVL(c_tbEcInquireMeisai.c_didiscountmeisai,'0')	* NVL(c_tbEcInquireMeisai.c_dihenpinnum,'0'))								--割引額 * 返品数量
                    )/(DECODE(c_tbEcInquireMeisai.diitemprc,0,1,(c_tbEcInquireMeisai.ditotalprc/c_tbEcInquireMeisai.diitemprc)))						--消費税率
            )
        )AS	MEISAINUKIKINGAKU																															--割引後明細税抜金額：(税込金額(単価) * 返品数量 - 割引額 * 返品数量) * 消費税率																															

        ,NVL(c_tbEcInquireMeisai.c_didiscountrate,'0') 													AS	WARIRITU				--割引率
        ,-1 * (NVL(c_tbEcInquireMeisai.diTotalPrc,'0'))													AS	WARIMAEKOMITANKA		--割引前税込単価：単価(税込)
        ,-1 * (NVL((c_tbEcInquireMeisai.diitemprc * c_tbEcInquireMeisai.c_dihenpinnum),'0'))				AS	WARIMAENUKIKINGAKU		--割引前明細税抜金額：販売単価 * 返品数量
        ,-1 * (NVL((c_tbEcInquireMeisai.diTotalPrc * c_tbEcInquireMeisai.c_dihenpinnum),'0'))				AS	WARIMAEKOMIKINGAKU		--割引前明細税込金額：税込金額(単価) * 返品数量
        ,cast((c_tbEcInquireMeisai.c_dikesaiid)	as varchar)			AS DISPSALENO
        ,c_tbEcInquireMeisai.c_dikesaiid						AS KESAIID			--決済ID
        ,null								AS INQMEISAIROWID
        ,null								AS INQKESAIROWID
        ,null										AS KESAIROWID
        ,NVL(c_tbEcInquireKesai.C_DSHENPINSTS,'0')				AS HENPINSTS		--返品ステータス
        --返品交換依頼決済情報に紐付く返品交換依頼明細情報のみ
  FROM c_tbecinquiremeisai	--返品交換依頼明細情報
            ,c_tbecinquirekesai		--返品交換依頼決済情報
            ,c_tbeckesai			--決済情報
  WHERE	c_tbEcInquireMeisai.diinquireid = c_tbEcInquireKesai.diinquireid				--返品交換依頼明細情報.返品交換依頼ID = 返品交換依頼決済情報.返品交換依頼ID
        AND		c_tbEcInquireMeisai.diinquirekesaiid = c_tbEcInquireKesai.c_diinquirekesaiid 	--返品交換依頼明細情報.返品交換依頼決済ID = 返品交換依頼決済情報.返品交換依頼決済ID
        AND		c_tbEcInquireKesai.c_dikesaiid = c_tbEcKesai.c_dikesaiid						--返品交換依頼決済情報.決済ID = 決済情報.決済ID
        AND		c_tbEcKesai.diCancel = '0' --決済情報.キャンセルフラグ = '0'(0:有効)
        --AND		c_tbEcInquireMeisai.dihenpinkubun <> 0	--返品交換区分(返品された商品のみ)
        AND	c_tbEcInquireMeisai.DISETID = c_tbEcInquireMeisai.diID	--セット品の子供を除く
        AND		c_tbEcInquireMeisai.c_dsPointItemFlg = '0'			--ポイント交換対象外
        --削除フラグ
        AND c_tbEcInquireMeisai.dielimflg = '0'
        AND c_tbEcInquireKesai.dielimflg = '0'
        AND c_tbEcKesai.dielimflg = '0'
        --返品ステータス（返品済み、交換実施済みのものだけを表示）
        AND NVL(c_tbEcInquireKesai.C_DSHENPINSTS,'0') IN ('3010','5020') 
  ),
final
AS (
  SELECT 
        saleno::varchar(62) as saleno,
        gyono::number(18,0) as gyono,
        meisaikbn::varchar(20) as meisaikbn,
        itemcode::varchar(45) as itemcode,
        suryo::number(18,0) as suryo,
        tanka::number(18,0) as tanka,
        kingaku::number(18,0) as kingaku,
        meisainukikingaku::number(18,0) as meisainukikingaku,
        wariritu::number(18,0) as wariritu,
        warimaekomitanka::number(18,0) as warimaekomitanka,
        warimaenukikingaku::number(18,0) as warimaenukikingaku,
        warimaekomikingaku::number(18,0) as warimaekomikingaku,
        dispsaleno::varchar(60) as dispsaleno,
        kesaiid::number(10,0) as kesaiid,
        inqmeisairowid::varchar(1) as inqmeisairowid,
        inqkesairowid::varchar(1) as inqkesairowid,
        kesairowid::varchar(1) as kesairowid,
        henpinsts::varchar(6) as henpinsts
  FROM transformed
)
SELECT *
FROM final