with c_tbecregularmeisai as(
	select * from {{ ref('jpndclitg_integration__c_tbecregularmeisai') }} 
),
c_tbecregularoperatehist as(
	select * from {{ ref('jpndclitg_integration__c_tbecregularoperatehist') }} 
),
c_tbecregularcontract as(
	select * from {{ ref('jpndclitg_integration__c_tbecregularcontract') }} 
),
tbecordermeisai as(
	select * from {{ ref('jpndclitg_integration__tbecordermeisai') }} 
),
tbecorder as(
	select * from {{ ref('jpndclitg_integration__tbecorder') }} 
),
tbecitem as(
	select * from {{ ref('jpndclitg_integration__tbecitem') }} 
),
OTODOKE_SHOKAI AS (
	SELECT RCM.C_DIREGULARCONTRACTID, --定期契約ID
		MIN(RCM.C_DSDELEVERYYM) AS SHOKAI_YM --初回お届け月
	FROM C_TBECREGULARMEISAI RCM --定期契約明細情報
	WHERE RCM.C_DSORDERCREATEKBN <> '3'
		AND --対象外を除く
		RCM.DIELIMFLG = 0
	GROUP BY RCM.C_DIREGULARCONTRACTID --定期契約ID
	),
KAIYAKU AS (
	--BGN-MOD 20200304 R.ISHIGAMI ***変更20001(TEIKIKEIYAKU_DATA_MART_UNIの解約件数が正しく計上されないデータがある)****
	SELECT RCM.C_DIREGULARCONTRACTID --定期契約ID
		,
		MIN(RCM.C_DSDELEVERYYM) AS KAIYAKUTUKI --解約年月
		,
		MIN(TO_CHAR(RCM.C_DSTODOKEDATE, 'YYYYMMDD')) AS KAIYAKUBI --解約年月日
	FROM C_TBECREGULARMEISAI RCM --定期契約明細情報
		--※補足注意：購入月は解約扱いにはならない（出荷済のため解約フラグが立たない）ので翌月お届け日が解約年月日になる
	WHERE RCM.c_dsOrderCreateKbn = '3' --※解約月以降の生成済明細全て
		AND RCM.c_dsContractChangeKbn = '4' --※↑にさらに解約月のみ4、解約月以降の月は0
		AND RCM.C_DSSCHEDULECHG08KBN = '41' --★この項目を使うと確実※解約月以降の生成済明細全て
	GROUP BY RCM.C_DIREGULARCONTRACTID
		--SELECT RCO.C_DIREGULARCONTRACTID, --定期契約ID
		--       TO_CHAR(RCO.C_DSREGULAROPERATEDATE,'YYYYMM') AS KAIYAKUTUKI,--解約月
		--       TO_CHAR(RCO.C_DSREGULAROPERATEDATE,'YYYYMMDD') AS KAIYAKUBI--解約日
		--FROM   CI_NEXT.C_TBECREGULAROPERATEHIST RCO --定期契約手続履歴
		--INNER JOIN CI_NEXT.C_TBECREGULARCONTRACT RC --定期契約情報
		--        ON RCO.C_DIREGULARCONTRACTID = RC.C_DIREGULARCONTRACTID
		--WHERE RCO.C_DSCHGTARGETKBN in ('07') AND--解約
		--      RCO.DIELIMFLG = 0 AND
		--      RC.C_DICANCELFLG = 1 AND --解約フラグ(解約確定)
		--      RC.DIELIMFLG = 0 AND
		--      NOT EXISTS (
		--                  SELECT 'X'
		--                   FROM   CI_NEXT.C_TBECREGULAROPERATEHIST RCO2 --定期契約手続履歴
		--                   WHERE  RCO.C_DIREGULARCONTRACTID = RCO2.C_DIREGULARCONTRACTID AND--手続き履歴ID
		--                          RCO.C_DSREGULAROPERATEDATE < RCO2.C_DSREGULAROPERATEDATE AND --手続日時
		--                          RCO2.C_DSCHGTARGETKBN in ('07','08') AND--解約、解約解除
		--                          RCO2.DIELIMFLG = 0
		--                 )
		--END-MOD 20200304 R.ISHIGAMI ***変更20001(TEIKIKEIYAKU_DATA_MART_UNIの解約件数が正しく計上されないデータがある)****
	),
OPERATE_HIST AS (
	SELECT c_diregularcontractid,
		max(to_char(c_dsregularoperatedate, 'YYYYMMDD')) AS c_dsregularoperatedate
	FROM c_tbecregularoperatehist
	WHERE C_DSCHGTARGETKBN = '07'
	GROUP BY c_diregularcontractid
	),
transformed as(
	SELECT RC.C_DIREGULARCONTRACTID, --定期契約ID
	LPAD(RC.C_DIUSRID, 10, '0') AS C_DIUSRID, --顧客ID
	--BGN-ADD 20200304 D.KITANO ***変更20206(定期データマートに販売経路を追加)****
	RC.DIROUTEID, --販路
	--END-ADD 20200304 D.KITANO ***変更20206(定期データマートに販売経路を追加)****
	TO_CHAR(RC.C_DIREGULARCONTRACTDATE, 'YYYYMMDD') AS KEIYAKUBI, --定期契約日
	OTODOKE_SHOKAI.SHOKAI_YM, --初回お届け月
	KAIYAKU.KAIYAKUBI, --解約日
	RCM.C_DSREGULARMEISAIID, --定期契約明細ID
	CASE 
		WHEN RCM.C_DSDELEVERYYM = OTODOKE_SHOKAI.SHOKAI_YM
			THEN '1'
		ELSE '0'
		END AS HEADER_FLG,
	RCM.C_DSDELEVERYYM,
	RCM.DSITEMID,
	RCM.C_DIREGULARCOURSEID,
	CASE 
		WHEN RCM.C_DSORDERCREATEKBN = '2'
			THEN I.DIITEMSALESPRC
		ELSE 0
		END AS DIITEMSALESPRC,
	RCM.C_DSORDERCREATEKBN,
	RCM.C_DSCONTRACTCHANGEKBN,
	CASE 
		WHEN RCM.C_DSDELEVERYYM >= NVL(KAIYAKU.KAIYAKUTUKI, '999999')
			THEN '1'
		ELSE '0'
		END AS C_DICANCELFLG, --解約フラグ
	CASE 
		WHEN RCM.C_DSDELEVERYYM >= NVL(KAIYAKU.KAIYAKUTUKI, '999999')
			AND RCM.C_DSORDERCREATEKBN = '3'
			THEN '解約'
		ELSE '有効'
		END AS KAIYAKU_KBN, --解約区分(分析用)
	--BGN-MOD 20200304 R.ISHIGAMI ***変更20001(TEIKIKEIYAKU_DATA_MART_UNIの解約件数が正しく計上されないデータがある)****
	CASE 
		WHEN RCM.C_DSDELEVERYYM = OTODOKE_SHOKAI.SHOKAI_YM
			THEN (
					CASE 
						WHEN RCM.C_DSDELEVERYYM = NVL(KAIYAKU.KAIYAKUTUKI, '999999')
							THEN 'スポット' --新規と解約が同月・・・スポット（計上できないため新規、解約から除外
						ELSE '新規' --そうでなければ通常の新規契約 ※購入有無にかかわらず初月はすべて新規でカウントする
						END
					)
		WHEN RCM.C_DSDELEVERYYM < NVL(KAIYAKU.KAIYAKUTUKI, '999999')
			THEN --まだ解約月の値がない、もしくは解約月より前
				(
					CASE 
						WHEN RCM.C_DSCONTRACTCHANGEKBN = 3
							THEN '休止' --受注生成対象外＝「休止継続」「購入継続」「お届け前休止継続」のいずれか
						ELSE '継続' --ここでは購入継続のみ指す ※本来の定期契約継続は「休止継続」「購入継続」「お届け前休止継続」のパターン
						END
					)
		WHEN RCM.C_DSDELEVERYYM = NVL(KAIYAKU.KAIYAKUTUKI, '999999')
			THEN '解約' --解約なら通常解約扱い
		ELSE '解約以降' --その他（初回より後かつ解約より後）は解約以降
		END AS CONTRACT_KBN, --契約区分(分析用)
	--       CASE
	--           WHEN RCM.C_DSDELEVERYYM = OTODOKE_SHOKAI.SHOKAI_YM AND
	--                RCM.C_DSDELEVERYYM = NVL(KAIYAKU.KAIYAKUTUKI,'999999') THEN 'スポット' --スポット
	--           WHEN RCM.C_DSDELEVERYYM = NVL(KAIYAKU.KAIYAKUTUKI,'999999') THEN '解約'  --解約
	--           WHEN RCM.C_DSDELEVERYYM = OTODOKE_SHOKAI.SHOKAI_YM THEN '新規'  --新規
	--           WHEN RCM.C_DSDELEVERYYM < OTODOKE_SHOKAI.SHOKAI_YM THEN '初回お届け前'  --初回お届け前
	--           WHEN RCM.C_DSCONTRACTCHANGEKBN = 3 THEN '休止'  --休止
	--           WHEN RCM.C_DSDELEVERYYM > NVL(KAIYAKU.KAIYAKUTUKI,'999999') THEN '解約以降'  --解約以降
	--           ELSE '継続'  --継続
	--       END AS CONTRACT_KBN,--契約区分(分析用)
	--END-MOD 20200304 R.ISHIGAMI ***変更20001(TEIKIKEIYAKU_DATA_MART_UNIの解約件数が正しく計上されないデータがある)****
	O.DIORDERCODE,
	OM.C_DIKESAIID,
	OM.DIMEISAIID,
	OH.C_DSREGULAROPERATEDATE AS KAIYAKUMOUSHIDEBI
FROM C_TBECREGULARCONTRACT RC --定期契約情報
INNER JOIN C_TBECREGULARMEISAI RCM --定期契約明細情報
	ON RC.C_DIREGULARCONTRACTID = RCM.C_DIREGULARCONTRACTID
	AND RC.DIELIMFLG = 0
	AND RCM.DIELIMFLG = 0
LEFT JOIN /*INNER */ OTODOKE_SHOKAI --初回お届ビュー	--2023/08/10 Cancelled subscription modification to change inner join to outer
	ON RC.C_DIREGULARCONTRACTID = OTODOKE_SHOKAI.C_DIREGULARCONTRACTID
LEFT JOIN TBECORDERMEISAI OM --受注明細情報
	ON RCM.C_DSREGULARMEISAIID = OM.C_DSREGULARMEISAIID
	AND OM.DIELIMFLG = 0
LEFT JOIN TBECORDER O --受注情報
	ON OM.DIORDERID = O.DIORDERID
	AND O.DIELIMFLG = 0
LEFT JOIN TBECITEM I --商品マスタ
	ON RCM.DIID = I.DIID
	AND I.DIELIMFLG = 0
LEFT JOIN KAIYAKU --解約ビュー
	ON RC.C_DIREGULARCONTRACTID = KAIYAKU.C_DIREGULARCONTRACTID
LEFT JOIN OPERATE_HIST OH ON -- RCM.C_DSREGULARMEISAIID = OH.C_DSREGULARMEISAIID AND 
	RC.C_DIREGULARCONTRACTID = OH.C_DIREGULARCONTRACTID

)
select * from transformed