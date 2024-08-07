with wk_iv_edi as(
    select * from {{ ref('jpnwks_integration__wk_iv_edi') }}
),
da_iv_edi_accum as(
    select * from {{ ref('jpnedw_integration__da_iv_edi_accum') }}
),
wk_iv_priority as(
    select * from {{ ref('jpnwks_integration__wk_iv_priority') }}
),
edi_cstm_m as(
    select * from {{ ref('jpnedw_integration__edi_cstm_m') }}
),
edi_item_m as(
    select * from {{ ref('jpnedw_integration__edi_item_m') }}
),
wk as(
    SELECT DISTINCT CSTM_CD,
			JAN_CD,
			TO_CHAR(INVT_DT, 'YYYYMM') INVT_YYYYMM
	FROM WK_IV_EDI
		
		UNION
		
    SELECT DISTINCT CSTM_CD,
        JAN_CD,
        TO_CHAR(ADD_MONTHS(INVT_DT, - 1), 'YYYYMM') INVT_YYYYMM
    FROM WK_IV_EDI
),

inv as(
    SELECT EID.CSTM_CD,
		EID.JAN_CD,
		PRI.INVT_YYYYMM,
		EID.ITEM_CD,
		EID.CS_QTY,
		EID.QTY,
		EID.PROC_DT,
		ROW_NUMBER() OVER (
			PARTITION BY EID.CSTM_CD,
			EID.ITEM_CD,
			PRI.INVT_YYYYMM ORDER BY PRI.PRIORITY DESC
			) RNUM
	FROM DA_IV_EDI_ACCUM EID
	INNER JOIN WK_IV_PRIORITY PRI ON PRI.INVT_LAST_DAY = EID.INVT_DT
	INNER JOIN WK ON EID.CSTM_CD = WK.CSTM_CD
		AND EID.JAN_CD = WK.JAN_CD
		AND PRI.INVT_YYYYMM = WK.INVT_YYYYMM
	LEFT OUTER JOIN EDI_CSTM_M CSM ON EID.CSTM_CD = CSM.CSTM_CD
	WHERE NVL(CSM.REBATE_REP_CD, '') != '112406'
		AND EID.VALID_FLG = '1'
),
inv2 as(
    SELECT EID.CSTM_CD,
		EID.JAN_CD,
		PRI.INVT_YYYYMM,
		EID.ITEM_CD,
		EID.CS_QTY,
		EID.QTY,
		EID.PROC_DT,
		ROW_NUMBER() OVER (
			PARTITION BY EID.CSTM_CD,
			EID.ITEM_CD,
			PRI.INVT_YYYYMM ORDER BY PRI.PRIORITY DESC
			) RNUM
	FROM DA_IV_EDI_ACCUM EID
	INNER JOIN WK_IV_PRIORITY PRI ON PRI.INVT_DAY_20 = EID.INVT_DT
	INNER JOIN  WK ON EID.CSTM_CD = WK.CSTM_CD
		AND EID.JAN_CD = WK.JAN_CD
		AND PRI.INVT_YYYYMM = WK.INVT_YYYYMM
	INNER JOIN EDI_CSTM_M CSM ON EID.CSTM_CD = CSM.CSTM_CD
	WHERE CSM.REBATE_REP_CD = '112406'
		AND EID.VALID_FLG = '1'
),
union1 as(
    SELECT INV.CSTM_CD as CSTM_CD,
        INV.JAN_CD as JAN_CD,
        LAST_DAY(TO_DATE(INV.INVT_YYYYMM || '01', 'YYYYMMDD')) as INVT_DT,
        MAX(INV.ITEM_CD) as ITEM_CD,
        MAX(ITM.PC) as PC,
        NVL(SUM(INV.CS_QTY), 0) as CS_QTY,
        NVL(SUM(INV.QTY), 0) as QTY,
        MAX(INV.PROC_DT) as PROC_DT
    FROM INV
    INNER JOIN EDI_ITEM_M ITM ON INV.ITEM_CD = ITM.ITEM_CD
    WHERE RNUM = 1
    GROUP BY INV.INVT_YYYYMM,
        INV.CSTM_CD,
        INV.JAN_CD
),
union2 as(
    SELECT INV2.CSTM_CD as CSTM_CD,
        INV2.JAN_CD as JAN_CD,
        LAST_DAY(TO_DATE(INV2.INVT_YYYYMM || '01', 'YYYYMMDD')) as INVT_DT,
        MAX(INV2.ITEM_CD) as ITEM_CD,
        MAX(ITM.PC) as PC,
        NVL(SUM(INV2.CS_QTY), 0) as CS_QTY,
        NVL(SUM(INV2.QTY), 0) as QTY,
        MAX(INV2.PROC_DT) as PROC_DT,
    FROM  INV2
    INNER JOIN EDI_ITEM_M ITM ON INV2.ITEM_CD = ITM.ITEM_CD
    WHERE RNUM = 1
    GROUP BY INV2.INVT_YYYYMM,
        INV2.CSTM_CD,
        INV2.JAN_CD
),
transformed as(
    select * from union1
    union all
    select * from union2
)
select * from transformed