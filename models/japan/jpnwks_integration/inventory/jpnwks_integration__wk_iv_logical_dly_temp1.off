with check_date as(
    select SUBSTRING(current_timestamp(),6,2)||SUBSTRING(current_timestamp(),9,2) as daate
),
DW_SI_SELL_IN_DLY as(
select * from DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.DW_SI_SELL_IN_DLY
),
EDI_ITEM_M as(
select * from DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.EDI_ITEM_M
),
ty_date as(
    select SUBSTRING(DATEADD(year,-1,DATEADD(day,1,SUBSTRING(current_timestamp(),1,10))),1,10) as ty_exec_start_dt
),
ty_date2 as(
    select DATEADD(day,1,to_date(round(SUBSTRING(current_timestamp()::timestamp_ntz(9),1,4)-1) ||  '0228', 'YYYYMMDD')) as ty_exec_start_dt
),
condi as(
    select
    case when check_date.daate = '0229'
    then ty_date2.ty_exec_start_dt
    else ty_date.ty_exec_start_dt
    end as ty_exec_start_dt_final
    from check_date, ty_date, ty_date2
),
t_date as(
    SELECT	SUBSTRING(MIN(MAIN.YMD_DT),1,10) as ty_tounen_start_dt FROM DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.MT_CLD	MAIN WHERE	EXISTS
(SELECT * FROM	DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.MT_CLD SUB 
WHERE MAIN.YEAR_445 = SUB.YEAR_445	AND	SUB.YMD_DT = to_date(current_timestamp())
)
),
union1 as(
    SELECT SID.CALDAY as CALDAY,
	'SELLIN' as DATA_TYPE,
	SID.CUSTOMER as CSTM_CD,
	ITM.JAN_CD as JAN_CD,
	NVL2(nullif(MAX(ITM.PC), ''), MAX(ITM.PC), NULL) as PC,
	NVL(SUM(SID.JCP_QTY * - 1), 0) as QTY,
	NVL(SUM(SID.AMOCCC * - 1), 0) as NET_PRC
FROM condi, t_date, DW_SI_SELL_IN_DLY SID
INNER JOIN EDI_ITEM_M ITM ON SID.MATERIAL = ITM.ITEM_CD
WHERE to_date(SID.CALDAY) >= LEAST(condi.ty_exec_start_dt_final, t_date.ty_tounen_start_dt, to_date( (substring(condi.ty_exec_start_dt_final, 1, 4) || '-01-01')))
	AND SID.MATERIAL != 'REBATE'
	AND SID.ACCOUNT IN  ('402000', '402098')
GROUP BY SID.CALDAY,
	SID.CUSTOMER,
	ITM.JAN_CD
),
union2 as(
    SELECT SPF.SHP_DATE as CALDAY,
	'"+context.ty_Data_Type_SellOut+"' as DATA_TYPE,
	SPF.JCP_SHP_TO_CD as CSTM_CD,
	SPF.ITEM_CD as JAN_CD,
	NVL2(nullif(MAX(ITM.PC), ''), MAX(ITM.PC), NULL) as PC,
	NVL(SUM(SPF.QTY), 0) as QTY,
	NVL(SUM(SPF.QTY * ITM.UNT_PRC), 0) as NET_PRC,
FROM JP_EDW.DW_SO_SELL_OUT_DLY SPF
INNER JOIN JP_EDW.EDI_ITEM_M ITM ON SPF.ITEM_CD = ITM.JAN_CD_SO
GROUP BY SPF.SHP_DATE,
	SPF.JCP_SHP_TO_CD,
	SPF.ITEM_CD
)
select * from union1

