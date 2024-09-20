with cim01kokya as (
select * from  {{ ref('jpndcledw_integration__cim01kokya') }}
),
c_tbecusrcomment as (
select * from {{ ref('jpndclitg_integration__c_tbecusrcomment') }}
),
transformed as (
SELECT
T.DIECUSRID AS DIECUSRID
,cast((MAX(T.C_DSUSRCOMMENTCLASSKBN)) as varchar) AS COM85FLG
,T.DSREN AS UPDATEDATE
--BGN-ADD 20200108 D.YAMASHITA ***Ã¥Â¤â€°Ã¦â€ºÂ´19855(JJÃ©â‚¬Â£Ã¦ï¿½ÂºÃ¥â€¡Â¦Ã§ï¿½â€ Ã£ï¿½Â®Ã¨Â¿Â½Ã¥Å Â Ã£ï¿½Â«Ã£ï¿½Å Ã£ï¿½â€˜Ã£â€šâ€¹DWHÃ£Æ’â€¡Ã£Æ’Â¼Ã£â€šÂ¿Ã£ï¿½Â®Ã¥Â·Â®Ã¥Ë†â€ Ã¦Å Â½Ã¥â€¡ÂºÃ¥Â®Å¸Ã§ï¿½Â¾Ã¥Å’â€“)****
--DnAÃ¥ï¿½Â´Ã£ï¿½Â§Ã£Æ’â€¡Ã£Æ’Â¼Ã£â€šÂ¿Ã£Æ’Å¾Ã£Æ’Â¼Ã£Æ’Ë†Ã£â€šâ€™Ã¤Â½Å“Ã¦Ë†ï¿½Ã£ï¿½â„¢Ã£â€šâ€¹Ã£ï¿½Å¸Ã£â€šï¿½Ã¥Â»Æ’Ã¦Â­Â¢
--,MAX(JOIN_REC_UPDDATE) AS JOIN_REC_UPDDATE
--END-ADD 20200108 D.YAMASHITA ***Ã¥Â¤â€°Ã¦â€ºÂ´19855(JJÃ©â‚¬Â£Ã¦ï¿½ÂºÃ¥â€¡Â¦Ã§ï¿½â€ Ã£ï¿½Â®Ã¨Â¿Â½Ã¥Å Â Ã£ï¿½Â«Ã£ï¿½Å Ã£ï¿½â€˜Ã£â€šâ€¹DWHÃ£Æ’â€¡Ã£Æ’Â¼Ã£â€šÂ¿Ã£ï¿½Â®Ã¥Â·Â®Ã¥Ë†â€ Ã¦Å Â½Ã¥â€¡ÂºÃ¥Â®Å¸Ã§ï¿½Â¾Ã¥Å’â€“)****
FROM
(
SELECT
	K.KOKYANO AS DIECUSRID
	,decode(C.C_DSUSRCOMMENTCLASSKBN,null,0,1) as C_DSUSRCOMMENTCLASSKBN
	,nvl(TO_CHAR(C.DSREN,'yyyymmdd'),cast(K.INSERTDATE as varchar)) AS DSREN
--BGN-ADD 20200108 D.YAMASHITA ***Ã¥Â¤â€°Ã¦â€ºÂ´19855(JJÃ©â‚¬Â£Ã¦ï¿½ÂºÃ¥â€¡Â¦Ã§ï¿½â€ Ã£ï¿½Â®Ã¨Â¿Â½Ã¥Å Â Ã£ï¿½Â«Ã£ï¿½Å Ã£ï¿½â€˜Ã£â€šâ€¹DWHÃ£Æ’â€¡Ã£Æ’Â¼Ã£â€šÂ¿Ã£ï¿½Â®Ã¥Â·Â®Ã¥Ë†â€ Ã¦Å Â½Ã¥â€¡ÂºÃ¥Â®Å¸Ã§ï¿½Â¾Ã¥Å’â€“)****
--DnAÃ¥ï¿½Â´Ã£ï¿½Â§Ã£Æ’â€¡Ã£Æ’Â¼Ã£â€šÂ¿Ã£Æ’Å¾Ã£Æ’Â¼Ã£Æ’Ë†Ã£â€šâ€™Ã¤Â½Å“Ã¦Ë†ï¿½Ã£ï¿½â„¢Ã£â€šâ€¹Ã£ï¿½Å¸Ã£â€šï¿½Ã¥Â»Æ’Ã¦Â­Â¢
--	,TO_NUMBER(GREATEST(
--			 K.JOIN_REC_UPDDATE
--			,TO_NUMBER(NVL(TO_CHAR(NVL(C.DSREN,C.DSPREP),'YYYYMMDDHH24MISS'),0))
--		)) AS JOIN_REC_UPDDATE     --Ã§Âµï¿½Ã¥ï¿½Ë†Ã£Æ’Â¬Ã£â€šÂ³Ã£Æ’Â¼Ã£Æ’â€°Ã¦â€ºÂ´Ã¦â€“Â°Ã¦â€”Â¥Ã¦â„¢â€š(JJÃ©â‚¬Â£Ã¦ï¿½ÂºÃ£ï¿½Â®Ã¥Â·Â®Ã¥Ë†â€ Ã¦Å Â½Ã¥â€¡ÂºÃ£ï¿½Â«Ã¤Â½Â¿Ã§â€�Â¨)
--END-ADD 20200108 D.YAMASHITA ***Ã¥Â¤â€°Ã¦â€ºÂ´19855(JJÃ©â‚¬Â£Ã¦ï¿½ÂºÃ¥â€¡Â¦Ã§ï¿½â€ Ã£ï¿½Â®Ã¨Â¿Â½Ã¥Å Â Ã£ï¿½Â«Ã£ï¿½Å Ã£ï¿½â€˜Ã£â€šâ€¹DWHÃ£Æ’â€¡Ã£Æ’Â¼Ã£â€šÂ¿Ã£ï¿½Â®Ã¥Â·Â®Ã¥Ë†â€ Ã¦Å Â½Ã¥â€¡ÂºÃ¥Â®Å¸Ã§ï¿½Â¾Ã¥Å’â€“)****
FROM
	CIM01KOKYA K
LEFT OUTER JOIN
	C_TBECUSRCOMMENT C
ON
	K.KOKYANO = LPAD(C.DIECUSRID,10,'0')
AND
	C.C_DSUSRCOMMENTCLASSKBN = '85'
AND
	C.DIELIMFLG <> '1'
) T
GROUP BY
T.DIECUSRID
,T.DSREN
),
final as (
select
diecusrid::varchar(20) as diecusrid,
com85flg::varchar(40) as com85flg,
updatedate::varchar(40) as updatedate,
current_timestamp()::timestamp_ntz(9) as INSERTED_DATE,
null::varchar(100) as inserted_by,
current_timestamp()::timestamp_ntz(9) as UPDATED_DATE,
null::varchar(100) as updated_by
from transformed
)
select * from final
