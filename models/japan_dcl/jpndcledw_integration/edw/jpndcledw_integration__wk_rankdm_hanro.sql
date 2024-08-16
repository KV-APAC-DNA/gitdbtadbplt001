
WITH WK_BIRTHDAY_HANRO
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__wk_birthday_hanro') }}
    ),
TBUSRPRAM
AS (
    SELECT *
    FROM {{ ref('jpndclitg_integration__tbusrpram') }}
    ),
WK_STORE_KONYU
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__wk_store_konyu') }}
    ),
transformed
AS (
	SELECT BDDM.DIUSRID::NUMBER(10, 0) AS DIUSRID,
		USR.dsdat60::VARCHAR(40) AS dsdat60,
		USR.dsdat61::VARCHAR(40) AS dsdat61,
		NVL(URI_TENPO.KINGAKU_SUM, 0)::NUMBER(10, 0) AS KINGAKU_SUM,
		BDDM.HANRO::VARCHAR(40) AS HANRO_BDDM,
		(
			CASE 
				WHEN USR.dsdat60 = '拒否'
					AND USR.dsdat61 = '希望'
					AND NVL(URI_TENPO.KINGAKU_SUM, 0) <= 0
					THEN '通販'
				ELSE BDDM.HANRO
				END
			)::VARCHAR(40) AS HANRO_RANK,
		(
			CASE 
				WHEN USR.dsdat60 = '拒否'
					AND USR.dsdat61 = '希望'
					AND NVL(URI_TENPO.KINGAKU_SUM, 0) <= 0
					THEN '1'
				ELSE '0'
				END
			)::VARCHAR(4) AS FORCED_TUHAN_DM_SEND_FLG
	FROM WK_BIRTHDAY_HANRO BDDM
	LEFT JOIN TBUSRPRAM USR ON BDDM.DIUSRID = USR.DIUSRID
	LEFT JOIN WK_STORE_KONYU URI_TENPO ON BDDM.DIUSRID = URI_TENPO.DIUSRID
	),
final
AS (
	SELECT diusrid::NUMBER(10, 0) AS diusrid,
		dsdat60::VARCHAR(40) AS dm_tuhan,
		dsdat61::VARCHAR(40) AS dm_tenpo,
		kingaku_sum::NUMBER(10, 0) AS kingaku_sum,
		hanro_bddm::VARCHAR(40) AS hanro_bddm,
		hanro_rank::VARCHAR(40) AS hanro_rank,
		forced_tuhan_dm_send_flg::VARCHAR(4) AS forced_tuhan_dm_send_flg
	FROM transformed
	)
SELECT *
FROM final
