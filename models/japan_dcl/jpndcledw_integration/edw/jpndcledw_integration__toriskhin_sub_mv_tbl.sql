with TM84_TORISKBETSUHIN
AS
(
select * from {{ ref('jpndcledw_integration__tm84_toriskbetsuhin') }}
),
CIT86OSALM_ASPAC as
(	
select * from {{ ref('jpndcledw_integration__cit86osalm_aspac') }} 
),
t1 as
(
    SELECT
            * 
    FROM 
        (
            SELECT
                CIT86OSALM_ASPAC.TOKUICODE AS TORISK_CD
                ,TORISKHIN.HIN_CD
                ,TORISKHIN.TORISK_HIN_CD
            FROM 
                TM84_TORISKBETSUHIN TORISKHIN
            LEFT OUTER JOIN 
                CIT86OSALM_ASPAC 
            ON 
                CIT86OSALM_ASPAC.TOKUICODE = TORISKHIN.TORISK_CD 
            AND 
                CIT86OSALM_ASPAC.ITEMCODE = TORISKHIN.HIN_CD
            WHERE
                CIT86OSALM_ASPAC.TOKUICODE IS NOT NULL
            UNION ALL
            SELECT
                CIT86OSALM_ASPAC.TOKUICODE AS TORISK_CD
                ,TORISKHIN.HIN_CD
                ,TORISKHIN.TORISK_HIN_CD
            FROM 
                TM84_TORISKBETSUHIN TORISKHIN
            LEFT OUTER JOIN 
                CIT86OSALM_ASPAC 
            ON 
                CIT86OSALM_ASPAC.SKYSK_CD = TORISKHIN.TORISK_CD 
            AND 
                CIT86OSALM_ASPAC.ITEMCODE = TORISKHIN.HIN_CD
            WHERE
                CIT86OSALM_ASPAC.SKYSK_CD IS NOT NULL
        )
    GROUP BY
        TORISK_CD,HIN_CD,TORISK_HIN_CD
),
final as
(
    SELECT TORISK_CD::VARCHAR(22) AS TORISK_CD,
	HIN_CD::VARCHAR(30) AS HIN_CD,
	TORISK_HIN_CD::VARCHAR(30) AS TORISK_HIN_CD,
	CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) AS INSERTED_DATE,
	'ETL_Batch'::VARCHAR(100) AS INSERTED_BY ,
	CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) AS UPDATED_DATE,
	NULL::VARCHAR(100) AS UPDATED_BY
    FROM T1
)
select * from final