{{
    config
    (
        materialized='incremental',
        incremental_strategy = 'append',
        pre_hook = "
                    {% if is_incremental() %}
                    UPDATE {{this}}
                    SET EXPORT_FLAG = '1'
                    WHERE EXPORT_FLAG = '0'
                        AND JCP_REC_SEQ IN (
                            SELECT jcp_rec_seq
                            FROM {{ ref('jpnwks_integration__consistency_error_2 ') }} 
                            WHERE exec_flag IN ('MANUAL', 'AUTOCORRECT')
                            )
                        AND JCP_REC_SEQ IN (
                            SELECT jcp_rec_seq
                            FROM {{ ref('jpnwks_integration__wk_so_planet_no_dup ') }} 
                            ); 
                    {% endif %}
                    ",
        post_hook = "
                    UPDATE {{this}}
                    SET EXPORT_FLAG = '1'
                    WHERE EXPORT_FLAG = '0'
                        AND (
                            JCP_REC_SEQ IN (
                                SELECT jcp_rec_seq
                                FROM {{ ref('jpnwks_integration__consistency_error_2 ') }}
                                WHERE exec_flag IN ('DELETE')
                                )
                            OR JCP_REC_SEQ IN (
                                SELECT jcp_rec_seq
                                FROM {{ ref('jpnwks_integration__wk_so_planet_cleansed') }} 
                                )
                            );
                    "
    )
}}

WITH consistency_error_2
AS (
	SELECT *
	FROM {{ ref('jpnwks_integration__consistency_error_2') }}
	),
wk_so_planet_no_dup
AS (
	SELECT *
	FROM {{ ref('jpnwks_integration__wk_so_planet_no_dup') }}
	),
ct1
AS (
	SELECT A.JCP_REC_SEQ,
		CASE 
			WHEN error_cd = 'ERR_001'
				THEN 'E001'
			ELSE '0'
			END ERROR_CD,
		A.EXEC_FLAG,
		'0' EXPORT_FLAG,
		to_timestamp(SUBSTRING(current_timestamp(), 1, 19)) AS JCP_CREATE_DATE
	FROM consistency_error_2 A
	INNER JOIN wk_so_planet_no_dup B ON A.JCP_REC_SEQ = B.JCP_REC_SEQ
	),
ct2
AS (
	SELECT A.JCP_REC_SEQ,
		CASE 
			WHEN error_cd = 'ERR_002'
				THEN 'E002'
			ELSE '0'
			END ERROR_CD,
		A.EXEC_FLAG,
		'0' EXPORT_FLAG,
		to_timestamp(SUBSTRING(current_timestamp(), 1, 19)) AS JCP_CREATE_DATE
	FROM consistency_error_2 A
	INNER JOIN wk_so_planet_no_dup B ON A.JCP_REC_SEQ = B.JCP_REC_SEQ
	),
ct3
AS (
	SELECT A.JCP_REC_SEQ,
		CASE 
			WHEN error_cd = 'ERR_004'
				THEN 'E004'
			ELSE '0'
			END ERROR_CD,
		A.EXEC_FLAG,
		'0' EXPORT_FLAG,
		to_timestamp(SUBSTRING(current_timestamp(), 1, 19)) AS JCP_CREATE_DATE
	FROM consistency_error_2 A
	INNER JOIN wk_so_planet_no_dup B ON A.JCP_REC_SEQ = B.JCP_REC_SEQ
	),
ct4
AS (
	SELECT A.JCP_REC_SEQ,
		CASE 
			WHEN error_cd = 'ERR_007'
				THEN 'E007'
			ELSE '0'
			END ERROR_CD,
		A.EXEC_FLAG,
		'0' EXPORT_FLAG,
		to_timestamp(SUBSTRING(current_timestamp(), 1, 19)) AS JCP_CREATE_DATE
	FROM consistency_error_2 A
	INNER JOIN wk_so_planet_no_dup B ON A.JCP_REC_SEQ = B.JCP_REC_SEQ
	),
ct5
AS (
	SELECT A.JCP_REC_SEQ,
		CASE 
			WHEN error_cd = 'ERR_008'
				THEN 'E008'
			ELSE '0'
			END ERROR_CD,
		A.EXEC_FLAG,
		'0' EXPORT_FLAG,
		to_timestamp(SUBSTRING(current_timestamp(), 1, 19)) AS JCP_CREATE_DATE
	FROM consistency_error_2 A
	INNER JOIN wk_so_planet_no_dup B ON A.JCP_REC_SEQ = B.JCP_REC_SEQ
	),
ct6
AS (
	SELECT A.JCP_REC_SEQ,
		CASE 
			WHEN error_cd = 'ERR_009'
				THEN 'E009'
			ELSE '0'
			END ERROR_CD,
		A.EXEC_FLAG,
		'0' EXPORT_FLAG,
		to_timestamp(SUBSTRING(current_timestamp(), 1, 19)) AS JCP_CREATE_DATE
	FROM consistency_error_2 A
	INNER JOIN wk_so_planet_no_dup B ON A.JCP_REC_SEQ = B.JCP_REC_SEQ
	),
ct7
AS (
	SELECT A.JCP_REC_SEQ,
		CASE 
			WHEN error_cd = 'ERR_012'
				THEN 'E012'
			ELSE '0'
			END ERROR_CD,
		A.EXEC_FLAG,
		'0' EXPORT_FLAG,
		to_timestamp(SUBSTRING(current_timestamp(), 1, 19)) AS JCP_CREATE_DATE
	FROM consistency_error_2 A
	INNER JOIN wk_so_planet_no_dup B ON A.JCP_REC_SEQ = B.JCP_REC_SEQ
	),
ct8
AS (
	SELECT A.JCP_REC_SEQ,
		CASE 
			WHEN error_cd = 'ERR_014'
				THEN 'E014'
			ELSE '0'
			END ERROR_CD,
		A.EXEC_FLAG,
		'0' EXPORT_FLAG,
		to_timestamp(SUBSTRING(current_timestamp(), 1, 19)) JCP_CREATE_DATE
	FROM consistency_error_2 A
	INNER JOIN wk_so_planet_no_dup B ON A.JCP_REC_SEQ = B.JCP_REC_SEQ
	),
ct9
AS (
	SELECT A.JCP_REC_SEQ,
		CASE 
			WHEN error_cd = 'ERR_017'
				THEN 'E017'
			ELSE '0'
			END ERROR_CD,
		A.EXEC_FLAG,
		'0' EXPORT_FLAG,
		to_timestamp(SUBSTRING(current_timestamp(), 1, 19)) JCP_CREATE_DATE
	FROM consistency_error_2 A
	INNER JOIN wk_so_planet_no_dup B ON A.JCP_REC_SEQ = B.JCP_REC_SEQ
	),
ct10
AS (
	SELECT A.JCP_REC_SEQ,
		CASE 
			WHEN error_cd = 'ERR_018'
				THEN 'E018'
			ELSE '0'
			END ERROR_CD,
		A.EXEC_FLAG,
		'0' EXPORT_FLAG,
		to_timestamp(SUBSTRING(current_timestamp(), 1, 19)) JCP_CREATE_DATE
	FROM consistency_error_2 A
	INNER JOIN wk_so_planet_no_dup B ON A.JCP_REC_SEQ = B.JCP_REC_SEQ
	),
ct11
AS (
	SELECT A.JCP_REC_SEQ,
		CASE 
			WHEN error_cd = 'ERR_020'
				THEN 'E020'
			ELSE '0'
			END ERROR_CD,
		A.EXEC_FLAG,
		'0' EXPORT_FLAG,
		to_timestamp(SUBSTRING(current_timestamp(), 1, 19)) JCP_CREATE_DATE
	FROM consistency_error_2 A
	INNER JOIN wk_so_planet_no_dup B ON A.JCP_REC_SEQ = B.JCP_REC_SEQ
	),
a
AS (
	SELECT *
	FROM ct1
	
	UNION
	
	SELECT *
	FROM ct2
	
	UNION
	
	SELECT *
	FROM ct3
	
	UNION
	
	SELECT *
	FROM ct4
	
	UNION
	
	SELECT *
	FROM ct5
	
	UNION
	
	SELECT *
	FROM ct6
	
	UNION
	
	SELECT *
	FROM ct7
	
	UNION
	
	SELECT *
	FROM ct8
	
	UNION
	
	SELECT *
	FROM ct9
	
	UNION
	
	SELECT *
	FROM ct10
	
	UNION
	
	SELECT *
	FROM ct11
	),
trns
AS (
	SELECT A.JCP_REC_SEQ,
		A.ERROR_CD,
		A.EXEC_FLAG,
		A.EXPORT_FLAG,
		A.JCP_CREATE_DATE
	FROM A
	WHERE A.error_cd != '0'
	),
final
AS (
	SELECT jcp_rec_seq::number(10, 0) AS jcp_rec_seq,
		error_cd::VARCHAR(20) AS error_cd,
		exec_flag::VARCHAR(50) AS exec_flag,
		export_flag::VARCHAR(1) AS export_flag,
		jcp_create_date::timestamp_ntz(9) AS jcp_create_date
	FROM trns
	)
SELECT *
FROM final