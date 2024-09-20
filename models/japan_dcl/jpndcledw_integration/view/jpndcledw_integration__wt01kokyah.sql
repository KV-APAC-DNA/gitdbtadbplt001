WITH cir01kokya
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__cir01kokya') }}
	)
	,cim05opera
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__cim05opera') }}
	)
	,cim04bumon
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__cim04bumon') }}
	)
	,tm58rireki_nm
AS (
	SELECT *
	FROM {{ ref('jpndcledw_integration__tm58rireki_nm') }}
	)
	,transformed as (SELECT CASE 
		WHEN ((len((cir01.ukedate)::CHARACTER VARYING)::TEXT) = 8)
			THEN (substring(((cir01.ukedate)::CHARACTER VARYING)::TEXT,1,4))::CHARACTER VARYING
		ELSE NULL::CHARACTER VARYING
		END AS ukeyear
	,CASE 
		WHEN (length(((cir01.ukedate)::CHARACTER VARYING)::TEXT) = 8)
			THEN (
					substring (
						((cir01.ukedate)::CHARACTER VARYING)::TEXT
						,5
						,2
						)
					)::CHARACTER VARYING
		ELSE NULL::CHARACTER VARYING
		END AS ukemonth
	,CASE 
		WHEN (length(((cir01.ukedate)::CHARACTER VARYING)::TEXT) = 8)
			THEN cir01.ukedate
		ELSE 99991231
		END AS ukedate
	,CASE 
		WHEN (length(((cir01.ukedate)::CHARACTER VARYING)::TEXT) = 8)
			THEN (date_part(DAYOFWEEKISO , dateadd(day,1,to_date(cir01.ukedate :: TEXT ,'YYYYMMDD')) ))
		ELSE NULL::CHARACTER VARYING
		END AS ukeyoubicode
	,cir01.rirebuncode
	,COALESCE(tm58.name, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS rirebunname
	,COALESCE(tm58.cname, (('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535))::TEXT) AS rirebuncname
	,cim05.bumoncode
	,COALESCE(cim04.name, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS bumonname
	,CASE 
		WHEN (
				((cim04.name)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
				OR (
					(cim04.name IS NULL)
					-- AND (NULL::"No value" IS NULL)
					)
				)
			THEN ('その他'::CHARACTER VARYING)::TEXT
		ELSE (((cim04.bumoncode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim04.name)::TEXT)
		END AS bumoncname
	,cim05.logincode AS opecode
	,COALESCE(cim05.opename, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS opename
	,CASE 
		WHEN (
				((cim05.opename)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
				OR (
					(cim05.opename IS NULL)
					-- AND (NULL::"unknown" IS NULL)
					)
				)
			THEN ('その他'::CHARACTER VARYING)::TEXT
		ELSE (((cim05.opecode)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (cim05.opename)::TEXT)
		END AS opecname
	,cir01.kokyano
	,"replace" (
		(cir01.comment1)::TEXT
		,(','::CHARACTER VARYING)::TEXT
		,('、'::CHARACTER VARYING)::TEXT
		) AS comment1 FROM (
	(
		(
			cir01kokya cir01 JOIN cim05opera cim05 ON (
					(
						((cir01.insertid)::TEXT = (cim05.opecode)::TEXT)
						AND ((cim05.ciflg)::TEXT = ('NEXT'::CHARACTER VARYING)::TEXT)
						)
					)
			) LEFT JOIN cim04bumon cim04 ON (
				(
					((cim05.bumoncode)::TEXT = (cim04.bumoncode)::TEXT)
					AND ((cim04.ciflg)::TEXT = ('NEXT'::CHARACTER VARYING)::TEXT)
					)
				)
		) LEFT JOIN tm58rireki_nm tm58 ON (((cir01.rirebuncode)::TEXT = (tm58.code)::TEXT))
	)
    
	)
 SELECT * FROM transformed 
