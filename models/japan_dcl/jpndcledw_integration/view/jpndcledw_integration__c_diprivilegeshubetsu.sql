WITH c_tbecprivilegemst
AS (
	SELECT *
	FROM {{ref('jpndclitg_integration__c_tbecprivilegemst')}}
	)
	
	,transformed
AS (
	SELECT c_tbecprivilegemst.c_diprivilegeshubetsu
		,CASE 
			WHEN (
					((c_tbecprivilegemst.c_diprivilegeshubetsu)::TEXT = ((1)::CHARACTER VARYING)::TEXT)
					OR (
						(c_tbecprivilegemst.c_diprivilegeshubetsu IS NULL)
						AND (1 IS NULL)
						)
					)
				THEN 'プレゼント'::CHARACTER VARYING
			WHEN (
					((c_tbecprivilegemst.c_diprivilegeshubetsu)::TEXT = ((2)::CHARACTER VARYING)::TEXT)
					OR (
						(c_tbecprivilegemst.c_diprivilegeshubetsu IS NULL)
						AND (2 IS NULL)
						)
					)
				THEN 'ポイント'::CHARACTER VARYING
			WHEN (
					((c_tbecprivilegemst.c_diprivilegeshubetsu)::TEXT = ((3)::CHARACTER VARYING)::TEXT)
					OR (
						(c_tbecprivilegemst.c_diprivilegeshubetsu IS NULL)
						AND (3 IS NULL)
						)
					)
				THEN '値引'::CHARACTER VARYING
			WHEN (
					((c_tbecprivilegemst.c_diprivilegeshubetsu)::TEXT = ((4)::CHARACTER VARYING)::TEXT)
					OR (
						(c_tbecprivilegemst.c_diprivilegeshubetsu IS NULL)
						AND (4 IS NULL)
						)
					)
				THEN '割引'::CHARACTER VARYING
			WHEN (
					((c_tbecprivilegemst.c_diprivilegeshubetsu)::TEXT = ((5)::CHARACTER VARYING)::TEXT)
					OR (
						(c_tbecprivilegemst.c_diprivilegeshubetsu IS NULL)
						AND (5 IS NULL)
						)
					)
				THEN '送料無料'::CHARACTER VARYING
			WHEN (
					((c_tbecprivilegemst.c_diprivilegeshubetsu)::TEXT = ((6)::CHARACTER VARYING)::TEXT)
					OR (
						(c_tbecprivilegemst.c_diprivilegeshubetsu IS NULL)
						AND (6 IS NULL)
						)
					)
				THEN '代引き手数料無料'::CHARACTER VARYING
			WHEN (
					((c_tbecprivilegemst.c_diprivilegeshubetsu)::TEXT = ((7)::CHARACTER VARYING)::TEXT)
					OR (
						(c_tbecprivilegemst.c_diprivilegeshubetsu IS NULL)
						AND (7 IS NULL)
						)
					)
				THEN '当日配送手数料無料'::CHARACTER VARYING
			ELSE '該当なし'::CHARACTER VARYING
			END AS c_diprivilegeshubetsuname
		,(
			((c_tbecprivilegemst.c_diprivilegeshubetsu)::TEXT || (':'::CHARACTER VARYING)::TEXT) || (
				CASE 
					WHEN (
							((c_tbecprivilegemst.c_diprivilegeshubetsu)::TEXT = ((1)::CHARACTER VARYING)::TEXT)
							OR (
								(c_tbecprivilegemst.c_diprivilegeshubetsu IS NULL)
								AND (1 IS NULL)
								)
							)
						THEN 'プレゼント'::CHARACTER VARYING
					WHEN (
							((c_tbecprivilegemst.c_diprivilegeshubetsu)::TEXT = ((2)::CHARACTER VARYING)::TEXT)
							OR (
								(c_tbecprivilegemst.c_diprivilegeshubetsu IS NULL)
								AND (2 IS NULL)
								)
							)
						THEN 'ポイント'::CHARACTER VARYING
					WHEN (
							((c_tbecprivilegemst.c_diprivilegeshubetsu)::TEXT = ((3)::CHARACTER VARYING)::TEXT)
							OR (
								(c_tbecprivilegemst.c_diprivilegeshubetsu IS NULL)
								AND (3 IS NULL)
								)
							)
						THEN '値引'::CHARACTER VARYING
					WHEN (
							((c_tbecprivilegemst.c_diprivilegeshubetsu)::TEXT = ((4)::CHARACTER VARYING)::TEXT)
							OR (
								(c_tbecprivilegemst.c_diprivilegeshubetsu IS NULL)
								AND (4 IS NULL)
								)
							)
						THEN '割引'::CHARACTER VARYING
					WHEN (
							((c_tbecprivilegemst.c_diprivilegeshubetsu)::TEXT = ((5)::CHARACTER VARYING)::TEXT)
							OR (
								(c_tbecprivilegemst.c_diprivilegeshubetsu IS NULL)
								AND (5 IS NULL)
								)
							)
						THEN '送料無料'::CHARACTER VARYING
					WHEN (
							((c_tbecprivilegemst.c_diprivilegeshubetsu)::TEXT = ((6)::CHARACTER VARYING)::TEXT)
							OR (
								(c_tbecprivilegemst.c_diprivilegeshubetsu IS NULL)
								AND (6 IS NULL)
								)
							)
						THEN '代引き手数料無料'::CHARACTER VARYING
					WHEN (
							((c_tbecprivilegemst.c_diprivilegeshubetsu)::TEXT = ((7)::CHARACTER VARYING)::TEXT)
							OR (
								(c_tbecprivilegemst.c_diprivilegeshubetsu IS NULL)
								AND (7 IS NULL)
								)
							)
						THEN '当日配送手数料無料'::CHARACTER VARYING
					ELSE '該当なし'::CHARACTER VARYING
					END
				)::TEXT
			) AS c_diprivilegeshubetsuname_ren
	FROM c_tbecprivilegemst c_tbecprivilegemst
	)

SELECT *
FROM transformed
