WITH c_tbeckesai
AS (
	SELECT *
	FROM dev_dna_core.snapjpdclitg_integration.c_tbeckesai
	)
	
	,c_tbecorderhistory
AS (
	SELECT *
	FROM dev_dna_core.snapjpdclitg_integration.c_tbecorderhistory
	)
	
	,tbecorderhist_qv
AS (
	SELECT *
	FROM dev_dna_core.snapjpdcledw_integration.tbecorderhist_qv
	)
	
	,c_tbeckesaihistory
AS (
	SELECT *
	FROM dev_dna_core.snapjpdclitg_integration.c_tbeckesaihistory
	)
	
	,c_tbecprivilegekesaihistory
AS (
	SELECT *
	FROM dev_dna_core.snapjpdclitg_integration.c_tbecprivilegekesaihistory
	)
	
	,c_tbecprivilegemst
AS (
	SELECT *
	FROM dev_dna_core.snapjpdcledw_integration.c_tbecprivilegemst
	)
	
	,c_tbecordermeisaihistory
AS (
	SELECT *
	FROM dev_dna_core.snapjpdclitg_integration.c_tbecordermeisaihistory
	)
	
	,transformed_cte_1
AS (
	(
		(
			SELECT ((('U'::CHARACTER VARYING)::TEXT || ((c_tbeckesai.c_dikesaiid)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS saleno
				,9000000001::BIGINT AS gyono
				,'利用ポイント数(交換以外)' AS meisaikbn
				,'X000000001' AS itemcode
				,1 AS suryo
				,(- COALESCE(c_tbeckesai.diusepoint, (0)::BIGINT)) AS tanka
				,(- COALESCE(c_tbeckesai.diusepoint, (0)::BIGINT)) AS kingaku
				,(- COALESCE(c_tbeckesai.diusepoint, (0)::BIGINT)) AS meisainukikingaku
				,0 AS wariritu
				,(- COALESCE(c_tbeckesai.diusepoint, (0)::BIGINT)) AS warimaekomitanka
				,(- COALESCE(c_tbeckesai.diusepoint, (0)::BIGINT)) AS warimaenukikingaku
				,(- COALESCE(c_tbeckesai.diusepoint, (0)::BIGINT)) AS warimaekomikingaku
				,(c_tbeckesai.c_dikesaiid)::CHARACTER VARYING AS dispsaleno
				,c_tbeckesai.c_dikesaiid AS kesaiid
				,tbecorder.diordercode
			FROM c_tbeckesaihistory c_tbeckesai
				,c_tbecorderhistory tbecorder
				,tbecorderhist_qv tbecorderhist_qv
			WHERE (
					(
						(
							(
								(
									(
										(c_tbeckesai.diorderid = tbecorder.diorderid)
										AND (tbecorderhist_qv.diorderhistid = tbecorder.diorderhistid)
										)
									AND (c_tbeckesai.diorderhistid = tbecorder.diorderhistid)
									)
								AND ((c_tbeckesai.dicancel)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
								)
							AND (c_tbeckesai.diusepoint <> 0)
							)
						AND ((c_tbeckesai.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
						)
					AND ((tbecorder.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
					)
			
			UNION ALL
			
			SELECT ((('U'::CHARACTER VARYING)::TEXT || ((c_tbeckesai.c_dikesaiid)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS saleno
				,9000000002::BIGINT AS gyono
				,'利用ポイント数(交換)' AS meisaikbn
				,'X000000002' AS itemcode
				,1 AS suryo
				,(- COALESCE(c_tbeckesai.c_diexchangepoint, (0)::BIGINT)) AS tanka
				,(- COALESCE(c_tbeckesai.c_diexchangepoint, (0)::BIGINT)) AS kingaku
				,(- COALESCE(c_tbeckesai.c_diexchangepoint, (0)::BIGINT)) AS meisainukikingaku
				,0 AS wariritu
				,(- COALESCE(c_tbeckesai.c_diexchangepoint, (0)::BIGINT)) AS warimaekomitanka
				,(- COALESCE(c_tbeckesai.c_diexchangepoint, (0)::BIGINT)) AS warimaenukikingaku
				,(- COALESCE(c_tbeckesai.c_diexchangepoint, (0)::BIGINT)) AS warimaekomikingaku
				,(c_tbeckesai.c_dikesaiid)::CHARACTER VARYING AS dispsaleno
				,c_tbeckesai.c_dikesaiid AS kesaiid
				,tbecorder.diordercode
			FROM c_tbeckesaihistory c_tbeckesai
				,c_tbecorderhistory tbecorder
				,tbecorderhist_qv tbecorderhist_qv
			WHERE (
					(
						(
							(
								(
									(
										(c_tbeckesai.diorderid = tbecorder.diorderid)
										AND (tbecorderhist_qv.diorderhistid = tbecorder.diorderhistid)
										)
									AND (c_tbeckesai.diorderhistid = tbecorder.diorderhistid)
									)
								AND ((c_tbeckesai.dicancel)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
								)
							AND (c_tbeckesai.c_diexchangepoint <> 0)
							)
						AND ((c_tbeckesai.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
						)
					AND ((tbecorder.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
					)
			)
		
		UNION ALL
		
		SELECT ((('U'::CHARACTER VARYING)::TEXT || ((c_tbeckesai.c_dikesaiid)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS saleno
			,(((((c_tbecprivilegekesai.c_diprivilegeid)::CHARACTER VARYING)::TEXT || ((c_tbecprivilegekesai.diorderid)::CHARACTER VARYING)::TEXT))::NUMERIC)::NUMERIC(18, 0) AS gyono
			,'特典' AS meisaikbn
			,(c_tbecprivilegekesai.c_diprivilegeid)::CHARACTER VARYING AS itemcode
			,COALESCE(c_tbecprivilegekesai.c_diprivilegenum, (0)::BIGINT) AS suryo
			,CASE 
				WHEN (
						((c_tbecprivilegemst.c_diprivilegeshubetsu)::TEXT = ((3)::CHARACTER VARYING)::TEXT)
						OR (
							(c_tbecprivilegemst.c_diprivilegeshubetsu IS NULL)
							AND (3 IS NULL)
							)
						)
					THEN (- COALESCE(c_tbecprivilegekesai.c_diprivilegeprc, (0)::BIGINT))
				ELSE (0)::BIGINT
				END AS tanka
			,CASE 
				WHEN (
						((c_tbecprivilegemst.c_diprivilegeshubetsu)::TEXT = ((3)::CHARACTER VARYING)::TEXT)
						OR (
							(c_tbecprivilegemst.c_diprivilegeshubetsu IS NULL)
							AND (3 IS NULL)
							)
						)
					THEN (- COALESCE(c_tbecprivilegekesai.c_diprivilegetotalprc, (0)::BIGINT))
				ELSE (0)::BIGINT
				END AS kingaku
			,CASE 
				WHEN (
						((c_tbecprivilegemst.c_diprivilegeshubetsu)::TEXT = ((3)::CHARACTER VARYING)::TEXT)
						OR (
							(c_tbecprivilegemst.c_diprivilegeshubetsu IS NULL)
							AND (3 IS NULL)
							)
						)
					THEN (- ceil(((COALESCE(c_tbecprivilegekesai.c_diprivilegetotalprc, (0)::BIGINT) / ((100 + tbecorder.ditaxrate) / 100)))::DOUBLE PRECISION))
				ELSE (0)::DOUBLE PRECISION
				END AS meisainukikingaku
			,0 AS wariritu
			,0 AS warimaekomitanka
			,0 AS warimaenukikingaku
			,0 AS warimaekomikingaku
			,(c_tbeckesai.c_dikesaiid)::CHARACTER VARYING AS dispsaleno
			,c_tbeckesai.c_dikesaiid AS kesaiid
			,tbecorder.diordercode
		FROM c_tbeckesaihistory c_tbeckesai
			,c_tbecorderhistory tbecorder
			,c_tbecprivilegekesaihistory c_tbecprivilegekesai
			,c_tbecprivilegemst c_tbecprivilegemst
			,tbecorderhist_qv tbecorderhist_qv
		WHERE (
				(
					(
						(
							(
								(
									(
										(
											(
												(
													(
														(
															(c_tbeckesai.diorderid = tbecorder.diorderid)
															AND (tbecorderhist_qv.diorderhistid = tbecorder.diorderhistid)
															)
														AND (c_tbeckesai.diorderhistid = tbecorder.diorderhistid)
														)
													AND (c_tbeckesai.c_dikesaiid = c_tbecprivilegekesai.c_dikesaiid)
													)
												AND (tbecorder.diorderhistid = c_tbecprivilegekesai.diorderhistid)
												)
											AND (c_tbecprivilegekesai.c_diprivilegeid = c_tbecprivilegemst.c_diprivilegeid)
											)
										AND ((c_tbeckesai.dicancel)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
										)
									AND ((c_tbecprivilegekesai.c_dinondispflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
									)
								AND ((c_tbeckesai.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
								)
							AND ((tbecorder.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
							)
						AND ((c_tbecprivilegekesai.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
						)
					AND ((c_tbecprivilegemst.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
					)
				AND ((c_tbecprivilegekesai.c_dstekiyojyogaiflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
				)
		)
	
	UNION ALL
	
	SELECT ((('U'::CHARACTER VARYING)::TEXT || ((tbecordermeisai.c_dikesaiid)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS saleno
		,COALESCE(tbecordermeisai.dimeisaiid, (0)::BIGINT) AS gyono
		,'ポイント交換商品' AS meisaikbn
		,tbecordermeisai.dsitemid AS itemcode
		,tbecordermeisai.diitemnum AS suryo
		,CASE 
			WHEN (
					(tbecordermeisai.diitemnum = 0)
					OR (
						(tbecordermeisai.diitemnum IS NULL)
						AND (0 IS NULL)
						)
					)
				THEN (0)::BIGINT
			ELSE ((COALESCE(tbecordermeisai.c_diitemtotalprc, (0)::BIGINT) - COALESCE(tbecordermeisai.c_didiscountmeisai, (0)::BIGINT)) / COALESCE(tbecordermeisai.diitemnum, (1)::BIGINT))
			END AS tanka
		,(COALESCE(tbecordermeisai.c_diitemtotalprc, (0)::BIGINT) - COALESCE(tbecordermeisai.c_didiscountmeisai, (0)::BIGINT)) AS kingaku
		,(COALESCE(tbecordermeisai.c_diitemtotalprc, (0)::BIGINT) - COALESCE(tbecordermeisai.c_didiscountmeisai, (0)::BIGINT)) AS meisainukikingaku
		,COALESCE((tbecordermeisai.c_didiscountrate)::INTEGER, 0) AS wariritu
		,COALESCE(tbecordermeisai.ditotalprc, (0)::BIGINT) AS warimaekomitanka
		,COALESCE((tbecordermeisai.diusualprc * tbecordermeisai.diitemnum), (0)::BIGINT) AS warimaenukikingaku
		,COALESCE(tbecordermeisai.c_diitemtotalprc, (0)::BIGINT) AS warimaekomikingaku
		,(tbecordermeisai.c_dikesaiid)::CHARACTER VARYING AS dispsaleno
		,tbecordermeisai.c_dikesaiid AS kesaiid
		,tbecorder.diordercode
	FROM c_tbecordermeisaihistory tbecordermeisai
		,c_tbeckesaihistory c_tbeckesai
		,c_tbecorderhistory tbecorder
		,tbecorderhist_qv tbecorderhist_qv
	WHERE (
			(
				(
					(
						(
							(
								(
									(
										(
											(
												(tbecordermeisai.c_dikesaiid = c_tbeckesai.c_dikesaiid)
												AND (tbecorderhist_qv.diorderhistid = tbecorder.diorderhistid)
												)
											AND (tbecordermeisai.diorderhistid = c_tbeckesai.diorderhistid)
											)
										AND (tbecordermeisai.diorderid = tbecorder.diorderid)
										)
									AND (tbecordermeisai.diorderhistid = tbecorder.diorderhistid)
									)
								AND ((c_tbeckesai.dicancel)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
								)
							AND (tbecordermeisai.disetid = tbecordermeisai.diid)
							)
						AND ((tbecordermeisai.c_dspointitemflg)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
						)
					AND ((tbecorder.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
					)
				AND ((c_tbeckesai.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
				)
			AND ((tbecordermeisai.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
			)
	)
	
SELECT *
FROM transformed_cte_1
