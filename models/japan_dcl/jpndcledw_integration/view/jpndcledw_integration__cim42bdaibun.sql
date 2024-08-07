WITH tbpromotioncate AS
(
    SELECT * FROM dev_dna_core.snapjpdclitg_integration.tbpromotioncate
),

final AS
(
    SELECT b.dipromprtcateid AS daibuncode,
		a.dsname AS daibunname
	FROM tbpromotioncate a,
		(
			SELECT tbpromotioncate.dipromprtcateid
			FROM tbpromotioncate tbpromotioncate
			GROUP BY tbpromotioncate.dipromprtcateid
			) b
	WHERE (b.dipromprtcateid = a.dipromcateid)
)

SELECT * FROM final