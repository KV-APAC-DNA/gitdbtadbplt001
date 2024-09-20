WITH tbpromotioncate AS
(
    SELECT * FROM {{ ref('jpndclitg_integration__tbpromotioncate') }}
),

final AS
(
    SELECT (tbpromotioncate.dipromcateid)::CHARACTER VARYING AS syobuncode,
			tbpromotioncate.dsname AS syobunname,
			(tbpromotioncate.dipromprtcateid)::CHARACTER VARYING AS daibuncode
		FROM tbpromotioncate
)

SELECT * FROM final