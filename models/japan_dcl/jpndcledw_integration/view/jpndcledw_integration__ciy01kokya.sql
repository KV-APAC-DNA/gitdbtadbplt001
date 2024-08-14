WITH tbusrpram AS
(
    SELECT * FROM {{ ref('jpndclitg_integration__tbusrpram') }}
),

final AS
(
    SELECT tbusrpram.dsdat7 AS cardkokyano,
		lpad(((tbusrpram.diusrid)::CHARACTER VARYING)::TEXT, 10, ((0)::CHARACTER VARYING)::TEXT) AS kokyano
	FROM tbusrpram
)

SELECT * FROM final