WITH tbusrpram AS
(
    SELECT * FROM dev_dna_core.snapjpdclitg_integration.tbusrpram
),

final AS
(
    SELECT tbusrpram.dsdat7 AS cardkokyano,
		lpad(((tbusrpram.diusrid)::CHARACTER VARYING)::TEXT, 10, ((0)::CHARACTER VARYING)::TEXT) AS kokyano
	FROM tbusrpram
)

SELECT * FROM final