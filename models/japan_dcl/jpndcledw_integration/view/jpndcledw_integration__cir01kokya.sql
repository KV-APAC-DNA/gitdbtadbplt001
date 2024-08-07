WITH c_tbecusrcomment AS
(
    SELECT * FROM dev_dna_core.snapjpdclitg_integration.c_tbecusrcomment
),

final AS
(
    SELECT lpad(((c_tbecusrcomment.diecusrid)::CHARACTER VARYING)::TEXT, 10, ('0'::CHARACTER VARYING)::TEXT) AS kokyano,
		c_tbecusrcomment.c_dsusrcommentclasskbn AS rirebuncode,
		(to_char(((c_tbecusrcomment.c_dsusrcommentdate)::DATE)::TIMESTAMP without TIME zone, ('YYYYMMDD'::CHARACTER VARYING)::TEXT))::INTEGER AS ukedate,
		c_tbecusrcomment.c_dsusrcomment AS comment1,
		(COALESCE(c_tbecusrcomment.diprepusr, (0)::BIGINT))::CHARACTER VARYING AS insertid,
		(to_char(((c_tbecusrcomment.dsren)::DATE)::TIMESTAMP without TIME zone, ('YYYYMMDD'::CHARACTER VARYING)::TEXT))::INTEGER AS updatedate
	FROM c_tbecusrcomment
)

SELECT * FROM final