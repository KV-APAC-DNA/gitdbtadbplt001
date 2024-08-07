WITH tbpromotion AS
(
    SELECT * FROM dev_dna_core.snapjpdclitg_integration.tbpromotion
),

final AS
(
    SELECT tbpromotion.dipromid,
        tbpromotion.dspromcode AS baitaicode,
        tbpromotion.dspromname AS baitainame,
        (tbpromotion.dipromcateid)::CHARACTER VARYING AS sbunruicode
    FROM tbpromotion
    WHERE ((tbpromotion.dielimflg)::TEXT = ((0)::CHARACTER VARYING)::TEXT)
)

SELECT * FROM final