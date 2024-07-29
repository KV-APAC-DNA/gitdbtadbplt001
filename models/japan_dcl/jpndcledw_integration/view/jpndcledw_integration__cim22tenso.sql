WITH source AS
(
    SELECT * FROM dev_dna_core.snapjpdclitg_integration.c_tbecclient
),

final AS
(
SELECT 
    s.c_dstempocode AS sokocode,
    s.c_dstemponame AS sokoname
FROM source s
)

SELECT * FROM final