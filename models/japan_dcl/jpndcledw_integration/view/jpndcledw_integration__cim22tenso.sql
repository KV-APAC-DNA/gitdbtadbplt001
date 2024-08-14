WITH c_tbecclient AS
(
    SELECT * from {{ ref('jpndclitg_integration__c_tbecclient') }}
),

final AS
(
SELECT 
    s.c_dstempocode AS sokocode,
    s.c_dstemponame AS sokoname
FROM c_tbecclient s
)

SELECT * FROM final 