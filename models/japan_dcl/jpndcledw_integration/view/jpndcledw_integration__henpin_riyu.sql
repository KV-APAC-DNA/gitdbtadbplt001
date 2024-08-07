WITH tbechenpinriyu
AS (
    SELECT *
    FROM {{ref('jpndclitg_integration__tbechenpinriyu')}}
    ),
final
AS (
    SELECT tbechenpinriyu.dihenpinriyuid,
        tbechenpinriyu.dshenpinriyu,
        tbechenpinriyu.dshenpinriyushosai
    FROM tbechenpinriyu
    )
SELECT *
FROM final
