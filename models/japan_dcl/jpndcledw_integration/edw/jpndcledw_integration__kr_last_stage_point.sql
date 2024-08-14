with KR_LAST_STAGE_POINT_WK_NAYOSE as(
    select * from {{ ref('jpndcledw_integration__kr_last_stage_point_wk_nayose') }}
),
TBUSRPRAM as(
    select * from {{ ref('jpndclitg_integration__tbusrpram') }}
),
transformed as(
    SELECT DISTINCT YYYYMM::varchar(9) as yyyymm,
        USRID::number(38,0) as usrid,
        NVL(THISTOTALPRC, 0)::number(38,18) as thistotalprc,
        STAGE::varchar(18) as stage,
        STAGE_CD::varchar(2) as stage_cd,
        NVL(GOALPOINT, 0)::number(18,0) as goalpoint,
        INSERTDATE::varchar(25) as insertdate
    FROM KR_LAST_STAGE_POINT_WK_NAYOSE WK
    WHERE EXISTS (
            SELECT DIUSRID
            FROM TBUSRPRAM USR
            WHERE WK.USRID = USR.DIUSRID
                AND USR.DISECESSIONFLG = 0
            )
)
select * from transformed
