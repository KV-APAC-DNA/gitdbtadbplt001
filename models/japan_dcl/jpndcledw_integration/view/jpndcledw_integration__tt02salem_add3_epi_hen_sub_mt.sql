with c_tbecinquiremeisai as (
    select * from {{ ref('jpndclitg_integration__c_tbecinquiremeisai') }}
),
c_tbecinquirekesai as (
    select * from {{ ref('jpndclitg_integration__c_tbecinquirekesai') }}
),
c_tbeckesai as (
    select * from {{ ref('jpndclitg_integration__c_tbeckesai') }}
),

final as (
SELECT (('H'::CHARACTER VARYING)::TEXT || ((c_tbecinquiremeisai.diinquirekesaiid)::CHARACTER VARYING)::TEXT) AS saleno,
    COALESCE(c_tbecinquiremeisai.diordermeisaiid, (0)::BIGINT) AS gyono,
    'ポイント交換商品' AS meisaikbn,
    c_tbecinquiremeisai.dsitemid AS itemcode,
    COALESCE(c_tbecinquiremeisai.c_dihenpinnum, (0)::BIGINT) AS suryo,
    (- 1 * (COALESCE(c_tbecinquiremeisai.c_disetitemprc, (0)::BIGINT) - c_tbecinquiremeisai.c_didiscountmeisai)) AS tanka,
    (- 1 * ((COALESCE(c_tbecinquiremeisai.c_disetitemprc, (0)::BIGINT) * COALESCE(c_tbecinquiremeisai.c_dihenpinnum, (0)::BIGINT)) - (COALESCE(c_tbecinquiremeisai.c_didiscountmeisai, (0)::BIGINT) * COALESCE(c_tbecinquiremeisai.c_dihenpinnum, (0)::BIGINT)))) AS kingaku,
    (
        (- (1)::DOUBLE PRECISION) * ceil((
                (
                    ((COALESCE(c_tbecinquiremeisai.c_disetitemprc, (0)::BIGINT) * COALESCE(c_tbecinquiremeisai.c_dihenpinnum, (0)::BIGINT)) - (COALESCE(c_tbecinquiremeisai.c_didiscountmeisai, (0)::BIGINT) * COALESCE(c_tbecinquiremeisai.c_dihenpinnum, (0)::BIGINT))) / CASE 
                        WHEN (
                                (c_tbecinquiremeisai.diitemprc = 0)
                                OR (
                                    (c_tbecinquiremeisai.diitemprc IS NULL)
                                    AND (0 IS NULL)
                                    )
                                )
                            THEN (1)::BIGINT
                        ELSE CASE 
                                WHEN (
                                        (c_tbecinquiremeisai.c_disetitemprc = 0)
                                        OR (
                                            (c_tbecinquiremeisai.c_disetitemprc IS NULL)
                                            AND (0 IS NULL)
                                            )
                                        )
                                    THEN (1)::BIGINT
                                ELSE (c_tbecinquiremeisai.c_disetitemprc / c_tbecinquiremeisai.diitemprc)
                                END
                        END
                    )
                )::DOUBLE PRECISION)
        ) AS meisainukikingaku,
    COALESCE(c_tbecinquiremeisai.c_didiscountrate, (0)::SMALLINT) AS wariritu,
    (- 1 * COALESCE(c_tbecinquiremeisai.c_disetitemprc, (0)::BIGINT)) AS warimaekomitanka,
    (- 1 * COALESCE((c_tbecinquiremeisai.c_disetitemprc * c_tbecinquiremeisai.c_dihenpinnum), (0)::BIGINT)) AS warimaenukikingaku,
    (- 1 * COALESCE((c_tbecinquiremeisai.c_disetitemprc * c_tbecinquiremeisai.c_dihenpinnum), (0)::BIGINT)) AS warimaekomikingaku,
    (c_tbecinquiremeisai.c_dikesaiid)::CHARACTER VARYING AS dispsaleno,
    c_tbecinquiremeisai.c_dikesaiid AS kesaiid,
    COALESCE(c_tbecinquirekesai.c_dshenpinsts, '0'::CHARACTER VARYING) AS henpinsts
FROM c_tbecinquiremeisai,
    c_tbecinquirekesai,
    c_tbeckesai
WHERE (
        (
            (
                (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            (c_tbecinquiremeisai.diinquireid = c_tbecinquirekesai.diinquireid)
                                            AND (c_tbecinquiremeisai.diinquirekesaiid = c_tbecinquirekesai.c_diinquirekesaiid)
                                            )
                                        AND (c_tbecinquirekesai.c_dikesaiid = c_tbeckesai.c_dikesaiid)
                                        )
                                    AND ((c_tbeckesai.dicancel)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
                                    )
                                AND (c_tbecinquiremeisai.disetid = c_tbecinquiremeisai.diid)
                                )
                            AND ((c_tbecinquiremeisai.c_dspointitemflg)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
                            )
                        AND ((c_tbecinquiremeisai.c_diitemtype)::TEXT <> ('08'::CHARACTER VARYING)::TEXT)
                        )
                    AND ((c_tbecinquiremeisai.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
                    )
                AND ((c_tbecinquirekesai.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
                )
            AND ((c_tbeckesai.dielimflg)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
            )
        AND (
            ((COALESCE(c_tbecinquirekesai.c_dshenpinsts, '0'::CHARACTER VARYING))::TEXT = ('3010'::CHARACTER VARYING)::TEXT)
            OR ((COALESCE(c_tbecinquirekesai.c_dshenpinsts, '0'::CHARACTER VARYING))::TEXT = ('5020'::CHARACTER VARYING)::TEXT)
            )
        )
)

select * from final