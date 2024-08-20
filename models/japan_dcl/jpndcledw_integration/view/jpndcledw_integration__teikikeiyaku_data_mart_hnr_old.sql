with c_tbecregularcontract as (
    select * from {{ ref('jpndclitg_integration__c_tbecregularcontract') }}
),
c_tbecregularmeisai as (
    select * from {{ ref('jpndclitg_integration__c_tbecregularmeisai') }}
),
tbecordermeisai as (
    select * from {{ ref('jpndclitg_integration__tbecordermeisai') }}
),
tbecorder as (
    select * from {{ ref('jpndclitg_integration__tbecorder') }}
),
tbecitem as (
    select * from {{ ref('jpndclitg_integration__tbecitem') }}
),
result as(
SELECT DISTINCT rc.c_diregularcontractid,
    lpad((rc.c_diusrid)::TEXT, 10, '0'::TEXT) AS c_diusrid,
    rc.dirouteid,
    to_char(rc.c_diregularcontractdate, 'YYYYMMDD'::TEXT) AS keiyakubi,
    otodoke_shokai.shokai_ym,
    kaiyaku.kaiyakubi,
    rcm.c_dsregularmeisaiid,
    CASE 
        WHEN ((rcm.c_dsdeleveryym)::TEXT = otodoke_shokai.shokai_ym)
            THEN '1'::TEXT
        ELSE '0'::TEXT
        END AS header_flg,
    rcm.c_dsdeleveryym,
    rcm.dsitemid,
    rcm.c_diregularcourseid,
    CASE 
        WHEN ((rcm.c_dsordercreatekbn)::TEXT = '2'::TEXT)
            THEN i.diitemsalesprc
        ELSE (0)::BIGINT
        END AS diitemsalesprc,
    rcm.c_dsordercreatekbn,
    rcm.c_dscontractchangekbn,
    CASE 
        WHEN ((rcm.c_dsdeleveryym)::TEXT >= COALESCE(kaiyaku.kaiyakutuki, '999999'::TEXT))
            THEN '1'::TEXT
        ELSE '0'::TEXT
        END AS c_dicancelflg,
    CASE 
        WHEN (
                ((rcm.c_dsdeleveryym)::TEXT >= COALESCE(kaiyaku.kaiyakutuki, '999999'::TEXT))
                AND ((rcm.c_dsordercreatekbn)::TEXT = '3'::TEXT)
                )
            THEN '解約'::TEXT
        ELSE '有効'::TEXT
        END AS kaiyaku_kbn,
    CASE 
        WHEN ((rcm.c_dsdeleveryym)::TEXT = otodoke_shokai.shokai_ym)
            THEN CASE 
                    WHEN ((rcm.c_dsdeleveryym)::TEXT = COALESCE(kaiyaku.kaiyakutuki, '999999'::TEXT))
                        THEN 'スポット'::TEXT
                    ELSE '新規'::TEXT
                    END
        WHEN ((rcm.c_dsdeleveryym)::TEXT < COALESCE(kaiyaku.kaiyakutuki, '999999'::TEXT))
            THEN CASE 
                    WHEN ((rcm.c_dscontractchangekbn)::TEXT = (3)::TEXT)
                        THEN '休止'::TEXT
                    ELSE '継続'::TEXT
                    END
        WHEN ((rcm.c_dsdeleveryym)::TEXT = COALESCE(kaiyaku.kaiyakutuki, '999999'::TEXT))
            THEN '解約'::TEXT
        ELSE '解約以降'::TEXT
        END AS contract_kbn,
    o.diordercode,
    om.c_dikesaiid,
    om.dimeisaiid
FROM (
    (
        (
            (
                (
                    (
                        c_tbecregularcontract rc JOIN c_tbecregularmeisai rcm ON (
                                (
                                    (
                                        (rc.c_diregularcontractid = rcm.c_diregularcontractid)
                                        AND ((rc.dielimflg)::TEXT = (0)::TEXT)
                                        )
                                    AND ((rcm.dielimflg)::TEXT = (0)::TEXT)
                                    )
                                )
                        ) JOIN (
                        SELECT rcm.c_diregularcontractid,
                            min((rcm.c_dsdeleveryym)::TEXT) AS shokai_ym
                        FROM c_tbecregularmeisai rcm
                        WHERE ((rcm.dielimflg)::TEXT = (0)::TEXT)
                        GROUP BY rcm.c_diregularcontractid
                        ) otodoke_shokai ON ((rc.c_diregularcontractid = otodoke_shokai.c_diregularcontractid))
                    ) LEFT JOIN tbecordermeisai om ON (
                        (
                            (rcm.c_dsregularmeisaiid = om.c_dsregularmeisaiid)
                            AND ((om.dielimflg)::TEXT = (0)::TEXT)
                            )
                        )
                ) LEFT JOIN tbecorder o ON (
                    (
                        (om.diorderid = o.diorderid)
                        AND ((o.dielimflg)::TEXT = (0)::TEXT)
                        )
                    )
            ) LEFT JOIN tbecitem i ON (
                (
                    (rcm.diid = i.diid)
                    AND ((i.dielimflg)::TEXT = (0)::TEXT)
                    )
                )
        ) LEFT JOIN (
        SELECT rcm.c_diregularcontractid,
            min((rcm.c_dsdeleveryym)::TEXT) AS kaiyakutuki,
            min(to_char(rcm.c_dstodokedate, 'YYYYMMDD'::TEXT)) AS kaiyakubi
        FROM c_tbecregularmeisai rcm
        WHERE (
                (
                    ((rcm.c_dsordercreatekbn)::TEXT = '3'::TEXT)
                    AND ((rcm.c_dscontractchangekbn)::TEXT = '4'::TEXT)
                    )
                AND ((rcm.c_dsschedulechg08kbn)::TEXT = '41'::TEXT)
                )
        GROUP BY rcm.c_diregularcontractid
        ) kaiyaku ON ((rc.c_diregularcontractid = kaiyaku.c_diregularcontractid))
    )
)

select * from result