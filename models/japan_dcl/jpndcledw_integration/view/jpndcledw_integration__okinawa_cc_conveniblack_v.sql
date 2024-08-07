with tbecsalesroutemst as (
    select * from dev_dna_core.snapjpdclitg_integration.tbecsalesroutemst
),

tbusrpram as (
    select * from dev_dna_core.snapjpdclitg_integration.tbusrpram
),

tbecorder as (
    select * from dev_dna_core.snapjpdclitg_integration.tbecorder
),
odr_latest as (
    SELECT tbecorder.diecusrid,
            "max" (tbecorder.diorderid) AS diorderid
    FROM tbecorder
    GROUP BY tbecorder.diecusrid
),
cinext as (
    SELECT lpad((usr.diusrid)::TEXT, 10, (0)::TEXT) AS diusrid,
            "max" ((usr.dsname)::TEXT) AS last_name,
            "max" ((usr.dsname2)::TEXT) AS first_name,
            "max" ((usr.dszip)::TEXT) AS post_code,
            "max" ((usr.dspref)::TEXT) AS prefecture,
            ("max" ((COALESCE(usr.dscity, ''::CHARACTER VARYING))::TEXT) || "max" ((COALESCE(usr.dsaddr, ''::CHARACTER VARYING))::TEXT) || "max" ((COALESCE(usr.dstatemono, ''::CHARACTER VARYING))::TEXT)) AS address,
            "max" ((usr.dstel)::TEXT) AS tel_home,
            "max" ((usr.dsdat2)::TEXT) AS tel_cell,
            "max" ((usr.dsdat3)::TEXT) AS tel_other,
            "max" ((usr.dsdat4)::TEXT) AS tel_main,
            "max" (odr.dirouteid) AS dirouteid
        FROM (
            (
                tbusrpram usr JOIN odr_latest ON ((usr.diusrid = odr_latest.diecusrid))
                ) JOIN tbecorder odr ON ((odr_latest.diorderid = odr.diorderid))
            )
        WHERE ((usr.dsdat14)::TEXT = '後払不可'::TEXT)
        GROUP BY usr.diusrid
),

cte1 as (
SELECT '0' AS sort_key,
    '顧客ID' AS diusrid,
    '氏' AS last_name,
    '名' AS first_name,
    '郵便番号' AS post_code,
    '都道府県' AS prefecture,
    '住所' AS address,
    'TEL（固）' AS tel_home,
    'TEL（携）' AS tel_cell,
    'TEL（他）' AS tel_other,
    'メインTEL' AS tel_main,
    '通信経路' AS dsroutename
    ),
    
cte2 as (
SELECT '1' AS sort_key,
    (cinext.diusrid)::CHARACTER VARYING AS diusrid,
    (cinext.last_name)::CHARACTER VARYING AS last_name,
    (cinext.first_name)::CHARACTER VARYING AS first_name,
    (cinext.post_code)::CHARACTER VARYING AS post_code,
    (cinext.prefecture)::CHARACTER VARYING AS prefecture,
    (cinext.address)::CHARACTER VARYING AS address,
    (cinext.tel_home)::CHARACTER VARYING AS tel_home,
    (cinext.tel_cell)::CHARACTER VARYING AS tel_cell,
    (cinext.tel_other)::CHARACTER VARYING AS tel_other,
    (cinext.tel_main)::CHARACTER VARYING AS tel_main,
    rt.dsroutename
FROM ( cinext JOIN tbecsalesroutemst rt ON (
            (
                (cinext.dirouteid = rt.dirouteid)
                AND ((rt.dielimflg)::TEXT = '0'::TEXT)
                )
            )
    )
),

final as (
    select * from cte1
    union all
    select * from cte2
)

select * from final