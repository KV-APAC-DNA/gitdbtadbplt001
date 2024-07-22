with dm_integration_dly as(
    select * from SNAPJPNEDW_INTEGRATION.dm_integration_dly
),
mt_cld as(
    select * from SNAPJPNEDW_INTEGRATION.mt_cld
),
mt_bravo_sap_map as(
    select * from SNAPJPNEDW_INTEGRATION.mt_bravo_sap_map
),
union1 as(
    SELECT cld.year_445
        ,cld.month_445
        ,cld.half_445
        ,(
            "left" (
                (cld.quarter_445)::TEXT
                ,1
                )
            )::CHARACTER VARYING AS quarter_445
        ,"map".bravo_account_cd
        ,sum(dm.jcp_amt) AS jcp_amt
    FROM (
        (
            dm_integration_dly dm JOIN mt_cld cld ON ((dm.jcp_date = cld.ymd_dt))
            ) JOIN mt_bravo_sap_map "map" ON (((dm.jcp_account)::TEXT = ("map".sap_account_cd)::TEXT))
        )
    WHERE ((dm.jcp_data_source)::TEXT = 'SI'::TEXT)
    GROUP BY cld.year_445
        ,cld.month_445
        ,cld.half_445
        ,cld.quarter_445
        ,"map".bravo_account_cd
),
union2 as(
    SELECT cld.year_445
        ,cld.month_445
        ,cld.half_445
        ,(
            "left" (
                (cld.quarter_445)::TEXT
                ,1
                )
            )::CHARACTER VARYING AS quarter_445
        ,'0099999999' AS bravo_account_cd
        ,sum(dm.jcp_amt) AS jcp_amt
    FROM (
        (
            dm_integration_dly dm JOIN mt_cld cld ON ((dm.jcp_date = cld.ymd_dt))
            ) JOIN mt_bravo_sap_map "map" ON (((dm.jcp_account)::TEXT = ("map".sap_account_cd)::TEXT))
        )
    WHERE ((dm.jcp_data_source)::TEXT = 'SI'::TEXT)
    GROUP BY cld.year_445
        ,cld.month_445
        ,cld.half_445
        ,cld.quarter_445
),
transformed as(
    SELECT * from union1
    union all
    select * from union2
)
select * from transformed