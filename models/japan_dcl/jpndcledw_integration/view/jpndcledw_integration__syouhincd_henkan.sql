with syouhincd_henkan_sub as (
    select * from dev_dna_core.snapjpdcledw_integration.syouhincd_henkan_sub
),

sscmnhin as (
    select * from dev_dna_core.snapjpdclitg_integration.sscmnhin
),

hanyo_attr as (
    select * from dev_dna_core.snapjpdcledw_integration.hanyo_attr
),

cte1 as (
SELECT syouhincd_henkan_sub.itemcode,
    syouhincd_henkan_sub.koseiocode
FROM syouhincd_henkan_sub syouhincd_henkan_sub
),

cte2 as (
SELECT hin.bar_cd2 AS itemcode,
    ("max" ((hin.hin_cd)::TEXT))::CHARACTER VARYING AS koseiocode
FROM (
    sscmnhin hin JOIN hanyo_attr hanyo ON (
            (
                ((hanyo.kbnmei)::TEXT = ('KAISYA'::CHARACTER VARYING)::TEXT)
                AND ((hin.kaisha_cd)::TEXT = (hanyo.attr1)::TEXT)
                )
            )
    )
WHERE (
        (
            NOT (
                EXISTS (
                    SELECT syo.itemcode,
                        syo.koseiocode
                    FROM syouhincd_henkan_sub syo
                    WHERE ((syo.itemcode)::TEXT = (hin.bar_cd2)::TEXT)
                    )
                )
            )
        AND (hin.bar_cd2 IS NOT NULL)
        )
GROUP BY hin.bar_cd2
),

final as (
    select * from cte1
    union all
    select * from cte2
)

select * from final