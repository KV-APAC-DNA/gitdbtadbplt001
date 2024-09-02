with sscmhtoriskbetsuhin as (
    select * from {{ source('jpdclitg_integration', 'sscmhtoriskbetsuhin') }}
),

hanyo_attr as (
    select * from {{ ref('jpndcledw_integration__hanyo_attr') }}
),

final as (
SELECT toriskbetsuhin.torisk_cd,
    toriskbetsuhin.hin_cd,
    toriskbetsuhin.torisk_hin_cd
FROM (
    sscmhtoriskbetsuhin toriskbetsuhin LEFT JOIN hanyo_attr hanyo ON (((hanyo.kbnmei)::TEXT = ('KAISYA'::CHARACTER VARYING)::TEXT))
    )
WHERE ((toriskbetsuhin.kaisha_cd)::TEXT = (hanyo.attr1)::TEXT)
)

select * from final