with hanyo_attr
as
(
    SELECT * FROM {{ ref('jpndcledw_integration__hanyo_attr') }}
),
final as
(
    SELECT hanyo_attr.attr1 AS dmngflg
    FROM  hanyo_attr
    WHERE ((hanyo_attr.kbnmei)::TEXT = ('DMNGFLG_TENPO'::CHARACTER VARYING)::TEXT)
)
SELECT * FROM final