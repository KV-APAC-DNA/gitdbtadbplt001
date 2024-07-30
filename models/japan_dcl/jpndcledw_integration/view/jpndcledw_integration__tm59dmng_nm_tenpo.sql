with hanyo_attr
as
(
    SELECT * FROM jpdcledw_integration.hanyo_attr
),
final as
(
    SELECT hanyo_attr.attr1 AS dmngflg
    FROM  hanyo_attr
    WHERE ((hanyo_attr.kbnmei)::TEXT = ('DMNGFLG_TENPO'::CHARACTER VARYING)::TEXT)
)
SELECT * FROM final