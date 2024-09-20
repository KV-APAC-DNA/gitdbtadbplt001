with hanyo_attr
AS
(
    SELECT * FROM {{ ref('jpndcledw_integration__hanyo_attr') }}
),
final AS
(
    SELECT hanyo_attr.attr1 AS name
    FROM hanyo_attr
    WHERE ((hanyo_attr.kbnmei)::TEXT = ('SHOKUGYOU'::CHARACTER VARYING)::TEXT)
)
SELECT * FROM final