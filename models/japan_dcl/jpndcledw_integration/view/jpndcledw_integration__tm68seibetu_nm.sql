WITH hanyo_attr AS
(
    SELECT * FROM dev_dna_core.snapjpdcledw_integration.hanyo_attr
),

final AS
(
    SELECT hanyo_attr.attr1 AS kbncode,
        hanyo_attr.attr2 AS kbnname
    FROM hanyo_attr
    WHERE ((hanyo_attr.kbnmei)::TEXT = ('SEIBETSU'::CHARACTER VARYING)::TEXT)
)

SELECT * FROM final