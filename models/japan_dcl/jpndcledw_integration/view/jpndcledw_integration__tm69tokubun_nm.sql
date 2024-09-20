WITH hanyo_attr AS
(
    SELECT * FROM {{ source('jpdcledw_integration', 'hanyo_attr_bkp') }}
),

final AS
(
    SELECT hanyo_attr.attr1 AS code,
            hanyo_attr.attr2 AS name,
            (((hanyo_attr.attr1)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (hanyo_attr.attr2)::TEXT) AS cname
        FROM hanyo_attr
        WHERE ((hanyo_attr.kbnmei)::TEXT = ('TOKUBUNRUI'::CHARACTER VARYING)::TEXT)
)

SELECT * FROM final