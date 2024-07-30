SELECT hanyo_attr.attr1 AS code,
    COALESCE(hanyo_attr.attr2, 'その他'::CHARACTER VARYING) AS name,
    CASE 
        WHEN (
                ((hanyo_attr.attr2)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
                OR (
                    (hanyo_attr.attr2 IS NULL)
                    AND (NULL::"unknown" IS NULL)
                    )
                )
            THEN ('その他'::CHARACTER VARYING)::TEXT
        ELSE (((hanyo_attr.attr1)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (hanyo_attr.attr2)::TEXT)
        END AS cname,
    hanyo_attr.attr3 AS juchkbnbun
FROM jp_dcl_edw.hanyo_attr hanyo_attr
WHERE ((hanyo_attr.kbnmei)::TEXT = ('JUCHKBN'::CHARACTER VARYING)::TEXT);
