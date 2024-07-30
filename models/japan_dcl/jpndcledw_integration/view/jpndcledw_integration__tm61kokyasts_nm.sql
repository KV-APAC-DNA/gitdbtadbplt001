with jp_dcl_edw.hanyo_attr
as
(
    select * from jpdcledw_integration.hanyo_attr
),
final as
(
    SELECT hanyo_attr.attr1 AS name
    FROM  hanyo_attr
    WHERE ((hanyo_attr.kbnmei)::TEXT = ('KOKYASTS'::CHARACTER VARYING)::TEXT)

)
select * from final