with hanyo_attr
as
(
    select * from {{ ref('jpndcledw_integration__hanyo_attr') }}
),
final as
(
    SELECT hanyo_attr.attr1 AS name
    FROM hanyo_attr
    WHERE ((hanyo_attr.kbnmei)::TEXT = ('KOKYAKBN'::CHARACTER VARYING)::TEXT)
)
select * from final