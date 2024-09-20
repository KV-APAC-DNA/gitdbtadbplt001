with hanyo_attr
as
(
    select * from {{ ref('jpndcledw_integration__hanyo_attr') }}
),
final as
(
    SELECT hanyo_attr.attr1 AS dmngflg
    FROM  hanyo_attr
    WHERE ((hanyo_attr.kbnmei)::TEXT = ('DMNGFLG_TSUHAN'::CHARACTER VARYING)::TEXT)
)
select * from final