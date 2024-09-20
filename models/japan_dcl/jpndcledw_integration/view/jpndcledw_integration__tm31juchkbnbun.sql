with hanyo_attr
AS
(
    select * from {{ ref('jpndcledw_integration__hanyo_attr') }}
),
final AS
(
    SELECT DISTINCT hanyo_attr.attr3::varchar(60) AS juchkbnbunname
    FROM hanyo_attr
    WHERE ((hanyo_attr.kbnmei)::TEXT = ('JUCHKBN'::CHARACTER VARYING)::TEXT)
)
select * from final
