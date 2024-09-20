with hanyo_attr as (
    select * from {{ ref('jpndcledw_integration__hanyo_attr') }}
),
c_tbmembunitrel as (
    select * from {{ ref('jpndclitg_integration__c_tbmembunitrel') }}
    ),

derived_table1 AS (
        SELECT hanyo_attr.attr1,
            hanyo_attr.attr2
        FROM hanyo_attr
        WHERE ((hanyo_attr.kbnmei)::TEXT = 'DAILYFROM'::TEXT)
        ),
        
final as (

SELECT lpad((c_tbmembunitrel.c_dichildusrid)::TEXT, 10, (0)::TEXT) AS kokyakuno,
    to_number(to_char((to_date((c_tbmembunitrel.dsren)::TEXT, 'YYYY-MM-DD HH24:MI:SS'::TEXT))::TIMESTAMP without TIME zone, 'YYYYMMDD'::TEXT), '99999999'::TEXT) AS lastdate,
    to_number(regexp_replace("right" (
                (c_tbmembunitrel.dsren)::TEXT,
                8
                ), (':'::CHARACTER VARYING)::TEXT), '999999'::TEXT) AS lasttime,
    lpad((c_tbmembunitrel.c_diparentusrid)::TEXT, 10, (0)::TEXT) AS nayosesakino
FROM c_tbmembunitrel,
    derived_table1
WHERE (
        (to_char((to_date((c_tbmembunitrel.dsren)::TEXT, 'YYYY-MM-DD HH24:MI:SS'::TEXT))::TIMESTAMP without TIME zone, 'YYYYMMDD'::TEXT) >= to_char(dateadd(day, derived_table1.attr1,current_timestamp()::TIMESTAMP without TIME zone), 'YYYYMMDD'::TEXT))
        AND (to_char((to_date((c_tbmembunitrel.dsren)::TEXT, 'YYYY-MM-DD HH24:MI:SS'::TEXT))::TIMESTAMP without TIME zone, 'YYYYMMDD'::TEXT) <= to_char(dateadd(day, derived_table1.attr2,current_timestamp()::TIMESTAMP without TIME zone), 'YYYYMMDD'::TEXT))
        )
    )

select * from final