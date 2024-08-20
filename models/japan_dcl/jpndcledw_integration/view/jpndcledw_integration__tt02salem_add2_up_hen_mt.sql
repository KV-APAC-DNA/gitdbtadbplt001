with tt01saleh_hen_mv_mt as (
    select * from {{ ref('jpndcledw_integration__tt01saleh_hen_mv_mt') }}
),
final as (
SELECT tt01saleh_hen_mv_mt.saleno,
    9000000001::BIGINT AS gyono,
    '利用ポイント数(交換以外)'::CHARACTER VARYING AS meisaikbn,
    'X000000001' AS itemcode,
    1 AS suryo,
    tt01saleh_hen_mv_mt.riyopoint AS tanka,
    tt01saleh_hen_mv_mt.riyopoint AS kingaku,
    tt01saleh_hen_mv_mt.riyopoint AS meisainukikingaku,
    0 AS wariritu,
    tt01saleh_hen_mv_mt.riyopoint AS warimaekomitanka,
    tt01saleh_hen_mv_mt.riyopoint AS warimaenukikingaku,
    tt01saleh_hen_mv_mt.riyopoint AS warimaekomikingaku,
    tt01saleh_hen_mv_mt.dispsaleno,
    tt01saleh_hen_mv_mt.kesaiid,
    tt01saleh_hen_mv_mt.henpinsts
FROM tt01saleh_hen_mv_mt
WHERE (tt01saleh_hen_mv_mt.riyopoint <> ((0)::NUMERIC)::NUMERIC(18, 0))
)

select * from final