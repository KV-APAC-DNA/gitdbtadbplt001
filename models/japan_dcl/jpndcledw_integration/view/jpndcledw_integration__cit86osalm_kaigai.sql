with cit86osalm_uri_mv as (
    select * from {{ source('jpdcledw_integration', 'cit86osalm_uri_mv') }}
),
cit86osalm_hen_mv as (
    select * from {{ source('jpdcledw_integration', 'cit86osalm_hen_mv') }}
),
cte1 as (
SELECT cit86osalm_uri.ourino,
    cit86osalm_uri.gyono,
    cit86osalm_uri.itemcode,
    cit86osalm_uri.itemcode_hanbai,
    cit86osalm_uri.suryo,
    cit86osalm_uri.tanka,
    cit86osalm_uri.hensu,
    cit86osalm_uri.henusu,
    cit86osalm_uri.kingaku,
    cit86osalm_uri.meisainukikingaku,
    cit86osalm_uri.meisaitax,
    cit86osalm_uri.anbunmeisainukikingaku,
    cit86osalm_uri.tanka_tuka,
    cit86osalm_uri.kingaku_tuka,
    cit86osalm_uri.meisainukikingaku_tuka,
    cit86osalm_uri.meisaitax_tuka,
    cit86osalm_uri.kakokbn,
    cit86osalm_uri.kkng_kbn,
    cit86osalm_uri.dispourino,
    cit86osalm_uri.shimebi,
    cit86osalm_uri.shohzei_ritsu
FROM cit86osalm_uri_mv cit86osalm_uri
WHERE ((cit86osalm_uri.kkng_kbn)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
),

cte2 as (
SELECT cit86osalm_hen.ourino,
    cit86osalm_hen.gyono,
    cit86osalm_hen.itemcode,
    cit86osalm_hen.itemcode_hanbai,
    cit86osalm_hen.suryo,
    cit86osalm_hen.tanka,
    cit86osalm_hen.hensu,
    cit86osalm_hen.henusu,
    cit86osalm_hen.kingaku,
    cit86osalm_hen.meisainukikingaku,
    cit86osalm_hen.meisaitax,
    cit86osalm_hen.anbunmeisainukikingaku,
    cit86osalm_hen.tanka_tuka,
    cit86osalm_hen.kingaku_tuka,
    cit86osalm_hen.meisainukikingaku_tuka,
    cit86osalm_hen.meisaitax_tuka,
    cit86osalm_hen.kakokbn,
    cit86osalm_hen.kkng_kbn,
    cit86osalm_hen.dispourino,
    cit86osalm_hen.shimebi,
    cit86osalm_hen.shohzei_ritsu
FROM cit86osalm_hen_mv cit86osalm_hen
WHERE ((cit86osalm_hen.kkng_kbn)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
),

final as (
    select * from cte1
    union all
    select * from cte2
)


select * from final