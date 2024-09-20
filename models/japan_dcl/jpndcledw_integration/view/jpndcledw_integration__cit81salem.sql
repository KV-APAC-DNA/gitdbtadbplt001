with cit81salem_uri as (
    select * from {{ source('jpdcledw_integration', 'cit81salem_uri_mv') }}
),
cit81salem_hen as (
    select * from {{ source('jpdcledw_integration', 'cit81salem_hen_mv') }}
),
t1 as (
    SELECT cit81salem_uri.saleno,
    cit81salem_uri.gyono,
    cit81salem_uri.itemcode,
    cit81salem_uri.itemcode_hanbai,
    cit81salem_uri.suryo,
    cit81salem_uri.jyu_suryo,
    cit81salem_uri.oyaflg,
    cit81salem_uri.hensu,
    cit81salem_uri.meisainukikingaku,
    cit81salem_uri.meisaitax,
    cit81salem_uri.juchgyono,
    cit81salem_uri.kakokbn,
    cit81salem_uri.dispsaleno,
    cit81salem_uri.tyoseikikingaku,
    cit81salem_uri.anbunmeisainukikingaku,
    cit81salem_uri.juch_shur,
    'U' AS urihen_marker
FROM cit81salem_uri
),
t2 as (
    SELECT cit81salem_hen.saleno,
    cit81salem_hen.gyono,
    cit81salem_hen.itemcode,
    cit81salem_hen.itemcode_hanbai,
    cit81salem_hen.suryo,
    cit81salem_hen.suryo AS jyu_suryo,
    '' AS oyaflg,
    cit81salem_hen.hensu,
    cit81salem_hen.meisainukikingaku,
    cit81salem_hen.meisaitax,
    cit81salem_hen.juchgyono,
    cit81salem_hen.kakokbn,
    cit81salem_hen.dispsaleno,
    cit81salem_hen.tyoseikikingaku,
    cit81salem_hen.anbunmeisainukikingaku,
    cit81salem_hen.juch_shur,
    'H' AS urihen_marker
FROM cit81salem_hen
),
final as (
    SELECT *
    FROM t1
    
    UNION ALL
    
    SELECT *
    FROM t2 
)
Select * from final