with item_jizen_bunkai_tbl 
as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_tbl') }}
),
final as
(
    SELECT (item_jizen_bunkai_tbl.item_cd)::CHARACTER VARYING(65535) AS itemcode,
        (item_jizen_bunkai_tbl.kosei_cd)::CHARACTER VARYING(65535) AS kosecode,
        (item_jizen_bunkai_tbl.suryo)::NUMERIC(14, 4) AS suryo,
        (NULL::CHARACTER VARYING)::CHARACTER VARYING(11) AS kosetanka,
        (item_jizen_bunkai_tbl.koseritsu)::NUMERIC(18, 8) AS koseritsu,
        (NULL::CHARACTER VARYING)::CHARACTER VARYING(11) AS koseanbuntanka,
        (NULL::CHARACTER VARYING)::CHARACTER VARYING(11) AS motoinsertdate,
        (NULL::CHARACTER VARYING)::CHARACTER VARYING(11) AS motoupdatedate,
        (item_jizen_bunkai_tbl.insertdate)::CHARACTER VARYING(11) AS insertdate,
        (item_jizen_bunkai_tbl.inserttime)::CHARACTER VARYING(11) AS inserttime,
        item_jizen_bunkai_tbl.insertid,
        (item_jizen_bunkai_tbl.insertdate)::CHARACTER VARYING(11) AS updatedate,
        (item_jizen_bunkai_tbl.inserttime)::CHARACTER VARYING(11) AS updatetime,
        item_jizen_bunkai_tbl.insertid AS updateid
    FROM item_jizen_bunkai_tbl
    WHERE (item_jizen_bunkai_tbl.marker = 1)
)
select * from final