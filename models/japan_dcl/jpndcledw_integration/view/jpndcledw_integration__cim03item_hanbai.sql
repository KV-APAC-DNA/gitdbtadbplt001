with ITEM_HANBAI_TBL as 
(
    select * from SNAPJPDCLEDW_INTEGRATION.ITEM_HANBAI_TBL
),final as 
(
SELECT iht.h_itemcode AS itemcode,
    (iht.h_itemname)::CHARACTER VARYING(192) AS itemname,
    (iht.itemnamer)::CHARACTER VARYING(6000) AS itemnamer,
    (iht.itemkbn)::CHARACTER VARYING(3) AS itemkbn,
    (iht.tanka)::BIGINT AS tanka,
    iht.bunruicode2,
    (iht.bunruicode3)::CHARACTER VARYING(6000) AS bunruicode3,
    iht.bunruicode4,
    (iht.bunruicode5)::CHARACTER VARYING(6000) AS bunruicode5,
    (iht.insertdate)::INTEGER AS insertdate,
    (iht.dsoption001)::CHARACTER VARYING(6000) AS dsoption001,
    (iht.dsoption002)::CHARACTER VARYING(6000) AS dsoption002,
    (iht.dsoption010)::CHARACTER VARYING(6000) AS dsoption010,
    NULL AS haiban_hin_cd,
    (iht.syutoku_kbn)::CHARACTER VARYING(4) AS syutoku_kbn
FROM ITEM_HANBAI_TBL iht
WHERE (
        (
            (iht.marker = 1)
            OR (iht.marker = 2)
            )
        OR (iht.marker = 3)
        )
)
select * from final