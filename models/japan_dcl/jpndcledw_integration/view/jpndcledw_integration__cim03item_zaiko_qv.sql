with ITEM_ZAIKO_TBL as 
(
    select * from SNAPJPDCLEDW_INTEGRATION.ITEM_ZAIKO_TBL
)
,final as 
(
SELECT izt.z_itemcode AS itemcode,
    izt.z_itemname AS itemname,
    izt.itemnamer,
    izt.jancode,
    izt.itemkbn,
    izt.itemkbnname,
    (izt.tanka)::NUMERIC(22, 3) AS tanka,
    izt.bunruicode2,
    izt.bunruicode3,
    izt.bunruicode4,
    izt.bunruicode5,
    izt.insertdate,
    izt.dsoption001,
    izt.dsoption002,
    izt.haiban_hin_cd,
    izt.hin_katashiki,
    (izt.syutoku_kbn)::CHARACTER VARYING(6) AS syutoku_kbn
FROM ITEM_ZAIKO_TBL izt
)
select * from final 