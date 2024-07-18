with item_hanbai_tbl as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.ITEM_HANBAI_TBL
),
final as (SELECT iht.h_itemcode,
    CASE 
        WHEN ((iht.h_itemcode)::TEXT = ('X000000002'::CHARACTER VARYING)::TEXT)
            THEN '利用ポイント数(交換)'::CHARACTER VARYING
        ELSE iht.h_itemname
        END AS h_itemname,
    iht.diid,
    iht.tanka_sales,
    iht.syutoku_kbn
FROM item_hanbai_tbl iht)
select * from final