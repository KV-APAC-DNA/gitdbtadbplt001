with item_zaiko_tbl as (
    select * from dev_dna_core.snapjpdcledw_integration.item_zaiko_tbl
),

final as (
SELECT izt.z_itemcode AS itemcode,
    izt.z_itemname AS itemname,
    izt.itemnamer,
    izt.jancode,
    izt.itemkbn,
    izt.itemkbnname,
    (izt.tanka)::NUMERIC(22, 3) AS tanka,
    izt.bunruicode2,
    izt.bunruicode3,
    izt.bunruicode3_nm,
    izt.bunruicode4,
    izt.bunruicode5,
    izt.insertdate,
    izt.dsoption001,
    izt.dsoption002,
    izt.haiban_hin_cd,
    izt.hin_katashiki,
    substring(izt.syutoku_kbn,1,4)::varchar(9) AS syutoku_kbn,
    izt.bar_cd2
FROM item_zaiko_tbl izt
WHERE (
        (izt.marker = 1)
        OR (izt.marker = 2)
        )
    )

select * from final
