with item_hanbai_tbl 
as
(
    select * from {{ ref('jpndcledw_integration__item_hanbai_tbl') }}
),
final as
(
    SELECT iht.h_itemcode AS itemcode,
        (iht.h_itemname)::CHARACTER VARYING(192) AS itemname,
        (iht.itemnamer)::CHARACTER VARYING(6000) AS itemnamer,
        (iht.itemkbn)::CHARACTER VARYING(3) AS itemkbn,
        iht.bunruicode2,
        (iht.bunruicode3)::CHARACTER VARYING(6000) AS bunruicode3,
        (iht.bunruicode5)::CHARACTER VARYING(6000) AS bunruicode5,
        (
            CASE 
                WHEN ((iht.dsoption002)::TEXT = ('単品'::CHARACTER VARYING)::TEXT)
                    THEN '0'::CHARACTER VARYING
                ELSE '1'::CHARACTER VARYING
                END
            )::CHARACTER VARYING(1) AS settanpinkbncode,
        (
            CASE 
                WHEN ((iht.dsoption002)::TEXT = ('単品'::CHARACTER VARYING)::TEXT)
                    THEN '単品'::CHARACTER VARYING
                ELSE 'セット品'::CHARACTER VARYING
                END
            )::CHARACTER VARYING(12) AS settanpinsetkbn,
        (iht.dsoption010)::CHARACTER VARYING(6000) AS teikikeiyaku
    FROM item_hanbai_tbl iht
)
select * from final
