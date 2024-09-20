WITH cim03item_hanbai
AS (
    SELECT *
    FROM {{ref ('jpndcledw_integration__cim03item_hanbai')}}
    ),
final
AS (
    SELECT cim03item_hanbai.itemcode,
        cim03item_hanbai.itemname,
        cim03item_hanbai.itemnamer,
        cim03item_hanbai.itemkbn,
        cim03item_hanbai.bunruicode2,
        cim03item_hanbai.bunruicode3,
        cim03item_hanbai.bunruicode5,
        CASE 
            WHEN ((cim03item_hanbai.dsoption002)::TEXT = ('単品'::CHARACTER VARYING)::TEXT)
                THEN '0'::CHARACTER VARYING
            ELSE '1'::CHARACTER VARYING
            END AS settanpinkbncode,
        CASE 
            WHEN ((cim03item_hanbai.dsoption002)::TEXT = ('単品'::CHARACTER VARYING)::TEXT)
                THEN '単品'::CHARACTER VARYING
            ELSE 'セット品'::CHARACTER VARYING
            END AS settanpinsetkbn,
        cim03item_hanbai.dsoption010 AS teikikeiyaku
    FROM cim03item_hanbai
    )
SELECT *
FROM final