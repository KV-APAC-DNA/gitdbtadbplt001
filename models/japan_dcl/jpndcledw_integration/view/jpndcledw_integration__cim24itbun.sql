with cim24itbun_ikou as (
    select * from dev_dna_core.snapjpdcledw_integration.cim24itbun_ikou
),
item_bunrval_v as (
    select * from dev_dna_core.snapjpdcledw_integration.item_bunrval_v
),

derived_table1 as (
    SELECT CASE 
            WHEN (
                    ((sscmnhinbunrval.hin_bunr_taik_id)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
                    AND ((sscmnhinbunrval.hin_bunr_kaisou_kbn)::TEXT = ('3'::CHARACTER VARYING)::TEXT)
                    )
                THEN '5'::CHARACTER VARYING
            WHEN (
                    ((sscmnhinbunrval.hin_bunr_taik_id)::TEXT = ('3'::CHARACTER VARYING)::TEXT)
                    AND ((sscmnhinbunrval.hin_bunr_kaisou_kbn)::TEXT = ('2'::CHARACTER VARYING)::TEXT)
                    )
                THEN '2'::CHARACTER VARYING
            WHEN (
                    ((sscmnhinbunrval.hin_bunr_taik_id)::TEXT = ('3'::CHARACTER VARYING)::TEXT)
                    AND ((sscmnhinbunrval.hin_bunr_kaisou_kbn)::TEXT = ('3'::CHARACTER VARYING)::TEXT)
                    )
                THEN '3'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
            END AS itbunshcode,
        sscmnhinbunrval.hin_bunr_val_cd AS itbuncode,
        sscmnhinbunrval.hin_bunr_val_nms AS itbunname
    FROM item_bunrval_v sscmnhinbunrval
),

cte1 as (
SELECT (derived_table1.itbunshcode)::CHARACTER VARYING(4) AS itbunshcode,
    (derived_table1.itbuncode)::CHARACTER VARYING(7) AS itbuncode,
    derived_table1.itbunname
FROM derived_table1
WHERE (derived_table1.itbunshcode IS NOT NULL)
),

cte2 as (
SELECT ikou.itbunshcode,
    ikou.itbuncode,
    ikou.itbunname
FROM cim24itbun_ikou ikou
),

final as (
    select * from cte1
    union
    select * from cte2
)

select * from final