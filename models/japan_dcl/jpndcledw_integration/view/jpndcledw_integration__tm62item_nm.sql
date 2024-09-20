with item_bunrval_v
as
(
select * from {{ref('jpndcledw_integration__item_bunrval_v')}}
),
final as
(
    SELECT sscmnhinbunrval.hin_bunr_val_cd AS code,
        sscmnhinbunrval.hin_bunr_val_nms AS name
    FROM item_bunrval_v sscmnhinbunrval
    WHERE (
            ((sscmnhinbunrval.hin_bunr_taik_id)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
            AND ((sscmnhinbunrval.hin_bunr_kaisou_kbn)::TEXT = ('3'::CHARACTER VARYING)::TEXT)
            )

    UNION

    SELECT '99' AS code,
        'その他' AS name
)
select * from final