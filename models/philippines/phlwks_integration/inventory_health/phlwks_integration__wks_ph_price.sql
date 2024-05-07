with source as(
    select * from DEV_DNA_CORE.PHLitg_INTEGRATION.itg_mds_ph_pos_pricelist
),
transformed as(
    SELECT DISTINCT JJ_MNTH_ID,
        ITEM_CD,
        LST_PRICE_UNIT
    FROM source
    WHERE active = 'Y'
)
select * from transformed