with itg_sg_listprice as
(
    select * from {{ ref("sgpitg_integration__itg_sg_listprice") }}
),
final as 
(
    select
        'SG' as cntry_key,
        'SINGAPORE' as cntry_nm,
        b.plant,
        b.cnty,
        b.item_cd,
        b.item_desc,
        b.valid_from,
        b.valid_to,
        b.rate,
        b.currency,
        b.price_unit,
        b.uom,
        b.yearmo,
        b.mnth_type,
        b.snapshot_dt
        from itg_sg_listprice as b
        where
        (
            (
                cast((b.mnth_type) as text) 
                = cast('JJ' as text)
            )
            or 
            (
            cast((b.mnth_type) as text)
            = cast('CAL' as text)
            )
        )
)
select * from final