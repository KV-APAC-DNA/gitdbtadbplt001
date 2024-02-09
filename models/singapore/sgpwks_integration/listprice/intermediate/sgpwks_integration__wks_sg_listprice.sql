with edw_list_price as
(
    select * from {{ ref('aspedw_integration__edw_list_price') }}
),
edw_vw_sg_material_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_sg_material_dim') }}
),
filtered_edw_list_price as
(
    select * from edw_list_price
    where sls_org = '2210'

),
transformed_join as
(
    select
        sls_org::varchar(4) as plant,
        knart::varchar(4) as cnty,
        ltrim(material, '0')::varchar(20) as item_cd,
        veomd.sap_mat_desc::varchar(100) as item_desc,
        to_date(dt_from, 'YYYYMMDD')::varchar(10) as valid_from,
        to_date(valid_to, 'YYYYMMDD')::varchar(10) as valid_to,
        amount::number(20,4)as rate,
        currency::varchar(4) as currency,
        price_unit::number(16,4) as price_unit,
        unit::varchar(6) as uom,
        cdl_dttm::varchar(255) as cdl_dttm,
        date_trunc('day',dateadd(day, -1, cast(current_timestamp() as timestamp_ntz)))::date as snapshot_dt,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        cast(null as date)::timestamp_ntz(9) as updt_dttm
    from filtered_edw_list_price as relp,edw_vw_sg_material_dim as veomd
    where ltrim(veomd.sap_matl_num(+), '0') = ltrim(relp.material, '0')
),
final as
(
    select
        plant,
        cnty,
        item_cd,
        item_desc,
        valid_from,
        valid_to,
        rate,
        currency,
        price_unit,
        uom,
        cdl_dttm,
        snapshot_dt,
        crtd_dttm,
        updt_dttm
    from transformed_join

)
select * from final