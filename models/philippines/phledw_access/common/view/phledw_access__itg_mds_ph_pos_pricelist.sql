with source as (
    select * from {{ ref('phlitg_integration__itg_mds_ph_pos_pricelist') }}
),
final as
(
    select code as "code",
        item_cd as "item_cd",
        item_desc as "item_desc",
        jj_mnth_id as "jj_mnth_id",
        consumer_bar_cd as "consumer_bar_cd",
        shippers_bar_cd as "shippers_bar_cd",
        dz_per_case as "dz_per_case",
        lst_price_case as "lst_price_case",
        lst_price_dz as "lst_price_dz",
        lst_price_unit as "lst_price_unit",
        srp as "srp",
        status as "status",
        status_nm as "status_nm",
        last_chg_datetime as "last_chg_datetime",
        effective_from as "effective_from",
        effective_to as "effective_to",
        active as "active",
        crtd_dttm as "crtd_dttm",
        updt_dttm as "updt_dttm"
    from source
)
select * from final