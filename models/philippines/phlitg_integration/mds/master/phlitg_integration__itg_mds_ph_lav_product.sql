{{
    config
    (
        pre_hook="{{build_itg_mds_ph_lav_product()}}"
    )
}}
with sdl_mds_ph_lav_product as 
(
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_lav_product') }}
),
wks as 
(
    select 
        item_cd,
        item_nm,
        ims_otc_tag,
        ims_otc_tag_nm,
        npi_strt_period,
        price_lst_period,
        promo_strt_period,
        promo_end_period,
        promo_reg_ind,
        promo_reg_nm,
        hero_sku_ind,
        hero_sku_nm,
        rpt_grp_1,
        rpt_grp_1_desc,
        rpt_grp_2,
        rpt_grp_2_desc,
        rpt_grp_3,
        rpt_grp_3_desc,
        rpt_grp_4,
        rpt_grp_4_desc,
        rpt_grp_5,
        rpt_grp_5_desc,
        scard_brand_cd,
        scard_brand_desc,
        scard_franchise_cd,
        scard_franchise_desc,
        scard_put_up_cd,
        scard_put_up_desc,
        scard_varient_cd,
        scard_varient_desc,
        last_chg_datetime,
        effective_from,
        case
            when to_date(effective_to) = '9999-12-31' then dateadd (day, -1, current_timestamp)
            else effective_to
        end as effective_to,
        'N' as active,
        crtd_dttm,
        current_timestamp as updt_dttm
    from 
        (
            select itg.*
            from {{this}} itg,
                sdl_mds_ph_lav_product sdl
            where sdl.lastchgdatetime != itg.last_chg_datetime
                and trim(sdl.code) = itg.item_cd
        )
    union all
    select trim(code) as item_cd,
        trim(name) as item_nm,
        trim(imsotctag_code) as ims_otc_tag,
        trim(imsotctag_name) as ims_otc_tag_nm,
        trim(npistartperiod) as npi_strt_period,
        trim(pricelastperiod) as price_lst_period,
        trim(promostartperiod) as promo_strt_period,
        trim(promoendperiod) as promo_end_period,
        trim(promoreg_code) as promo_reg_ind,
        trim(promoreg_name) as promo_reg_nm,
        trim(hero_sku_code) as hero_sku_ind,
        trim(hero_sku_name) as hero_sku_nm,
        trim(reportgroup1desc_code) as rpt_grp_1,
        trim(reportgroup1desc_name) as rpt_grp_1_desc,
        trim(reportgroup2desc_code) as rpt_grp_2,
        trim(reportgroup2desc_name) as rpt_grp_2_desc,
        trim(reportgroup3desc_code) as rpt_grp_3,
        trim(reportgroup3desc_name) as rpt_grp_3_desc,
        trim(reportgroup4desc_code) as rpt_grp_4,
        trim(reportgroup4desc_name) as rpt_grp_4_desc,
        trim(reportgroup5desc_code) as rpt_grp_5,
        trim(reportgroup5desc_name) as rpt_grp_5_desc,
        trim(scardbrandcode_code) as scard_brand_cd,
        trim(scardbrandcode_name) as scard_brand_desc,
        trim(scardfranchisecode_code) as scard_franchise_cd,
        trim(scardfranchisecode_name) as scard_franchise_desc,
        trim(scardputupcode_code) as scard_put_up_cd,
        trim(scardputupcode_name) as scard_put_up_desc,
        trim(scardvariantcode_code) as scard_varient_cd,
        trim(scardvariantcode_name) as scard_varient_desc,
        lastchgdatetime as last_chg_datetime,
        current_timestamp as effective_from,
        '9999-12-31' as effective_to,
        'Y' as active,
        current_timestamp as crtd_dttm,
        current_timestamp as updt_dttm
    from (
            select sdl.*
            from {{this}} itg,
                sdl_mds_ph_lav_product sdl
            where sdl.lastchgdatetime != itg.last_chg_datetime
                and trim(sdl.code) = itg.item_cd
                and itg.active = 'Y'
        )
    union all
    select 
        trim(code) as item_cd,
        trim(name) as item_nm,
        trim(imsotctag_code) as ims_otc_tag,
        trim(imsotctag_name) as ims_otc_tag_nm,
        trim(npistartperiod) as npi_strt_period,
        trim(pricelastperiod) as price_lst_period,
        trim(promostartperiod) as promo_strt_period,
        trim(promoendperiod) as promo_end_period,
        trim(promoreg_code) as promo_reg_ind,
        trim(promoreg_name) as promo_reg_nm,
        trim(hero_sku_code) as hero_sku_ind,
        trim(hero_sku_name) as hero_sku_nm,
        trim(reportgroup1desc_code) as rpt_grp_1,
        trim(reportgroup1desc_name) as rpt_grp_1_desc,
        trim(reportgroup2desc_code) as rpt_grp_2,
        trim(reportgroup2desc_name) as rpt_grp_2_desc,
        trim(reportgroup3desc_code) as rpt_grp_3,
        trim(reportgroup3desc_name) as rpt_grp_3_desc,
        trim(reportgroup4desc_code) as rpt_grp_4,
        trim(reportgroup4desc_name) as rpt_grp_4_desc,
        trim(reportgroup5desc_code) as rpt_grp_5,
        trim(reportgroup5desc_name) as rpt_grp_5_desc,
        trim(scardbrandcode_code) as scard_brand_cd,
        trim(scardbrandcode_name) as scard_brand_desc,
        trim(scardfranchisecode_code) as scard_franchise_cd,
        trim(scardfranchisecode_name) as scard_franchise_desc,
        trim(scardputupcode_code) as scard_put_up_cd,
        trim(scardputupcode_name) as scard_put_up_desc,
        trim(scardvariantcode_code) as scard_varient_cd,
        trim(scardvariantcode_name) as scard_varient_desc,
        lastchgdatetime as last_chg_datetime,
        effective_from,
        '9999-12-31' as effective_to,
        'Y' as active,
        current_timestamp as crtd_dttm,
        current_timestamp as updt_dttm
    from (
            select sdl.*,
                itg.effective_from
            from {{this}} itg,
                sdl_mds_ph_lav_product sdl
            where sdl.lastchgdatetime = itg.last_chg_datetime
                and trim(sdl.code) = itg.item_cd
        )
    UNION ALL
    select trim(code) as item_cd,
        trim(name) as item_nm,
        trim(imsotctag_code) as ims_otc_tag,
        trim(imsotctag_name) as ims_otc_tag_nm,
        trim(npistartperiod) as npi_strt_period,
        trim(pricelastperiod) as price_lst_period,
        trim(promostartperiod) as promo_strt_period,
        trim(promoendperiod) as promo_end_period,
        trim(promoreg_code) as promo_reg_ind,
        trim(promoreg_name) as promo_reg_nm,
        trim(hero_sku_code) as hero_sku_ind,
        trim(hero_sku_name) as hero_sku_nm,
        trim(reportgroup1desc_code) as rpt_grp_1,
        trim(reportgroup1desc_name) as rpt_grp_1_desc,
        trim(reportgroup2desc_code) as rpt_grp_2,
        trim(reportgroup2desc_name) as rpt_grp_2_desc,
        trim(reportgroup3desc_code) as rpt_grp_3,
        trim(reportgroup3desc_name) as rpt_grp_3_desc,
        trim(reportgroup4desc_code) as rpt_grp_4,
        trim(reportgroup4desc_name) as rpt_grp_4_desc,
        trim(reportgroup5desc_code) as rpt_grp_5,
        trim(reportgroup5desc_name) as rpt_grp_5_desc,
        trim(scardbrandcode_code) as scard_brand_cd,
        trim(scardbrandcode_name) as scard_brand_desc,
        trim(scardfranchisecode_code) as scard_franchise_cd,
        trim(scardfranchisecode_name) as scard_franchise_desc,
        trim(scardputupcode_code) as scard_put_up_cd,
        trim(scardputupcode_name) as scard_put_up_desc,
        trim(scardvariantcode_code) as scard_varient_cd,
        trim(scardvariantcode_name) as scard_varient_desc,
        lastchgdatetime as last_chg_datetime,
        current_timestamp as effective_from,
        '9999-12-31' as effective_to,
        'Y' as active,
        current_timestamp as crtd_dttm,
        current_timestamp as updt_dttm
    from (
            select *
            from sdl_mds_ph_lav_product sdl
            where trim(code) not in (
                    select distinct item_cd
                    from {{this}}
                )
        )
),
transformed as
(
    select * from wks
    union all
    select * from {{this}}
    where item_cd not in 
    (
        select trim(item_cd) from wks
    )
),
final as
(
    select 
        item_cd::varchar(30) as item_cd,
        item_nm::varchar(255) as item_nm,
        ims_otc_tag::varchar(50) as ims_otc_tag,
        ims_otc_tag_nm::varchar(50) as ims_otc_tag_nm,
        npi_strt_period::varchar(50) as npi_strt_period,
        price_lst_period::varchar(50) as price_lst_period,
        promo_strt_period::varchar(50) as promo_strt_period,
        promo_end_period::varchar(255) as promo_end_period,
        promo_reg_ind::varchar(50) as promo_reg_ind,
        promo_reg_nm::varchar(50) as promo_reg_nm,
        hero_sku_ind::varchar(50) as hero_sku_ind,
        hero_sku_nm::varchar(50) as hero_sku_nm,
        rpt_grp_1::varchar(50) as rpt_grp_1,
        rpt_grp_1_desc::varchar(255) as rpt_grp_1_desc,
        rpt_grp_2::varchar(50) as rpt_grp_2,
        rpt_grp_2_desc::varchar(255) as rpt_grp_2_desc,
        rpt_grp_3::varchar(50) as rpt_grp_3,
        rpt_grp_3_desc::varchar(255) as rpt_grp_3_desc,
        rpt_grp_4::varchar(50) as rpt_grp_4,
        rpt_grp_4_desc::varchar(255) as rpt_grp_4_desc,
        rpt_grp_5::varchar(50) as rpt_grp_5,
        rpt_grp_5_desc::varchar(255) as rpt_grp_5_desc,
        scard_brand_cd::varchar(50) as scard_brand_cd,
        scard_brand_desc::varchar(255) as scard_brand_desc,
        scard_franchise_cd::varchar(50) as scard_franchise_cd,
        scard_franchise_desc::varchar(255) as scard_franchise_desc,
        scard_put_up_cd::varchar(50) as scard_put_up_cd,
        scard_put_up_desc::varchar(255) as scard_put_up_desc,
        scard_varient_cd::varchar(50) as scard_varient_cd,
        scard_varient_desc::varchar(255) as scard_varient_desc,
        last_chg_datetime::timestamp_ntz(9) as last_chg_datetime,
        effective_from::timestamp_ntz(9) as effective_from,
        effective_to::timestamp_ntz(9) as effective_to,
        active::varchar(10) as active,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from transformed
)
select * from final