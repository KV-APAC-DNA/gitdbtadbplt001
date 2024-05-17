{{
    config(
        pre_hook="{{build_itg_mds_ph_pos_product()}}"
    )
}}
with sdl_mds_ph_pos_product as 
(
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_pos_product') }}
),
trans as
(
    (
    with wks as (
        select code,
            mnth_id,
            item_cd,
            bar_cd,
            item_nm,
            sap_item_cd,
            sap_item_desc,
            parent_cust_cd,
            parent_cust_nm,
            jnj_item_desc,
            jnj_matl_cse_barcode,
            jnj_matl_pc_barcode,
            early_bk_period,
            cust_conv_factor,
            cust_item_prc,
            jnj_matl_shipper_barcode,
            jnj_matl_consumer_barcode,
            jnj_pc_per_cust_unit,
            computed_price_per_unit,
            jj_price_per_unit,
            cust_sku_grp,
            uom,
            jnj_pc_per_cse,
            lst_period,
            cust_cd,
            cust_cd2,
            last_chg_datetime,
            effective_from,
            case
            when
                to_date(effective_to) = '9999-12-31'
                then dateadd(day, -1, current_timestamp)
            else effective_to
            end as effective_to,
            'N' as active,
            crtd_dttm,
            current_timestamp as updt_dttm
        from (
                select itg.*
                from {{this}} itg,
                    sdl_mds_ph_pos_product sdl
                where sdl.lastchgdatetime != itg.last_chg_datetime
                    and trunc(sdl.effective_sales_cycle) = itg.mnth_id
                    and trim(sdl.item_cd) = itg.item_cd
                    and trim(sdl.prefix) = itg.cust_cd
            )
        union all
        select trim(code) as code,
            trunc(effective_sales_cycle) as mnth_id,
            trim(item_cd) as item_cd,
            trim(barcode) as bar_cd,
            trim(item_nm) as item_nm,
            trim(sap_item_cd_code) as sap_item_cd,
            trim(sap_item_cd_name) as sap_item_desc,
            trim(parent_customer_code) as parent_cust_cd,
            trim(parent_customer_name) as parent_cust_nm,
            trim(jj_item_description) as jnj_item_desc,
            trim(jnj_matl_cse_barcode) as jnj_matl_cse_barcode,
            trim(jnj_matl_pc_barcode) as jnj_matl_pc_barcode,
            trim(early_bk_period) as early_bk_period,
            cast(
                (
                    case
                        when cust_conv_factor = ''
                        or upper(cust_conv_factor) = 'NULL' then null
                        else cust_conv_factor
                    end
                ) as numeric(20, 4)
            ) as cust_conv_factor,
            cast(
                (
                    case
                        when cust_item_prc = ''
                        or upper(cust_item_prc) = 'NULL' then null
                        else cust_item_prc
                    end
                ) as numeric(20, 4)
            ) as cust_item_prc,
            trim(jnj_matl_shipper_barcode) as jnj_matl_shipper_barcode,
            trim(jnj_matl_consumer_barcode) as jnj_matl_consumer_barcode,
            cast(
                (
                    case
                        when jnj_pc_per_cust_unit = ''
                        or upper(jnj_pc_per_cust_unit) = 'NULL' then '1'
                        else jnj_pc_per_cust_unit
                    end
                ) as numeric(20, 4)
            ) as jnj_pc_per_cust_unit,
            cast(
                (
                    case
                        when computed_price_per_unit = ''
                        or upper(computed_price_per_unit) = 'NULL' then null
                        else computed_price_per_unit
                    end
                ) as numeric(20, 4)
            ) as computed_price_per_unit,
            cast(
                (
                    case
                        when jj_price_per_unit = ''
                        or upper(jj_price_per_unit) = 'NULL' then null
                        else jj_price_per_unit
                    end
                ) as numeric(20, 4)
            ) as jj_price_per_unit,
            trim(cust_sku_grp) as cust_sku_grp,
            trim(uom) as uom,
            cast(
                (
                    case
                        when jnj_pc_per_cse = ''
                        or upper(jnj_pc_per_cse) = 'NULL' then null
                        else jnj_pc_per_cse
                    end
                ) as numeric(20, 4)
            ) as jnj_pc_per_cse,
            trim(lst_period) as lst_period,
            trim(prefix) as cust_cd,
            trim(cust_cd) as cust_cd2,
            lastchgdatetime as last_chg_datetime,
            current_timestamp as effective_from,
            '9999-12-31' as effective_to,
            'Y' as active,
            current_timestamp as crtd_dttm,
            current_timestamp as updt_dttm
        from (
                select sdl.*
                from {{this}} itg,
                    sdl_mds_ph_pos_product sdl
                where sdl.lastchgdatetime != itg.last_chg_datetime
                    and trunc(sdl.effective_sales_cycle) = itg.mnth_id
                    and trim(sdl.item_cd) = itg.item_cd
                    and trim(sdl.prefix) = itg.cust_cd
                    and itg.active = 'Y'
            )
        union all
        select trim(code) as code,
            trunc(effective_sales_cycle) as mnth_id,
            trim(item_cd) as item_cd,
            trim(barcode) as bar_cd,
            trim(item_nm) as item_nm,
            trim(sap_item_cd_code) as sap_item_cd,
            trim(sap_item_cd_name) as sap_item_desc,
            trim(parent_customer_code) as parent_cust_cd,
            trim(parent_customer_name) as parent_cust_nm,
            trim(jj_item_description) as jnj_item_desc,
            trim(jnj_matl_cse_barcode) as jnj_matl_cse_barcode,
            trim(jnj_matl_pc_barcode) as jnj_matl_pc_barcode,
            trim(early_bk_period) as early_bk_period,
            cast(
                (
                    case
                        when cust_conv_factor = ''
                        or upper(cust_conv_factor) = 'NULL' then null
                        else cust_conv_factor
                    end
                ) as numeric(20, 4)
            ) as cust_conv_factor,
            cast(
                (
                    case
                        when cust_item_prc = ''
                        or upper(cust_item_prc) = 'NULL' then null
                        else cust_item_prc
                    end
                ) as numeric(20, 4)
            ) as cust_item_prc,
            trim(jnj_matl_shipper_barcode) as jnj_matl_shipper_barcode,
            trim(jnj_matl_consumer_barcode) as jnj_matl_consumer_barcode,
            cast(
                (
                    case
                        when jnj_pc_per_cust_unit = ''
                        or upper(jnj_pc_per_cust_unit) = 'NULL' then '1'
                        else jnj_pc_per_cust_unit
                    end
                ) as numeric(20, 4)
            ) as jnj_pc_per_cust_unit,
            cast(
                (
                    case
                        when computed_price_per_unit = ''
                        or upper(computed_price_per_unit) = 'NULL' then null
                        else computed_price_per_unit
                    end
                ) as numeric(20, 4)
            ) as computed_price_per_unit,
            cast(
                (
                    case
                        when jj_price_per_unit = ''
                        or upper(jj_price_per_unit) = 'NULL' then null
                        else jj_price_per_unit
                    end
                ) as numeric(20, 4)
            ) as jj_price_per_unit,
            trim(cust_sku_grp) as cust_sku_grp,
            trim(uom) as uom,
            cast(
                (
                    case
                        when jnj_pc_per_cse = ''
                        or upper(jnj_pc_per_cse) = 'NULL' then null
                        else jnj_pc_per_cse
                    end
                ) as numeric(20, 4)
            ) as jnj_pc_per_cse,
            trim(lst_period) as lst_period,
            trim(prefix) as cust_cd,
            trim(cust_cd) as cust_cd2,
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
                    sdl_mds_ph_pos_product sdl
                where sdl.lastchgdatetime = itg.last_chg_datetime
                    and trunc(sdl.effective_sales_cycle) = itg.mnth_id
                    and trim(sdl.item_cd) = itg.item_cd
                    and trim(sdl.prefix) = itg.cust_cd
            )
        union all
        select trim(code) as code,
            trunc(effective_sales_cycle) as mnth_id,
            trim(item_cd) as item_cd,
            trim(barcode) as bar_cd,
            trim(item_nm) as item_nm,
            trim(sap_item_cd_code) as sap_item_cd,
            trim(sap_item_cd_name) as sap_item_desc,
            trim(parent_customer_code) as parent_cust_cd,
            trim(parent_customer_name) as parent_cust_nm,
            trim(jj_item_description) as jnj_item_desc,
            trim(jnj_matl_cse_barcode) as jnj_matl_cse_barcode,
            trim(jnj_matl_pc_barcode) as jnj_matl_pc_barcode,
            trim(early_bk_period) as early_bk_period,
            cast(
                (
                    case
                        when cust_conv_factor = ''
                        or upper(cust_conv_factor) = 'NULL' then null
                        else cust_conv_factor
                    end
                ) as numeric(20, 4)
            ) as cust_conv_factor,
            cast(
                (
                    case
                        when cust_item_prc = ''
                        or upper(cust_item_prc) = 'NULL' then null
                        else cust_item_prc
                    end
                ) as numeric(20, 4)
            ) as cust_item_prc,
            trim(jnj_matl_shipper_barcode) as jnj_matl_shipper_barcode,
            trim(jnj_matl_consumer_barcode) as jnj_matl_consumer_barcode,
            cast(
                (
                    case
                        when jnj_pc_per_cust_unit = ''
                        or upper(jnj_pc_per_cust_unit) = 'NULL' then '1'
                        else jnj_pc_per_cust_unit
                    end
                ) as numeric(20, 4)
            ) as jnj_pc_per_cust_unit,
            cast(
                (
                    case
                        when computed_price_per_unit = ''
                        or upper(computed_price_per_unit) = 'NULL' then null
                        else computed_price_per_unit
                    end
                ) as numeric(20, 4)
            ) as computed_price_per_unit,
            cast(
                (
                    case
                        when jj_price_per_unit = ''
                        or upper(jj_price_per_unit) = 'NULL' then null
                        else jj_price_per_unit
                    end
                ) as numeric(20, 4)
            ) as jj_price_per_unit,
            trim(cust_sku_grp) as cust_sku_grp,
            trim(uom) as uom,
            cast(
                (
                    case
                        when jnj_pc_per_cse = ''
                        or upper(jnj_pc_per_cse) = 'NULL' then null
                        else jnj_pc_per_cse
                    end
                ) as numeric(20, 4)
            ) as jnj_pc_per_cse,
            trim(lst_period) as lst_period,
            trim(prefix) as cust_cd,
            trim(cust_cd) as cust_cd2,
            lastchgdatetime as last_chg_datetime,
            current_timestamp as effective_from,
            '9999-12-31' as effective_to,
            'Y' as active,
            current_timestamp as crtd_dttm,
            current_timestamp as updt_dttm
        from (
                select *
                from sdl_mds_ph_pos_product sdl
                where trunc(effective_sales_cycle) || trim(item_cd) || trim(prefix) not in (
                        select distinct mnth_id || item_cd || cust_cd
                        from {{this}}
                    )
            )
    )
    select *
    from wks
    union all
    select *
    from {{this}}
    where mnth_id || item_cd || cust_cd not in (
            select trim(mnth_id) || trim(item_cd) || trim(cust_cd)
            from wks
        )
)
),
final as
(
select 
    code::varchar(100) as code,
    mnth_id::varchar(50) as mnth_id,
    item_cd::varchar(50) as item_cd,
    bar_cd::varchar(50) as bar_cd,
    item_nm::varchar(255) as item_nm,
    sap_item_cd::varchar(50) as sap_item_cd,
    sap_item_desc::varchar(255) as sap_item_desc,
    parent_cust_cd::varchar(30) as parent_cust_cd,
    parent_cust_nm::varchar(255) as parent_cust_nm,
    jnj_item_desc::varchar(255) as jnj_item_desc,
    jnj_matl_cse_barcode::varchar(50) as jnj_matl_cse_barcode,
    jnj_matl_pc_barcode::varchar(50) as jnj_matl_pc_barcode,
    early_bk_period::varchar(50) as early_bk_period,
    cust_conv_factor::number(20,4) as cust_conv_factor,
    cust_item_prc::number(20,4) as cust_item_prc,
    jnj_matl_shipper_barcode::varchar(50) as jnj_matl_shipper_barcode,
    jnj_matl_consumer_barcode::varchar(50) as jnj_matl_consumer_barcode,
    jnj_pc_per_cust_unit::number(20,4) as jnj_pc_per_cust_unit,
    computed_price_per_unit::number(20,4) as computed_price_per_unit,
    jj_price_per_unit::number(20,4) as jj_price_per_unit,
    cust_sku_grp::varchar(50) as cust_sku_grp,
    uom::varchar(50) as uom,
    jnj_pc_per_cse::number(20,4) as jnj_pc_per_cse,
    lst_period::varchar(50) as lst_period,
    cust_cd::varchar(50) as cust_cd,
    cust_cd2::varchar(50) as cust_cd2,
    last_chg_datetime::timestamp_ntz(9) as last_chg_datetime,
    effective_from::timestamp_ntz(9) as effective_from,
    effective_to::timestamp_ntz(9) as effective_to,
    active::varchar(10) as active,
    current_timestamp()::timestamp_ntz(9) as crtd_dttm ,
    current_timestamp()::timestamp_ntz(9) as updt_dttm 
from trans
)
select * from final