{{
    config(
        pre_hook="{{build_phlitg_integration__itg_mds_ph_ref_distributors()}}"
    )
}}

with source as(
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_ref_distributors') }}
),
wks as (
    select  dstrbtr_grp_cd,
            dstrbtr_grp_nm,
            primary_sold_to,
            primary_sold_to_nm,
            rpt_grp_10,
            rpt_grp_10_desc,
            rpt_grp_12,
            rpt_grp_12_desc,
            rpt_grp_13,
            rpt_grp_13_desc,
            rpt_grp_14,
            rpt_grp_14_desc,
            rpt_grp_8,
            rpt_grp_8_desc,
            last_chg_datetime,
            effective_from,
            case 
                when to_date(effective_to) = '9999-12-31'
                    then dateadd(day, - 1, current_timestamp()::timestamp_ntz(9))
                else effective_to
                end as effective_to,
            'N' as active,
            crtd_dttm,
            current_timestamp()::timestamp_ntz(9) as updt_dttm
        from (
            select itg.*
            from {{this}} itg,
                source sdl
            where sdl.lastchgdatetime != itg.last_chg_datetime
                and trim(sdl.code) = itg.dstrbtr_grp_cd
            )

        union all

        select trim(code) as dstrbtr_grp_cd,
            trim(name) as dstrbtr_grp_nm,
            trim(primary_sold_to_code) as primary_sold_to,
            trim(primary_sold_to_name) as primary_sold_to_nm,
            trim(reportgroup10desc_code) as rpt_grp_10,
            trim(reportgroup10desc_name) as rpt_grp_10_desc,
            trim(reportgroup12desc_code) as rpt_grp_12,
            trim(reportgroup12desc_name) as rpt_grp_12_desc,
            trim(reportgroup13desc_code) as rpt_grp_13,
            trim(reportgroup13desc_name) as rpt_grp_13_desc,
            trim(reportgroup14desc_code) as rpt_grp_14,
            trim(reportgroup14desc_name) as rpt_grp_14_desc,
            trim(reportgroup8desc_code) as rpt_grp_8,
            trim(reportgroup8desc_name) as rpt_grp_8_desc,
            lastchgdatetime as last_chg_datetime,
            current_timestamp()::timestamp_ntz(9) as effective_from,
            '9999-12-31' as effective_to,
            'Y' as active,
            current_timestamp()::timestamp_ntz(9) as crtd_dttm,
            current_timestamp()::timestamp_ntz(9) as updt_dttm
        from (
            select sdl.*
            from {{this}} itg,
                source sdl
            where sdl.lastchgdatetime != itg.last_chg_datetime
                and trim(sdl.code) = itg.dstrbtr_grp_cd
                and itg.active = 'Y'
            )

        union all

        select trim(code) as dstrbtr_grp_cd,
            trim(name) as dstrbtr_grp_nm,
            trim(primary_sold_to_code) as primary_sold_to,
            trim(primary_sold_to_name) as primary_sold_to_nm,
            trim(reportgroup10desc_code) as rpt_grp_10,
            trim(reportgroup10desc_name) as rpt_grp_10_desc,
            trim(reportgroup12desc_code) as rpt_grp_12,
            trim(reportgroup12desc_name) as rpt_grp_12_desc,
            trim(reportgroup13desc_code) as rpt_grp_13,
            trim(reportgroup13desc_name) as rpt_grp_13_desc,
            trim(reportgroup14desc_code) as rpt_grp_14,
            trim(reportgroup14desc_name) as rpt_grp_14_desc,
            trim(reportgroup8desc_code) as rpt_grp_8,
            trim(reportgroup8desc_name) as rpt_grp_8_desc,
            lastchgdatetime as last_chg_datetime,
            effective_from,
            '9999-12-31' as effective_to,
            'Y' as active,
            current_timestamp()::timestamp_ntz(9) as crtd_dttm,
            current_timestamp()::timestamp_ntz(9) as updt_dttm
        from (
            select sdl.*,
                itg.effective_from
            from {{this}} itg,
                source sdl
            where sdl.lastchgdatetime = itg.last_chg_datetime
                and trim(sdl.code) = itg.dstrbtr_grp_cd
            )

        union all

        select trim(code) as dstrbtr_grp_cd,
            trim(name) as dstrbtr_grp_nm,
            trim(primary_sold_to_code) as primary_sold_to,
            trim(primary_sold_to_name) as primary_sold_to_nm,
            trim(reportgroup10desc_code) as rpt_grp_10,
            trim(reportgroup10desc_name) as rpt_grp_10_desc,
            trim(reportgroup12desc_code) as rpt_grp_12,
            trim(reportgroup12desc_name) as rpt_grp_12_desc,
            trim(reportgroup13desc_code) as rpt_grp_13,
            trim(reportgroup13desc_name) as rpt_grp_13_desc,
            trim(reportgroup14desc_code) as rpt_grp_14,
            trim(reportgroup14desc_name) as rpt_grp_14_desc,
            trim(reportgroup8desc_code) as rpt_grp_8,
            trim(reportgroup8desc_name) as rpt_grp_8_desc,
            lastchgdatetime as last_chg_datetime,
            current_timestamp()::timestamp_ntz(9) as effective_from,
            '9999-12-31' as effective_to,
            'Y' as active,
            current_timestamp()::timestamp_ntz(9) as crtd_dttm,
            current_timestamp()::timestamp_ntz(9) as updt_dttm
        from (
            select *
            from source sdl
            where trim(code) not in (
                    select distinct dstrbtr_grp_cd
                    from {{this}}
                    )
            )
			
),
transformed as(
    select * from wks
    union all
    select * from {{this}}
    where
    dstrbtr_grp_cd not in (
            select trim(dstrbtr_grp_cd) from wks
        )
)
select * from transformed