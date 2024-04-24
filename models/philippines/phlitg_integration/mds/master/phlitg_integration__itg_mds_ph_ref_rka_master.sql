{{
    config
    (
        pre_hook="{{build_itg_mds_ph_ref_rka_master()}}"
    )
}}
with sdl_mds_ph_ref_rka_master as (
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_ref_rka_master') }}
),

wks as (
    select
        rka_cd,
        rka_nm,
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
        from {{ this }} AS itg,
            sdl_mds_ph_ref_rka_master AS sdl
        where
            sdl.lastchgdatetime != itg.last_chg_datetime
            and trim(sdl.code) = itg.rka_cd
    )
    union all
    select
        trim(code) as rka_cd,
        trim(name) as rka_nm,
        lastchgdatetime as last_chg_datetime,
        current_timestamp as effective_from,
        '9999-12-31' as effective_to,
        'Y' as active,
        current_timestamp as crtd_dttm,
        current_timestamp as updt_dttm
    from (
        select sdl.*
        from {{ this }} AS itg,
            sdl_mds_ph_ref_rka_master AS sdl
        where
            sdl.lastchgdatetime != itg.last_chg_datetime
            and trim(sdl.code) = itg.rka_cd
            and itg.active = 'Y'
    )
    union all
    select
        trim(code) as rka_cd,
        trim(name) as rka_nm,
        lastchgdatetime as last_chg_datetime,
        effective_from,
        '9999-12-31' as effective_to,
        'Y' as active,
        current_timestamp as crtd_dttm,
        current_timestamp as updt_dttm
    from (
        select
            sdl.*,
            itg.effective_from
        from {{ this }} AS itg,
            sdl_mds_ph_ref_rka_master AS sdl
        where
            sdl.lastchgdatetime = itg.last_chg_datetime
            and trim(sdl.code) = itg.rka_cd
    )
    union all
    select
        trim(code) as rka_cd,
        trim(name) as rka_nm,
        lastchgdatetime as last_chg_datetime,
        current_timestamp as effective_from,
        '9999-12-31' as effective_to,
        'Y' as active,
        current_timestamp as crtd_dttm,
        current_timestamp as updt_dttm
    from (
        select *
        from sdl_mds_ph_ref_rka_master sdl 
        where trim(code) not in (
            select distinct rka_cd
            from {{ this }}
        )
    )
),

transformed as (
    select *
    from wks
    union all
    select *
    from {{ this }}
    where rka_cd not in (
        select trim(rka_cd)
        from wks
    )

),

final as (
    select
        rka_cd::varchar(255) as rka_cd,
        rka_nm::varchar(255) as rka_nm,
        last_chg_datetime::timestamp_ntz(9) as last_chg_datetime,
        effective_from::timestamp_ntz(9) as effective_from,
        effective_to::timestamp_ntz(9) as effective_to,
        active::varchar(10) as active,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from transformed
)

select * from final
