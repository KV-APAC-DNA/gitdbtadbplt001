{{
    config(
        pre_hook="{{build_itg_mds_ph_ref_pos_primary_sold_to()}}"
    )
}}
with sdl_mds_ph_ref_pos_primary_sold_to as 
(
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_ref_pos_primary_sold_to') }}
),
trans as
(
(
    with wks as (
        select cust_cd,
            primary_soldto,
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
                    sdl_mds_ph_ref_pos_primary_sold_to sdl
                where sdl.lastchgdatetime != itg.last_chg_datetime
                    and trim(sdl.name) = itg.cust_cd
            )
        union all
        select trim(name) as cust_cd,
            trim("sold to") as primary_soldto,
            lastchgdatetime as last_chg_datetime,
            current_timestamp as effective_from,
            '9999-12-31' as effective_to,
            'Y' as active,
            current_timestamp as crtd_dttm,
            current_timestamp as updt_dttm
        from (
                select sdl.*
                from {{this}} itg,
                    sdl_mds_ph_ref_pos_primary_sold_to sdl
                where sdl.lastchgdatetime != itg.last_chg_datetime
                    and trim(sdl.name) = itg.cust_cd
                    and itg.active = 'Y'
            )
        union all
        select trim(name) as cust_cd,
            trim("sold to") as primary_soldto,
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
                    sdl_mds_ph_ref_pos_primary_sold_to sdl
                where sdl.lastchgdatetime = itg.last_chg_datetime
                    and trim(sdl.name) = itg.cust_cd
            )
        union all
        select trim(name) as cust_cd,
            trim("sold to") as primary_soldto,
            lastchgdatetime as last_chg_datetime,
            current_timestamp as effective_from,
            '9999-12-31' as effective_to,
            'Y' as active,
            current_timestamp as crtd_dttm,
            current_timestamp as updt_dttm
        from (
                select *
                from sdl_mds_ph_ref_pos_primary_sold_to sdl
                where trim(name) not in (
                        select distinct cust_cd
                        from {{this}}
                    )
            )
    )
    select *
    from wks
    union all
    select *
    from {{this}}
    where cust_cd not in (
            select trim(cust_cd)
            from wks
        )
)
),
final as
(
select 
    cust_cd::varchar(30) as cust_cd,
    primary_soldto::varchar(30) as primary_soldto,
    last_chg_datetime::timestamp_ntz(9) as last_chg_datetime,
    effective_from::timestamp_ntz(9) as effective_from,
    effective_to::timestamp_ntz(9) as effective_to,
    active::varchar(10) as active,
    current_timestamp()::timestamp_ntz(9) as crtd_dttm ,
    current_timestamp()::timestamp_ntz(9) as updt_dttm 
from trans
)
select * from final