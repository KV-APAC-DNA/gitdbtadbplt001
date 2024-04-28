{{
    config(
        pre_hook="{{build_phlitg_integration__itg_mds_ph_ref_repbrand()}}"
    )
}}

with source as(
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_ref_repbrand') }}
),
wks as (
    select brand_id,
				brand_cd,
				brand_nm,
				franchise_cd,
				franchise_id,
				francise_nm,
				local_branc_desc,
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
					and trim(sdl.code) = itg.brand_cd
				)
			
			union all
			
			select cast(trim(brandid) as varchar) as brand_id,
				trim(code) as brand_cd,
				trim(name) as brand_nm,
				trim(franchisecd_code) as franchise_cd,
				cast(trim(franchisecd_id) as varchar) as franchise_id,
				trim(franchisecd_name) as francise_nm,
				trim(localbranddesc) as local_branc_desc,
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
					and trim(sdl.code) = itg.brand_cd
					and itg.active = 'Y'
				)
			
			union all
			
			select cast(trim(brandid) as varchar) as brand_id,
				trim(code) as brand_cd,
				trim(name) as brand_nm,
				trim(franchisecd_code) as franchise_cd,
				cast(trim(franchisecd_id) as varchar) as franchise_id,
				trim(franchisecd_name) as francise_nm,
				trim(localbranddesc) as local_branc_desc,
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
					and trim(sdl.code) = itg.brand_cd
				)
			
			union all
			
			select cast(trim(brandid) as varchar) as brand_id,
				trim(code) as brand_cd,
				trim(name) as brand_nm,
				trim(franchisecd_code) as franchise_cd,
				cast(trim(franchisecd_id) as varchar) as franchise_id,
				trim(franchisecd_name) as francise_nm,
				trim(localbranddesc) as local_branc_desc,
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
						select distinct brand_cd
						from {{this}}
						)
				)
			
),
transformed as(
    select * from wks
    union all
    select * from {{this}}
    where
    brand_cd not in (
            select trim(brand_cd) from wks
        )
),
final as(
    select 
        brand_id::varchar(30) as brand_id,
        brand_cd::varchar(30) as brand_cd,
        brand_nm::varchar(255) as brand_nm,
        franchise_cd::varchar(50) as franchise_cd,
        franchise_id::varchar(50) as franchise_id,
        francise_nm::varchar(255) as francise_nm,
        local_branc_desc::varchar(255) as local_branc_desc,
        last_chg_datetime::timestamp_ntz(9) as last_chg_datetime,
        effective_from::timestamp_ntz(9) as effective_from,
        effective_to::timestamp_ntz(9) as effective_to,
        active::varchar(10) as active,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from transformed
)
select * from final