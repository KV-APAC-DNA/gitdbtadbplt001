--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_mds_rg_sfmc_gender') }}
),

--Logical CTE

--Final CTE
final as (
    select 
        code::varchar(100) as gender_raw,
	    gender_mapped_code::varchar(100) as gender_standard,	
	    current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
)

--Final select
select * from final